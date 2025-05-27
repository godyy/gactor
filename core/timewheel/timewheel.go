package timewheel

import (
	"container/list"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// LevelConfig 时间轮层级配置
type LevelConfig struct {
	// Name 层级名称.
	Name string

	// Span 时间间隔.
	Span time.Duration

	// Slots 槽位数量.
	Slots int
}

// TimerArgs 定时器参数.
type TimerArgs struct {
	// TID 定时器ID.
	TID uint64

	// Args 定时器参数.
	Args any
}

// TimerFunc 定时器回调函数.
type TimerFunc func(TimerArgs)

// timer 定时器.
type timer struct {
	id            uint64    // 定时器ID.
	delay         int64     // 延迟时间, 已转换为tick数.
	periodic      bool      // 是否为周期性定时器.
	f             TimerFunc // 回调函数.
	args          any       // 参数.
	level         int       // 所在层级.
	slot          int       // 所在槽位.
	scheduleTicks int64     // 调度时间点, tick 数.
}

func newTimer(id uint64, delay int64, periodic bool, f TimerFunc, args any) *timer {
	t := &timer{}
	t.id = id
	t.delay = delay
	t.periodic = periodic
	t.f = f
	t.args = args
	t.level = -1
	t.slot = -1
	t.scheduleTicks = 0
	return t
}

// timerPool 定时器池.
type timerPool struct {
	mtx sync.Mutex        // 互斥锁.
	m   map[uint64]*timer // 映射.
}

func (p *timerPool) exists(timerId uint64) bool {
	_, exists := p.m[timerId]
	return exists
}

func (p *timerPool) reset() {
	p.m = make(map[uint64]*timer)
}

// slot 槽位.
type slot struct {
	mtx sync.RWMutex             // 互斥锁.
	l   *list.List               // 列表.
	m   map[uint64]*list.Element // 映射.
}

func (s *slot) add(t *timer) {
	s.m[t.id] = s.l.PushBack(t)
}

func (s *slot) remove(timerId uint64) {
	e, exists := s.m[timerId]
	if !exists {
		return
	}
	s.l.Remove(e)
	delete(s.m, timerId)
}

func (s *slot) empty() bool {
	return s.l.Len() == 0
}

func (s *slot) reset() {
	if s.l.Len() == 0 {
		return
	}
	s.l.Init()
	s.m = make(map[uint64]*list.Element, s.l.Len())
}

// level 层级结构.
type level struct {
	span        time.Duration // 时间跨度.
	slots       []*slot       // 槽位.
	currentSlot int           // 当前槽位.
}

func (l *level) reset() {
	for i := range l.slots {
		l.slots[i].reset()
	}
	l.currentSlot = 0
}

// TimerExecutor 定时器回调函数执行器.
type TimerExecutor func(f TimerFunc, args TimerArgs)

// TimeWheel 时间轮.
type TimeWheel struct {
	executor TimerExecutor // 执行器.
	lMaxSpan time.Duration // 最低层级最大时间跨度.
	hMaxSpan time.Duration // 最高层级最大时间跨度.

	mtx                sync.RWMutex
	stopped            bool           // 是否已停止.
	cStopped           chan struct{}  // 停止信号.
	timerIdGen         uint64         // ID生成器.
	timerPools         []*timerPool   // 定时器池.
	levels             []*level       // 层级.
	triggerSlots       []*slot        // 触发槽位.
	triggerSlotWorkers []chan *slot   // 触发槽位工作器.
	triggerSlotsWg     sync.WaitGroup // 触发槽位等待组.
	ticks              int64          // 当前tick数，用于追踪绝对时间.
	totalTickTime      time.Duration  // 时间轮总运行时长.

}

// NewTimeWheel 创建时间轮
func NewTimeWheel(configs []LevelConfig, executor TimerExecutor) (*TimeWheel, error) {
	if len(configs) == 0 {
		return nil, errors.New("timewheel: NewTimeWheel: empty level configs")
	}

	if executor == nil {
		return nil, errors.New("timewheel: NewTimeWheel: executor is nil")
	}

	// 检查配置有效性
	for i, cfg := range configs {
		if cfg.Slots <= 0 {
			return nil, fmt.Errorf("timewheel: NewTimeWheel: level %s Slots must > 0", cfg.Name)
		}
		if cfg.Span <= 0 {
			return nil, fmt.Errorf("timewheel: NewTimeWheel: level %s Span must > 0", cfg.Name)
		}
		// 检查层级跨度是否正确
		// 低层级跨度总合应该等于高层级的单位跨度
		if i > 0 {
			lowCfg := configs[i-1]
			lowMaxSpan := lowCfg.Span * time.Duration(lowCfg.Slots)
			if cfg.Span != lowMaxSpan {
				return nil, fmt.Errorf("timewheel: NewTimeWheel: span of level %s not equal max-span of level %s", cfg.Name, lowCfg.Name)
			}
		}
	}

	// 创建时间轮
	tw := &TimeWheel{
		executor:           executor,
		cStopped:           make(chan struct{}),
		timerIdGen:         0,
		timerPools:         make([]*timerPool, runtime.NumCPU()),
		levels:             make([]*level, len(configs)),
		triggerSlots:       make([]*slot, 0, len(configs)),
		triggerSlotWorkers: make([]chan *slot, len(configs)),
		ticks:              0,
		totalTickTime:      0,
	}

	// 计算最低、最高层级最大时间跨度.
	lowConfig := configs[0]
	tw.lMaxSpan = lowConfig.Span * time.Duration(lowConfig.Slots)
	highConfig := configs[len(configs)-1]
	tw.hMaxSpan = highConfig.Span * time.Duration(highConfig.Slots)

	// 初始化定时器池.
	for i := 0; i < runtime.NumCPU(); i++ {
		tw.timerPools[i] = &timerPool{
			m: make(map[uint64]*timer),
		}
	}

	// 初始化层级
	for i, cfg := range configs {
		// 创建层级
		level := &level{
			span:        cfg.Span,
			slots:       make([]*slot, cfg.Slots),
			currentSlot: 0,
		}
		// 初始化槽位
		for j := 0; j < cfg.Slots; j++ {
			level.slots[j] = &slot{
				l: list.New(),
				m: make(map[uint64]*list.Element),
			}
		}
		tw.levels[i] = level
	}

	// 初始化槽位触工作器.
	for i := range tw.triggerSlotWorkers {
		c := make(chan *slot, 1)
		tw.triggerSlotWorkers[i] = c
		go func() {
			for {
				select {
				case slot := <-c:
					tw.triggerSlot(slot)
					tw.triggerSlotsWg.Done()
				case <-tw.cStopped:
					select {
					case <-c:
						tw.triggerSlotsWg.Done()
					default:
					}
					return
				}
			}
		}()
	}

	return tw, nil
}

// isStopped 检查时间轮是否已停止.
func (tw *TimeWheel) isStopped() bool {
	tw.mtx.RLock()
	defer tw.mtx.RUnlock()
	return tw.stopped
}

// genTimerId 生成定时器ID.
func (tw *TimeWheel) genTimerId() uint64 {
	timerId := atomic.AddUint64(&tw.timerIdGen, 1)
	if timerId == 0 {
		timerId = atomic.AddUint64(&tw.timerIdGen, 1)
	}
	return timerId
}

// getTimerPool 获取定时器池.
func (tw *TimeWheel) getTimerPool(timerId uint64) *timerPool {
	return tw.timerPools[timerId%uint64(len(tw.timerPools))]
}

// tickSpan 获取时间轮的tick跨度.
func (tw *TimeWheel) tickSpan() time.Duration {
	return tw.levels[0].span
}

// calcScheduleTicks 计算定时器的调度时间点.
func (tw *TimeWheel) calcScheduleTicks(d time.Duration) int64 {
	tickSpan := tw.tickSpan()
	scheduleTicks := tw.ticks + int64(d/tickSpan)
	if d%tickSpan > 0 {
		scheduleTicks++
	}
	return scheduleTicks
}

// convertDelayToTicks 将延迟时间转换为tick数.
func convertDelayToTicks(delay time.Duration, tickSpan time.Duration) int64 {
	ticks := int64(delay / tickSpan)
	if delay%tickSpan > 0 {
		ticks++
	}
	return ticks
}

// ErrDelayMustGreaterThanZero 定时器延时必须大于0的错误.
var ErrDelayMustGreaterThanZero = errors.New("timewheel: delay time must be greater than 0")

// ErrDelayExceedMaxSpan 定时器延时超过时间轮最大允许时间跨度的错误.
var ErrDelayExceedMaxSpan = errors.New("timewheel: delay time exceeds max span")

// ErrTimerFuncIsNil 回调函数为nil的错误.
var ErrTimerFuncIsNil = errors.New("timewheel: timer func is nil")

// ErrTimeWheelStopped 时间轮已停止的错误.
var ErrTimeWheelStopped = errors.New("timewheel: time wheel is stopped")

// TimerOptions 添加定时器的参数选项
type TimerOptions struct {
	// Delay 定时器延时.
	Delay time.Duration

	// Offset 延时偏移量.
	Offset time.Duration

	// Periodic 是否周期性定时器.
	Periodic bool

	// Func 回调函数.
	Func TimerFunc

	// Args 定时器参数.
	Args any
}

// TimerIdNone 表示无效的定时器ID.
const TimerIdNone = 0

// AddTimer 添加定时器. 若成功返回定时器ID.
func (tw *TimeWheel) AddTimer(opts TimerOptions) (uint64, error) {
	if opts.Delay <= 0 {
		return TimerIdNone, ErrDelayMustGreaterThanZero
	}

	if opts.Delay > tw.hMaxSpan {
		return TimerIdNone, ErrDelayExceedMaxSpan
	}

	if opts.Offset < 0 {
		opts.Offset = 0
	}

	if opts.Func == nil {
		return TimerIdNone, ErrTimerFuncIsNil
	}

	// 生成定时器定时器ID
	timerId := tw.genTimerId()

	// 将延时转换为tick数.
	delay := convertDelayToTicks(opts.Delay, tw.tickSpan())

	// 创建定时器.
	timer := newTimer(timerId, delay, opts.Periodic, opts.Func, opts.Args)

	// 锁定时间轮.
	// 保证在添加定时器期间, 时间轮不会tick.
	tw.mtx.RLock()

	if tw.stopped {
		tw.mtx.RUnlock()
		return TimerIdNone, ErrTimeWheelStopped
	}

	// 计算定时器的调度时间点.
	timer.scheduleTicks = tw.calcScheduleTicks(opts.Delay + opts.Offset)

	// 添加定时器.
	timerPool := tw.getTimerPool(timerId)
	timerPool.mtx.Lock()
	timerPool.m[timerId] = timer
	timerPool.mtx.Unlock()

	// 固定定时器到时间轮
	tw.pinTimer(timer)

	// 解锁时间轮.
	tw.mtx.RUnlock()

	return timerId, nil
}

// pinTimer 将定时器固定到时间轮的层级结构中
func (tw *TimeWheel) pinTimer(timer *timer) {
	// 计算定时器的剩余延迟时间.
	tickSpan := tw.tickSpan()
	d := time.Duration(timer.scheduleTicks-tw.ticks) * tickSpan
	if timer.level == -1 {
		// 首次分派, 尝试对齐时间轮, 也就是当定时器被调度时, 低于当前的所有层级总是处于原点.
		// 使定时器依次从高层到低层调度时，每一层至多调度一次.
		timer.level = len(tw.levels) - 1
		if mod := time.Duration(tw.totalTickTime) % tw.levels[timer.level].span; mod > 0 {
			dd := d + mod
			if dd/tw.lMaxSpan > d/tw.lMaxSpan {
				dd = d
			}
		}
	}

	// 定位层级.
	var level *level
	for ; timer.level >= 0; timer.level-- {
		level = tw.levels[timer.level]
		if d >= level.span {
			break
		}
	}

	// 计算槽位跨度
	slots := int(d/level.span) - 1
	// 计算最终槽位：当前槽位 + 槽位跨度，然后对总槽位数取模
	timer.slot = (level.currentSlot + slots) % len(level.slots)

	// 将定时器添加到槽位中.
	slot := level.slots[timer.slot]
	slot.mtx.Lock()
	slot.add(timer)
	slot.mtx.Unlock()

}

// RemoveTimer 从时间轮中删除指定定时器
func (tw *TimeWheel) RemoveTimer(timerId uint64) bool {
	if tw.isStopped() {
		return false
	}

	timerPool := tw.getTimerPool(timerId)
	timerPool.mtx.Lock()

	// 查找定时器.
	timer, exists := timerPool.m[timerId]
	if !exists {
		timerPool.mtx.Unlock()
		return false
	}

	// 将定时器从时间轮层级中移除.
	if timer.level != -1 {
		slot := tw.levels[timer.level].slots[timer.slot]
		slot.mtx.Lock()
		slot.remove(timerId)
		slot.mtx.Unlock()
	}

	// 删除定时器.
	delete(timerPool.m, timerId)

	timerPool.mtx.Unlock()

	return true
}

// Tick 推进时间轮，处理到期定时器
func (tw *TimeWheel) Tick(ticks int) {
	if tw.isStopped() {
		return
	}

	for i := 0; i < ticks; i++ {
		// 推进时间轮.
		if !tw.advance() {
			return
		}

		// 触发到期槽位.
		if len(tw.triggerSlots) > 0 {
			stopped := false
			for i, slot := range tw.triggerSlots {
				select {
				case tw.triggerSlotWorkers[i] <- slot:
				case <-tw.cStopped:
					tw.triggerSlotsWg.Done()
					stopped = true
				}
			}
			tw.triggerSlotsWg.Wait()
			if stopped {
				return
			}
		}

	}
}

// advance 推进时间轮.
func (tw *TimeWheel) advance() bool {
	tw.mtx.Lock()

	if tw.stopped {
		tw.mtx.Unlock()
		return false
	}

	tw.triggerSlots = tw.triggerSlots[:0]

	// 推进当前tick数.
	tw.ticks++
	tw.totalTickTime += tw.tickSpan()

	// 推进层级, 获取待执行的定时器槽位.
	for _, level := range tw.levels {
		// 更新层级, 获取待执行的定时器槽位.
		slot := level.slots[level.currentSlot]
		slot.mtx.RLock()
		if !slot.empty() {
			tw.triggerSlots = append(tw.triggerSlots, slot)
		}
		slot.mtx.RUnlock()
		level.currentSlot = (level.currentSlot + 1) % len(level.slots)

		// 层级归零, 向上层推进.
		if level.currentSlot != 0 {
			break
		}
	}

	if n := len(tw.triggerSlots); n > 0 {
		tw.triggerSlotsWg.Add(n)
	}

	tw.mtx.Unlock()

	return true
}

// triggerSlot 触发槽位.
func (tw *TimeWheel) triggerSlot(slot *slot) {
	for {
		// 取出定时器.
		slot.mtx.Lock()
		front := slot.l.Front()
		if front == nil {
			slot.mtx.Unlock()
			break
		}
		timer := front.Value.(*timer)
		slot.l.Remove(front)
		delete(slot.m, timer.id)
		slot.mtx.Unlock()

		// 获取定时器池.
		timerPool := tw.getTimerPool(timer.id)

		// 触发定时器.
		if tw.ticks >= timer.scheduleTicks {
			// 定时器已到期.

			// 执行定时器
			tw.executor(timer.f, TimerArgs{
				TID:  timer.id,
				Args: timer.args,
			})

			// 根据定时器是否周期性做相应处理.
			if timer.periodic {
				// 周期性定时器, 重置状态.
				timerPool.mtx.Lock()
				if !timerPool.exists(timer.id) {
					timerPool.mtx.Unlock()
					continue
				}
				timer.level = -1
				timer.slot = -1
				timer.scheduleTicks = tw.ticks + timer.delay
				tw.pinTimer(timer)
				timerPool.mtx.Unlock()
			} else {
				// 非周期性定时器直接删除.
				timerPool.mtx.Lock()
				delete(timerPool.m, timer.id)
				timerPool.mtx.Unlock()
			}

		} else {
			// 定时器未到期, 降层调度.

			timerPool.mtx.Lock()
			if !timerPool.exists(timer.id) {
				timerPool.mtx.Unlock()
				continue
			}
			tw.pinTimer(timer)
			timerPool.mtx.Unlock()
		}

	}
}

// Reset 重置时间轮.
func (tw *TimeWheel) Reset() {
	tw.mtx.Lock()
	defer tw.mtx.Unlock()

	if tw.stopped {
		return
	}

	tw.reset()
}

// Stop 停止时间轮.
func (tw *TimeWheel) Stop() {
	tw.mtx.Lock()
	defer tw.mtx.Unlock()

	if tw.stopped {
		return
	}

	close(tw.cStopped)
	tw.reset()
	tw.stopped = true
}

// reset 重置时间轮.
func (tw *TimeWheel) reset() {
	// 先尽快停止正在触发的槽位.
	for _, slot := range tw.triggerSlots {
		slot.mtx.Lock()
		slot.reset()
		slot.mtx.Unlock()
	}

	// 重置层级.
	for _, level := range tw.levels {
		level.reset()
	}

	// 重置定时器池.
	for _, timerPool := range tw.timerPools {
		timerPool.mtx.Lock()
		timerPool.reset()
		timerPool.mtx.Unlock()
	}

	// 等待槽位触发完成.
	tw.triggerSlotsWg.Wait()

	// 重置其它数据.
	tw.timerIdGen = 0
	tw.ticks = 0
	tw.totalTickTime = 0
}
