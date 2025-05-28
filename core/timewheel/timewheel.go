package timewheel

import (
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
	mtx sync.RWMutex      // 互斥锁.
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
	mtx    sync.Mutex        // 互斥锁.
	timers map[uint64]*timer // 定时器.
}

func (s *slot) add(t *timer) {
	s.timers[t.id] = t
}

func (s *slot) remove(timerId uint64) {
	delete(s.timers, timerId)
}

func (s *slot) empty() bool {
	return len(s.timers) == 0
}

func (s *slot) reset() {
	if len(s.timers) == 0 {
		return
	}
	s.timers = make(map[uint64]*timer)
}

// level 层级结构.
type level struct {
	span  time.Duration // 时间跨度.
	slots []*slot       // 槽位.

	mtx         sync.RWMutex      // 读写锁.
	currentSlot int               // 当前槽位.
	triggerSlot int               // 正在触发的槽位. -1 表示没有触发. 正在触发的槽位无法添加定时器.
	delayTimers map[uint64]*timer // 延迟定时器列表, 表示需要在当前触发槽位触发完毕后添加到其中的的定时器.
}

func (l *level) addDelayTimer(timer *timer) {
	l.delayTimers[timer.id] = timer
}

func (l *level) removeDelayTimer(timerId uint64) {
	delete(l.delayTimers, timerId)
}

func (l *level) popDelayTimers() map[uint64]*timer {
	if len(l.delayTimers) == 0 {
		return nil
	}
	delayTimers := l.delayTimers
	l.delayTimers = make(map[uint64]*timer)
	return delayTimers
}

func (l *level) reset() {
	for i := range l.slots {
		if i != l.triggerSlot {
			l.slots[i].mtx.Lock()
			l.slots[i].reset()
			l.slots[i].mtx.Unlock()
		}
	}
	l.currentSlot = 0
	l.triggerSlot = -1
	l.delayTimers = make(map[uint64]*timer)
}

// TimerExecutor 定时器回调函数执行器.
type TimerExecutor func(f TimerFunc, args TimerArgs)

// TimeWheel 时间轮.
type TimeWheel struct {
	executor TimerExecutor // 执行器.
	lMaxSpan time.Duration // 最低层级最大时间跨度.
	hMaxSpan time.Duration // 最高层级最大时间跨度.

	mtx                 sync.RWMutex
	stopped             bool           // 是否已停止.
	cStopped            chan struct{}  // 停止信号.
	timerIdGen          uint64         // ID生成器.
	timerPools          []*timerPool   // 定时器池.
	levels              []*level       // 层级.
	triggerLevelAmount  int            // 触发层级数.
	triggerLevelWorkers []chan *level  // 触发槽位工作器.
	triggerLevelWG      sync.WaitGroup // 触发槽位等待组.
	ticks               int64          // 当前tick数，用于追踪绝对时间.
	totalTickTime       time.Duration  // 时间轮总运行时长.

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
		executor:            executor,
		cStopped:            make(chan struct{}),
		timerIdGen:          0,
		timerPools:          make([]*timerPool, runtime.NumCPU()),
		levels:              make([]*level, len(configs)),
		triggerLevelWorkers: make([]chan *level, len(configs)),
		ticks:               0,
		totalTickTime:       0,
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
			triggerSlot: -1,
			delayTimers: make(map[uint64]*timer),
		}
		// 初始化槽位
		for j := 0; j < cfg.Slots; j++ {
			level.slots[j] = &slot{
				timers: make(map[uint64]*timer),
			}
		}
		tw.levels[i] = level
	}

	// 初始化槽位触工作器.
	for i := range tw.triggerLevelWorkers {
		c := make(chan *level, 1)
		tw.triggerLevelWorkers[i] = c
		go func() {
			for {
				select {
				case level := <-c:
					tw.triggerLevel(level)
					tw.triggerLevelWG.Done()
				case <-tw.cStopped:
					select {
					case <-c:
						tw.triggerLevelWG.Done()
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
	{
		// 添加到定时器池.
		timerPool.m[timerId] = timer
		// 固定定时器到时间轮
		tw.pinTimer(timer, true)
	}
	timerPool.mtx.Unlock()

	return timerId, nil
}

// pinTimer 将定时器固定到时间轮的层级结构中
func (tw *TimeWheel) pinTimer(timer *timer, insideAddTimer bool) {
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

	// 锁定层级.
	level.mtx.RLock()

	// 如果是在添加定时器期间, 解锁时间轮.
	if insideAddTimer {
		tw.mtx.RUnlock()
	}

	// 计算最终槽位：当前槽位 + 槽位跨度，然后对总槽位数取模
	timer.slot = (level.currentSlot + slots) % len(level.slots)

	// 将定时器添加到对应槽位中.
	if timer.slot == level.triggerSlot {
		// 槽位正在触发, 将定时器添加到延迟队列中.
		level.addDelayTimer(timer)
	} else {
		// 将定时器添加到槽位中.
		slot := level.slots[timer.slot]
		slot.mtx.Lock()
		slot.add(timer)
		slot.mtx.Unlock()
	}

	// 解锁层级.
	level.mtx.RUnlock()

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

	// 删除定时器.
	delete(timerPool.m, timerId)

	timerPool.mtx.Unlock()

	// 将定时器从时间轮层级中移除.
	if timer.level != -1 {
		level := tw.levels[timer.level]
		level.mtx.Lock()
		if timer.slot == level.triggerSlot {
			level.removeDelayTimer(timerId)
		} else {
			slot := level.slots[timer.slot]
			slot.mtx.Lock()
			slot.remove(timerId)
			slot.mtx.Unlock()
		}
		level.mtx.Unlock()
	}

	return true
}

// Tick 推进时间轮，处理到期定时器
func (tw *TimeWheel) Tick() {
	if tw.isStopped() {
		return
	}

	// 等待上一次tick结束.
	tw.TickEnd()

	// 推进时间轮.
	if !tw.advance() {
		return
	}

	// 等待触发完成.
	tw.triggerLevelWG.Wait()

}

// TickEnd 等待时间轮tick结束.
func (tw *TimeWheel) TickEnd() {
	tw.triggerLevelWG.Wait()
}

// advance 推进时间轮.
func (tw *TimeWheel) advance() bool {
	tw.mtx.Lock()

	// 时间轮已停止.
	if tw.stopped {
		tw.mtx.Unlock()
		return false
	}

	// 重置触发层级数.
	tw.triggerLevelAmount = 0

	// 推进当前tick数.
	tw.ticks++
	tw.totalTickTime += tw.tickSpan()

	// 添加等待组
	tw.triggerLevelWG.Add(len(tw.levels))

	tw.mtx.Unlock()

	// 推进层级, 获取待执行的定时器槽位.
	for i, level := range tw.levels {
		// 是否触发标记.
		trigger := false

		// 锁定层级.
		level.mtx.Lock()

		// 更新触发槽位.
		slot := level.slots[level.currentSlot]
		slot.mtx.Lock()
		if !slot.empty() {
			// 槽位非空, 添加触发槽位, 并设置层级触发槽位.
			level.triggerSlot = level.currentSlot
			tw.triggerLevelAmount++
			trigger = true
		}
		slot.mtx.Unlock()

		// 更新层级当前槽位.
		level.currentSlot = (level.currentSlot + 1) % len(level.slots)

		// 判断层级是否归零.
		return2Zero := level.currentSlot == 0

		// 解锁层级.
		level.mtx.Unlock()

		// 分配触发层级.
		if trigger {
			tw.triggerLevelWorkers[i] <- level
		}

		// 层级归零, 才向上层推进.
		if !return2Zero {
			break
		}
	}

	// 根据已触发槽位数调整等待组.
	if n := len(tw.levels) - tw.triggerLevelAmount; n > 0 {
		tw.triggerLevelWG.Add(-n)
	}

	return true
}

// triggerLevel 触发层级.
func (tw *TimeWheel) triggerLevel(level *level) {
	// 获取触发槽位.
	level.mtx.RLock()
	if level.triggerSlot == -1 {
		level.mtx.RUnlock()
		return
	}
	slot := level.slots[level.triggerSlot]
	level.mtx.RUnlock()

	// 触发槽位中的定时器.
	for tid, timer := range slot.timers {
		// 删除定时器.
		delete(slot.timers, tid)

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
				tw.pinTimer(timer, false)
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
				slot.remove(tid)
				continue
			}
			tw.pinTimer(timer, false)
			timerPool.mtx.Unlock()
		}

	}

	// 更新层级, 取出延迟定时器.
	level.mtx.Lock()
	level.triggerSlot = -1
	delayTimers := level.popDelayTimers()
	level.mtx.Unlock()

	// 处理延迟定时器.
	if len(delayTimers) > 0 {
		for tid, timer := range delayTimers {
			timerPool := tw.getTimerPool(tid)
			timerPool.mtx.Lock()
			if !timerPool.exists(tid) {
				timerPool.mtx.Unlock()
				continue
			}
			slot.mtx.Lock()
			slot.add(timer)
			slot.mtx.Unlock()
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
	// 重置层级.
	for _, level := range tw.levels {
		level.mtx.Lock()
		level.reset()
		level.mtx.Unlock()
	}

	// 重置定时器池.
	for _, timerPool := range tw.timerPools {
		timerPool.mtx.Lock()
		timerPool.reset()
		timerPool.mtx.Unlock()
	}

	// 等待槽位触发完成.
	tw.triggerLevelWG.Wait()

	// 重置其它数据.
	tw.timerIdGen = 0
	tw.ticks = 0
	tw.totalTickTime = 0
	tw.triggerLevelAmount = 0
}
