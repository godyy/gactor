package gactor

import (
	"runtime"
	"sync/atomic"
	"time"

	"github.com/godyy/gtimewheel"
)

// TimerId 定时器ID.
type TimerId = gtimewheel.TimerID

// TimerIdNone 表示无效的定时器ID.
const TimerIdNone = TimerId(0)

// TimerFunc 定时器回调函数.
type TimerFunc func(TimerArgs)

// TimerArgs 定时器参数.
type TimerArgs struct {
	TID  TimerId // 定时器ID.
	Args any     // 定时器参数.
	Err  error   // 错误.
}

// TimerConfig 定时器配置.
type TimerConfig struct {
	// TimeWheelLevels 时间轮配置.
	TimeWheelLevels []gtimewheel.LevelConfig

	// MaxTimerAmount 最大定时器数量.
	// 目前用于控制定时器命令队列和已触发回调队列的大小.
	// 默认值 1000.
	MaxTimerAmount int
}

func (c *TimerConfig) init() {
	if len(c.TimeWheelLevels) == 0 {
		panic("gactor: TimerConfig: TimeWheelLevels not specified")
	}

	if c.MaxTimerAmount <= 0 {
		c.MaxTimerAmount = 1000
	}
}

// timer 定时器.
type timer struct {
	id       TimerId   // 定时器ID.
	periodic bool      // 是否为周期定时器.
	f        TimerFunc // 定时器回调函数.
	args     any       // 定时器参数.
	err      error     // 错误.
}

func newTimer(id TimerId, periodic bool, f TimerFunc, args any) *timer {
	return &timer{
		id:       id,
		periodic: periodic,
		f:        f,
		args:     args,
	}
}

func (t *timer) release() {}

func (t *timer) trigger() {
	defer t.release()
	t.f(TimerArgs{
		TID:  t.id,
		Args: t.args,
		Err:  t.err,
	})
}

// timerCmd 定时器命令.
type timerCmd interface {
	exec(*timerManager)
	release()
	discard()
}

// timerManager 定时器管理器.
// gtimewheel 的公开接口是非并发安全的，因此这里参考 rpcManager，
// 将 Add/Remove/Tick/Stop 串行化到单个协程中执行。
type timerManager struct {
	svc         *Service              // 所属 Service.
	timeWheel   *gtimewheel.TimeWheel // 时间轮.
	timerIdIncr TimerId               // 定时器ID自增键.
	chCmds      chan timerCmd         // 命令通道.
	chTriggerd  chan *timer           // 已触发、等待执行回调的定时器.
	lastTickAt  time.Time             // 上次推进时间轮的时间.
}

func newTimerManager(svc *Service) *timerManager {
	m := &timerManager{
		svc:        svc,
		chCmds:     make(chan timerCmd, svc.getCfg().MaxTimerAmount),
		chTriggerd: make(chan *timer, svc.getCfg().MaxTimerAmount),
	}

	wheel, err := gtimewheel.NewTimeWheel(&gtimewheel.Config{
		Levels:   svc.getCfg().TimeWheelLevels,
		Callback: m.onTimeWheelTimer,
	})
	if err != nil {
		panic(err)
	}
	m.timeWheel = wheel
	return m
}

// start 启动定时器管理器.
func (m *timerManager) start() {
	now := m.svc.getTimeSystem().Now()
	m.lastTickAt = now
	m.timeWheel.Start(now.UnixNano())

	stopWait := m.svc.getStopWait()
	stopWait.W.Add(1)
	go m.run()

	stopWait.W.Add(1)
	go m.runTicker()

	workers := runtime.NumCPU()
	stopWait.W.Add(workers)
	for i := 0; i < workers; i++ {
		go m.triggeredTimerWorker()
	}
}

// run 执行命令主循环.
func (m *timerManager) run() {
	defer m.svc.getStopWait().W.Done()
	for {
		select {
		case c := <-m.chCmds:
			c.exec(m)
			c.release()
		case <-m.svc.getStopWait().C:
			m.timeWheel.Stop()
			close(m.chTriggerd)
			return
		}
	}
}

// runTicker 周期性推进时间轮.
func (m *timerManager) runTicker() {
	defer m.svc.getStopWait().W.Done()

	ticker := time.NewTicker(m.svc.getCfg().TimeWheelLevels[0].Span)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			_ = m.enqueueCmd(newTimerTickCmd(m.svc.getTimeSystem().Now(), nil))
		case <-m.svc.getStopWait().C:
			return
		}
	}
}

// nextTimerId 生成定时器ID.
func (m *timerManager) nextTimerId() TimerId {
	return atomic.AddUint64(&m.timerIdIncr, 1)
}

// enqueueCmd 入队命令.
func (m *timerManager) enqueueCmd(c timerCmd) error {
	begin := time.Now()
	select {
	case m.chCmds <- c:
		if d := time.Since(begin); d > m.svc.getCfg().QueueWriteTimeAlarmThreshold {
			m.svc.getLogger().Warnf("enqueue timer cmd slowly, cost:%dms", d.Milliseconds())
		}
		if len(m.chCmds) >= m.svc.getCfg().MaxTimerAmount {
			m.svc.getLogger().Warn("enqueue timer cmd queue full")
		}
		return nil
	case <-m.svc.getStopWait().C:
		c.discard()
		return errServiceStopped
	}
}

// startTimer 启动定时器.
func (m *timerManager) startTimer(d time.Duration, periodic bool, args any, cb TimerFunc) TimerId {
	tid := m.nextTimerId()
	period := time.Duration(0)
	if periodic {
		period = d
	}
	if err := m.enqueueCmd(newTimerAddCmd(
		tid, m.svc.getTimeSystem().Now().Add(d).UnixNano(),
		period, cb, args,
	)); err != nil {
		return TimerIdNone
	}
	return tid
}

// stopTimer 停止定时器.
func (m *timerManager) stopTimer(tid TimerId) {
	if tid == TimerIdNone {
		return
	}
	_ = m.enqueueCmd(newTimerRemCmd(tid))
}

// tickTo 串行推进时间轮.
func (m *timerManager) tickTo(t time.Time) {
	tickSpan := m.svc.getCfg().TimeWheelLevels[0].Span
	elapsedTicks := int64(t.Sub(m.lastTickAt) / tickSpan)
	if elapsedTicks <= 0 {
		return
	}

	for i := int64(0); i < elapsedTicks; i++ {
		m.lastTickAt = m.lastTickAt.Add(tickSpan)
		m.timeWheel.Tick()
	}
}

// tickTimeWheel 推进时间轮到当前时间.
func (m *timerManager) tickTimeWheel() {
	done := make(chan struct{})
	if err := m.enqueueCmd(newTimerTickCmd(m.svc.getTimeSystem().Now(), done)); err != nil {
		return
	}
	<-done
}

// onTimeWheelTimer 时间轮定时器回调.
func (m *timerManager) onTimeWheelTimer(t gtimewheel.Timer) {
	m.svc.monitorTriggerTimerAmount(1)
	timer := t.Value.(*timer)
	m.enqueueTriggerdTimer(timer)
}

// enqueueTriggerdTimer 入队已触发的定时器.
func (m *timerManager) enqueueTriggerdTimer(t *timer) {
	begin := time.Now()
	select {
	case m.chTriggerd <- t:
		if d := time.Since(begin); d > m.svc.getCfg().QueueWriteTimeAlarmThreshold {
			m.svc.getLogger().Warnf("enqueue triggerd timer cost:%dms", d.Milliseconds())
		}
		if len(m.chTriggerd) > m.svc.getCfg().MaxTimerAmount {
			m.svc.getLogger().Warn("enqueue triggerd timer queue full")
		}
	case <-m.svc.getStopWait().C:
	}
}

// triggeredTimerWorker 已出发定时器worker.
func (m *timerManager) triggeredTimerWorker() {
	defer m.svc.getStopWait().W.Done()
	for {
		select {
		case t := <-m.chTriggerd:
			m.execTriggeredTimer(t)
		case <-m.svc.getStopWait().C:
			return
		}
	}
}

// execTriggeredTimer 执行定时器回调.
func (m *timerManager) execTriggeredTimer(t *timer) {
	defer recoverAndLog("exec triggered timer callback panic", m.svc.getLogger(), nil)
	t.trigger()
}

// timerAddCmd 添加定时器命令.
type timerAddCmd struct {
	tid       TimerId
	expiredAt int64
	period    time.Duration
	cb        TimerFunc
	args      any
}

func newTimerAddCmd(tid TimerId, expiredAt int64, period time.Duration, cb TimerFunc, args any) *timerAddCmd {
	return &timerAddCmd{
		tid:       tid,
		expiredAt: expiredAt,
		period:    period,
		cb:        cb,
		args:      args,
	}
}

func (c *timerAddCmd) exec(tm *timerManager) {
	t := newTimer(c.tid, c.period > 0, c.cb, c.args)
	err := tm.timeWheel.AddTimer(c.tid, c.expiredAt, c.period, t)
	if err == nil {
		tm.svc.monitorStartTimerAmount(1)
	} else {
		tm.svc.getLogger().Errorf("add timewheel timer, tid:%d, err:%v", c.tid, err)
		t.err = err
		tm.enqueueTriggerdTimer(t)
	}
}

func (c *timerAddCmd) release() {
	c.cb = nil
	c.args = nil
}

func (c *timerAddCmd) discard() {
	c.release()
}

// timerRemCmd 停止定时器命令.
type timerRemCmd struct {
	tid TimerId
}

func newTimerRemCmd(tid TimerId) *timerRemCmd {
	return &timerRemCmd{tid: tid}
}

func (c *timerRemCmd) exec(tm *timerManager) {
	value, ok := tm.timeWheel.RemoveTimer(c.tid)
	if !ok {
		return
	}
	t := value.(*timer)
	t.release()
	tm.svc.monitorStopTimerAmount(1)
}

func (c *timerRemCmd) release() {
}

func (c *timerRemCmd) discard() {
	c.release()
}

// timerTickCmd 推进时间轮命令.
type timerTickCmd struct {
	t    time.Time
	done chan struct{}
}

func newTimerTickCmd(t time.Time, done chan struct{}) *timerTickCmd {
	return &timerTickCmd{t: t, done: done}
}

func (c *timerTickCmd) exec(tm *timerManager) {
	tickSpan := tm.svc.getCfg().TimeWheelLevels[0].Span
	begin := time.Now()
	tm.tickTo(c.t)
	if cost := time.Since(begin); cost > tickSpan {
		tm.svc.getLogger().Warnf("tick time wheel cost:%dms", cost.Milliseconds())
	}
	if c.done != nil {
		close(c.done)
	}
}

func (c *timerTickCmd) release() {
	c.done = nil
}

func (c *timerTickCmd) discard() {
	if c.done != nil {
		close(c.done)
	}
	c.release()
}
