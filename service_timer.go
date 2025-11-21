package gactor

import (
	"runtime"
	"time"

	"github.com/godyy/gtimewheel"
)

// TimerConfig 定时器配置.
type TimerConfig struct {
	// TimeWheelLevels 时间轮配置.
	TimeWheelLevels []gtimewheel.LevelConfig

	// MaxTimerDelay 最大定时器延迟. 默认值为 TimeWheelLevels 配置支持的最大值.
	MaxTimerDelay time.Duration

	// MaxTriggerdTimerAmount 表示能够容纳的已触发的待执行定时器的最大数量. 当容量达
	// 到上限时, 新触发的定时器将等待先前的定时器执行后才能继续排队. 默认值 1000.
	MaxTriggerdTimerAmount int
}

func (c *TimerConfig) init() {
	if len(c.TimeWheelLevels) == 0 {
		panic("gactor: TimerConfig: TimeWheelLevels not specified")
	}

	timeWheelMaxDelay := time.Duration(0)
	timeWheelHLevel := c.TimeWheelLevels[len(c.TimeWheelLevels)-1]
	timeWheelMaxDelay = timeWheelHLevel.Span * time.Duration(timeWheelHLevel.Slots)
	if c.MaxTimerDelay <= 0 {
		c.MaxTimerDelay = timeWheelMaxDelay
	} else if c.MaxTimerDelay > timeWheelMaxDelay {
		panic("gactor: TimerConfig: MaxTimerDelay exceeds TimeWheelLevels")
	}

	if c.MaxTriggerdTimerAmount <= 0 {
		c.MaxTriggerdTimerAmount = 1000
	}
}

// startTimeWhell 启动时间轮.
func (s *Service) startTimeWheel() {
	s.lastTickTimeWheel = s.getTimeSystem().Now()
	s.timeWheelStop = make(chan struct{})
	go s.runTimeWheel()
	for i := 0; i < runtime.NumCPU(); i++ {
		go s.processTriggeredTimers()
	}
}

// runTimeWheel 运行时间轮.
func (s *Service) runTimeWheel() {
	ticker := time.NewTicker(s.cfg.TimeWheelLevels[0].Span)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.tickTimeWheel()
		case <-s.timeWheelStop:
			return
		}
	}
}

// stopTimeWheel 停止时间轮.
func (s *Service) stopTimeWheel() {
	close(s.timeWheelStop)
}

// StartTimer 启动定时器.
func (s *Service) StartTimer(d time.Duration, periodic bool, args any, cb TimerFunc) TimerId {
	if cb == nil {
		panic("gactor: cb is nil")
	}

	if err := s.lockState(serviceStateStarted, true); err != nil {
		return TimerIdNone
	}
	defer s.unlockState(true)

	s.mtxTimer.RLock()
	defer s.mtxTimer.RUnlock()

	now := s.getTimeSystem().Now()
	offset := now.Sub(s.lastTickTimeWheel)
	tid, err := s.timeWheel.AddTimer(gtimewheel.TimerOptions{
		Delay:    d,
		Offset:   offset,
		Periodic: periodic,
		Func:     cb,
		Args:     args,
	})
	if err != nil {
		// 更新监控数据.
		s.monitorStartTimerAmount(s.timeWheel.AddAmount())
	}

	return tid
}

// StopTimer 停止定时器.
func (s *Service) StopTimer(tid TimerId) {
	if err := s.lockState(serviceStateStarted, true); err != nil {
		return
	}
	defer s.unlockState(true)

	if s.timeWheel.RemoveTimer(tid) {
		// 更新监控数据.
		s.monitorStopTimerAmount(s.timeWheel.RemoveAmount())
	}
}

// actorTimerArgs Actor 定时器.
type actorTimerArgs struct {
	uid  ActorUID       // Actor UID.
	cb   ActorTimerFunc // 回调函数.
	args any            // 参数.
}

// startActorTimer 启动 Actor 定时器.
func (s *Service) startActorTimer(uid ActorUID, d time.Duration, periodic bool, args any, cb ActorTimerFunc) TimerId {
	if cb == nil {
		panic("gactor: cb is nil")
	}

	return s.StartTimer(d, periodic, &actorTimerArgs{
		uid:  uid,
		cb:   cb,
		args: args,
	}, s.execActorTimer)
}

// execActorTimer 执行 Actor 定时器.
func (s *Service) execActorTimer(args TimerArgs) {
	aargs := args.Args.(*actorTimerArgs)

	actor, err := s.refActor(aargs.uid)
	if err != nil || actor == nil {
		return
	}

	defer actor.core().deref()

	actor.core().receiveTriggerdTimer(args.TID, aargs.args, aargs.cb)
}

// tickTimer 推进时间轮.
func (s *Service) tickTimeWheel() {
	s.mtxTimer.Lock()

	// 计算已流逝的 tick 数.
	tickSpan := s.cfg.TimeWheelLevels[0].Span
	now := s.getTimeSystem().Now()
	elapsedTicks := int64(now.Sub(s.lastTickTimeWheel) / tickSpan)

	s.mtxTimer.Unlock()

	// 流逝时间不足一个tick.
	if elapsedTicks <= 0 {
		return
	}

	// 循环推进时间轮.
	for i := int64(0); i < elapsedTicks; i++ {
		s.mtxTimer.Lock()
		// 更新时间轮的 tick 时间.
		s.lastTickTimeWheel = s.lastTickTimeWheel.Add(tickSpan)
		// 推进时间轮.
		s.timeWheel.Tick()
		s.mtxTimer.Unlock()

		// 等待时间轮推进结束.
		s.timeWheel.TickEnd()

		// 更新定时器监控数据.
		s.monitorStartTimerAmount(s.timeWheel.AddAmount())
		s.monitorStopTimerAmount(s.timeWheel.RemoveAmount())
		s.monitorTriggerTimerAmount(s.timeWheel.TriggerAmount())
	}
}

// triggeredTimer 已触发的定时器.
type triggeredTimer struct {
	cb   TimerFunc
	args TimerArgs
}

// timerExecutor 定时器执行回调.
func (s *Service) timerExecutor(tf TimerFunc, args gtimewheel.TimerArgs) {
	select {
	case s.triggeredTimers <- triggeredTimer{
		cb:   tf,
		args: args,
	}:
	case <-s.timeWheelStop:
	}
}

// processTriggeredTimers 处理已触发的定时器..
func (s *Service) processTriggeredTimers() {
	for {
		select {
		case t := <-s.triggeredTimers:
			s.execTriggeredTimer(t)
		case <-s.timeWheelStop:
			return
		}
	}
}

// execTimer 执行定时器.
func (s *Service) execTriggeredTimer(t triggeredTimer) {
	defer recoverAndLog("exec triggered timer callback panic", s.getLogger(), nil)

	t.cb(t.args)
}
