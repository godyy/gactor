package gactor

import (
	"sync"
	"time"
)

// startTimerManager 启动定时器管理器.
func (s *Service) startTimerManager() {
	s.timerManager.start()
}

// StartTimer 启动定时器.
// Service 一旦开始停机, 调用无效.
func (s *Service) StartTimer(d time.Duration, periodic bool, args any, cb TimerFunc) TimerId {
	return s.startTimer(d, periodic, args, cb, false)
}

// startTimer 启动定时器.
// notStopped 是否在未停止状态下启动定时器.
func (s *Service) startTimer(d time.Duration, periodic bool, args any, cb TimerFunc, notStopped bool) TimerId {
	if cb == nil {
		panic("gactor: cb is nil")
	}

	if notStopped {
		if s.checkNotStopped() != nil {
			return TimerIdNone
		}
	} else {
		if s.checkStarted() != nil {
			return TimerIdNone
		}
	}

	return s.timerManager.startTimer(d, periodic, args, cb)
}

// StopTimer 停止定时器.
// 只要 Service 未完全停止, 就可以调用.
func (s *Service) StopTimer(tid TimerId) {
	if s.checkNotStopped() != nil {
		return
	}
	s.timerManager.stopTimer(tid)
}

// actorTimerArgs Actor 定时器参数.
type actorTimerArgs struct {
	uid  ActorUID       // Actor UID.
	cb   ActorTimerFunc // 回调函数.
	args any            // 参数.
}

var poolOfActorTimerArgs = &sync.Pool{
	New: func() any {
		return &actorTimerArgs{}
	},
}

func newActorTimerArgs(uid ActorUID, cb ActorTimerFunc, args any) *actorTimerArgs {
	aArgs := poolOfActorTimerArgs.Get().(*actorTimerArgs)
	aArgs.uid = uid
	aArgs.cb = cb
	aArgs.args = args
	return aArgs
}

func (a *actorTimerArgs) release() {
	a.args = nil
	a.cb = nil
	poolOfActorTimerArgs.Put(a)
}

// startActorTimer 启动 Actor 定时器.
func (s *Service) startActorTimer(uid ActorUID, d time.Duration, periodic bool, args any, cb ActorTimerFunc, notStopped bool) TimerId {
	if cb == nil {
		panic("gactor: cb is nil")
	}

	return s.startTimer(d, periodic, newActorTimerArgs(uid, cb, args), s.execActorTimer, notStopped)
}

// execActorTimer 执行 Actor 定时器.
func (s *Service) execActorTimer(args TimerArgs) {
	aargs := args.Args.(*actorTimerArgs)
	if !args.Periodic {
		defer aargs.release()
	}

	actor, err := s.refActor(aargs.uid)
	if err != nil || actor == nil {
		return
	}

	defer actor.core().deref()

	actor.core().receiveTriggerdTimer(args.TID, aargs.args, aargs.cb)
}

// tickTimeWheel 推进时间轮.
// 主要用于测试场景主动驱动时间轮。
func (s *Service) tickTimeWheel() {
	s.timerManager.tickTimeWheel()
}
