package gactor

import (
	"time"
)

// startTimerManager 启动定时器管理器.
func (s *Service) startTimerManager() {
	s.timerManager.start()
}

// StartTimer 启动定时器.
// Service 一旦开始停机, 调用无效.
func (s *Service) StartTimer(d time.Duration, periodic bool, cb TimerFunc, cbctx any) TimerId {
	return s.startTimer(d, periodic, cb, cbctx, false)
}

// startTimer 启动定时器.
// notStopped 是否在未停止状态下启动定时器.
func (s *Service) startTimer(d time.Duration, periodic bool, cb TimerFunc, cbctx any, notStopped bool) TimerId {
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

	return s.timerManager.startTimer(d, periodic, cb, cbctx)
}

// StopTimer 停止定时器.
// 只要 Service 未完全停止, 就可以调用.
func (s *Service) StopTimer(tid TimerId) {
	if s.checkNotStopped() != nil {
		return
	}
	s.timerManager.stopTimer(tid)
}

// cbctxActorTimer 构造 Actor 定时器回调上下文.
func (s *Service) cbctxActorTimer(uid ActorUID, cb ActorTimerFunc, cbctx any) CbCtx {
	return NewCbCtx(cbctxKeyUID, uid, cbctxKeyCallback, cb, cbctxKeyCbCtx, cbctx)
}

// startActorTimer 启动 Actor 定时器.
func (s *Service) startActorTimer(uid ActorUID, d time.Duration, periodic bool, cb ActorTimerFunc, cbctx any, notStopped bool) TimerId {
	if cb == nil {
		panic("gactor: cb is nil")
	}

	return s.startTimer(d, periodic, s.execActorTimer, s.cbctxActorTimer(uid, cb, cbctx), notStopped)
}

// execActorTimer 执行 Actor 定时器.
func (s *Service) execActorTimer(args TimerArgs) {
	cbctx := args.Ctx.(CbCtx)
	if !args.Periodic {
		defer cbctx.Release()
	}

	uid, _ := CbCtxGet[ActorUID](cbctx, cbctxKeyUID)
	cb, _ := CbCtxGet[ActorTimerFunc](cbctx, cbctxKeyCallback)
	innerCbCtx, _ := CbCtxGet[any](cbctx, cbctxKeyCbCtx)

	actor, err := s.refActor(uid)
	if err != nil || actor == nil {
		return
	}

	defer actor.core().deref()

	actor.core().receiveTriggerdTimer(args.TID, cb, innerCbCtx)
}

// tickTimeWheel 推进时间轮.
// 主要用于测试场景主动驱动时间轮。
func (s *Service) tickTimeWheel() {
	s.timerManager.tickTimeWheel()
}
