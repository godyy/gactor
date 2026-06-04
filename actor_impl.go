package gactor

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/godyy/glog"
	"github.com/godyy/gmpsc"
)

// actorImpl Actor 内部实现接口封装.
type actorImpl interface {
	Actor
	core() *actorCore
	start() error
	stop(shutdown bool) error
	stopped()
}

// actorStart Actor 启动逻辑.
func actorStart(a actorImpl) error {
	core := a.core()

	if err := core.start(); err != nil {
		return err
	}

	// 启动主循环.
	go actorLoop(a)

	return nil
}

// actorBeforeLoop 主循环前置逻辑.
func actorBeforeLoop(a actorImpl) error {
	core := a.core()
	svc := core.service()

	// 启动存续定时器.
	if core.needRecycle() {
		core.keepAliveTimerId = a.StartTimer(svc.getCfg().KeepAliveInterval, true, nil, actorOnKeepAlive)
	}

	// 启动行为.
	if err := a.Behavior().OnStart(); err != nil {
		core.getLogger().ErrorFields("OnStart failed", lfdError(err))
		a.core().service().monitorActorOnStartErr(a.core().category)
		return ErrCodeActorStartFailed
	}

	core.getLogger().DebugFields("OnStart")

	// 重置回收定时器.
	core.resetRecycleTimer(false)

	return nil
}

// actorLoop Actor 主循环逻辑.
func actorLoop(a actorImpl) {
	defer recoverAndLog("actor loop panic", a.core().getLogger(), func() {
		a.core().service().monitorActorPanic(a.core().category)
		actorStopWithErr(a, ErrCodeActorLoopError)
	})

	// 主循环前置逻辑.
	if err := actorBeforeLoop(a); err != nil {
		actorStopWithErr(a, err)
		return
	}

	core := a.core()
actor_loop_for:
	for {
		// 优先处理定时器和异步RPC回调.
	handle_timer_rpc:
		for {
			select {
			case timer := <-core.triggeredTimer:
				actorExecTimer(a, &timer)
			case call := <-core.completedAsyncRPC:
				actorInvokeAsyncRPCFunc(a, &call)
			case <-core.sigStop:
				break actor_loop_for
			default:
				break handle_timer_rpc
			}
		}

		// 处理消息, 等待前.
		if msg, ok := core.messageBox.Dequeue(); ok {
			core.waitMessage.Store(false)
			actorHandleMsg(a, msg)
			if core.messageBox.Size() <= 0 {
				core.resetRecycleTimer(false)
			}
			continue
		}

		core.waitMessage.Store(true)

		// 处理消息, 等待后.
		if msg, ok := core.messageBox.Dequeue(); ok {
			core.waitMessage.Store(false)
			actorHandleMsg(a, msg)
			if core.messageBox.Size() <= 0 {
				core.resetRecycleTimer(false)
			}
			continue
		}

		// 等待新消息的到来.
		select {
		case timer := <-core.triggeredTimer:
			actorExecTimer(a, &timer)
		case call := <-core.completedAsyncRPC:
			actorInvokeAsyncRPCFunc(a, &call)
		case <-core.messageNotify:
		case <-core.sigStop:
			break actor_loop_for
		}
	}

	// 停机处理.
	for {
		// 清空信箱.
		if a.core().service().isRunning() {
			actorDrain(a, nil)
		} else {
			actorDrain(a, ErrCodeServiceStopped)
		}

		// 检查是否可以停止.
		if err := actorCheckCouldStopped(a); err != nil {
			if errors.Is(err, errActorBeReferenced) || errors.Is(err, errActorMessageNotDrained) {
				runtime.Gosched()
				continue
			}

			core.getLogger().ErrorFields("check could stopped failed", lfdError(err))
		}

		actorStopped(a)
		return
	}
}

// actorStopWithErr 因为错误 err 停机.
func actorStopWithErr(a actorImpl, err error) {
	core := a.core()

	if err := core.stopWithErr(); err != nil {
		core.getLogger().ErrorFields("stopWithErr failed", lfdError(err))
		return
	}

	// 清空信箱.
	actorDrain(a, err)

	actorStopped(a)
}

// actorCheckCouldStopped 检查 Actor 是否能够停机.
func actorCheckCouldStopped(a actorImpl) error {
	core := a.core()

	if err := core.lockState(actorStateStopping, true); err != nil {
		return err
	}

	defer core.unlock(true)

	if core.refCount > 0 {
		return errActorBeReferenced
	}
	if core.messageBox.Size() > 0 || len(core.completedAsyncRPC) > 0 {
		return errActorMessageNotDrained
	}

	return nil
}

// actorStopped Actor 最终停机逻辑.
func actorStopped(a actorImpl) {
	core := a.core()
	svc := core.service()

	// 注销 Actor.
	actorUnregister(a)

	// 注销存续定时器.
	if core.keepAliveTimerId != TimerIdNone {
		core.StopTimer(core.keepAliveTimerId)
		core.keepAliveTimerId = TimerIdNone
	}

	a.stopped()
	svc.onActorStopped(a)
}

// actorUnregister 注销 Actor.
func actorUnregister(a actorImpl) {
	core := a.core()
	svc := core.service()
	reg := svc.getCfg().Handler.GetActorRegistry()

	// 若设置 actorFlagShutdown 标志, 或服务即将停机. 直接注销.
	// 否则, 使其在注册表中再存续一小段时间, 方便再次唤醒.
	if core.hasFlag(actorFlagShutdown) || !svc.isRunning() {
		if err := reg.UnregisterActor(ActorUnregisterParams{
			UID:     core.ActorUID(),
			NodeId:  svc.nodeId(),
			LeaseId: core.leaseID,
		}); err != nil {
			core.getLogger().ErrorFields("unregister actor failed", lfdError(err))
		}
	} else {
		if err := reg.KeepActorAlive(ActorKeepAliveParams{
			UID:     core.ActorUID(),
			NodeId:  svc.nodeId(),
			LeaseId: core.leaseID,
			TTL:     actorUnregisterThreshold,
		}); err != nil {
			core.getLogger().ErrorFields("unregister actor by keep alive failed", lfdError(err))
		}
	}
}

// actorDrain Actor 清空消息.
func actorDrain(a actorImpl, err error) {
	core := a.core()
	drained := 0
	for drained < 2 {
		select {
		case call := <-core.completedAsyncRPC:
			actorInvokeAsyncRPCFunc(a, &call)
		default:
			drained++
		}

		if msg, ok := core.messageBox.Dequeue(); ok {
			if err == nil {
				actorHandleMsg(a, msg)
			} else {
				actorHandleMsgErr(a, msg, err)
			}
		} else if core.messageBox.Size() <= 0 {
			drained++
		}
	}
}

// actorHandleMsg Actor 处理消息.
func actorHandleMsg(a actorImpl, msg message) {
	defer recoverAndLog("actor handle msg panic", a.core().getLogger(), func() {
		a.core().service().monitorActorPanic(a.core().category)
	})
	msg.handle(a)
	msg.release()
}

// actorHandleMsgErr Actor 处理消息错误.
func actorHandleMsgErr(a actorImpl, msg message, err error) {
	defer recoverAndLog("actor handle msg err panic", a.core().getLogger(), func() {
		a.core().service().monitorActorPanic(a.core().category)
	})
	msg.handleError(a, err)
	msg.release()
}

// actorExecTimer Actor 执行定时器.
func actorExecTimer(a actorImpl, timer *actorTriggeredTimer) {
	core := a.core()
	if !core.isRunning() || !core.service().isRunning() {
		return
	}
	defer recoverAndLog("actor exec timer panic", core.getLogger(), func() {
		core.service().monitorActorPanic(core.category)
	})
	args := ActorTimerArgs{
		Actor: a,
		TID:   timer.tid,
		Args:  timer.args,
	}
	timer.f(&args)
}

// actorInvokeAsyncRPCFunc 调用异步 RPC 回调.
func actorInvokeAsyncRPCFunc(a actorImpl, call *actorCompletedAsyncRPC) {
	defer recoverAndLog("actor invoke async rpc panic", a.core().getLogger(), func() {
		a.core().service().monitorActorPanic(a.core().category)
	})
	resp := RPCResp{
		svc:     a.core().service(),
		payload: call.payload,
		err:     call.err,
	}
	call.cb(a, &resp)
	resp.release()
}

// actorOnRecycle Actor 回收定时器回调.
func actorOnRecycle(args *ActorTimerArgs) {
	a := args.Actor.(actorImpl)
	if args.TID != a.core().recycleTimerId {
		return
	}

	if err := a.stop(false); err != nil {
		a.core().getLogger().ErrorFields("stop failed on recycle", lfdError(err))
	}
}

// actorOnKeepAlive Actor 存续定时器回调.
func actorOnKeepAlive(args *ActorTimerArgs) {
	a := args.Actor.(actorImpl)
	core := a.core()
	svc := core.service()
	reg := svc.getCfg().Handler.GetActorRegistry()
	if err := reg.KeepActorAlive(ActorKeepAliveParams{
		UID:     core.ActorUID(),
		NodeId:  svc.nodeId(),
		TTL:     svc.getCfg().RegistryTTL,
		LeaseId: core.leaseID,
	}); err != nil {
		core.getLogger().ErrorFields("keepalive actor failed", lfdError(err))
	}
}

var (
	// errActorBeReferenced 表示 Actor 被引用.
	errActorBeReferenced = errors.New("gactor: actor be referenced")

	// errActorNotBeReferenced 表示 Actor 未被引用.
	errActorNotBeReferenced = errors.New("gactor: actor not be referenced")

	// errActorNotStarted 表示 Actor 未启动.
	errActorNotStarted = errors.New("gactor: actor not started")

	// errActorStarted 表示 Actor 已启动.
	errActorStarted = errors.New("gactor: actor started")

	// errActorStopping 表示 Actor 正在停机.
	errActorStopping = errors.New("gactor: actor stopping")

	// errActorStopped 表示 Actor 已停机.
	errActorStopped = errors.New("gactor: actor stopped")

	// errActorMessageNotDrained 表示 Actor 消息未被处理完.
	errActorMessageNotDrained = errors.New("gactor: actor message not drained")

	// errActorAsyncRPCLimitExceeded 表示异步RPC调用数量已达到上限
	errActorAsyncRPCLimitExceeded = errors.New("gactor: actor async rpc amount exceeded limit")
)

// ErrIsActorStop error 是否表示 Actor 停机.
func ErrIsActorStop(err error) bool {
	return errors.Is(err, errActorStopping) || errors.Is(err, errActorStopped)
}

const (
	actorStateInit     = 0 // 初始状态.
	actorStateStarted  = 1 // 已启动.
	actorStateStopping = 2 // 正在停机.
	actorStateStopped  = 3 // 已停机.
)

// actorStateErrs Actor State error 映射.
var actorStateErrs = map[int8]error{
	actorStateInit:     errActorNotStarted,
	actorStateStarted:  errActorStarted,
	actorStateStopping: errActorStopping,
	actorStateStopped:  errActorStopped,
}

func actorStateErr(state int8) error {
	err, ok := actorStateErrs[state]
	if !ok {
		panic(fmt.Sprintf("gactor: invalid actor state %d", state))
	}
	return err
}

const (
	// actorFlagShutdown 表示 Actor 停止时, 直接注销 Actor, 否则, 刷新 Actor TTL, 确保
	// 在一小段时间内, 其还能在当前实例上被唤醒.
	actorFlagShutdown = int8(1 << 0)
)

// actorTriggeredTimer Actor 已触发定时器.
type actorTriggeredTimer struct {
	tid  TimerId        // 定时器ID.
	f    ActorTimerFunc // 定时器方法.
	args any            // 参数.
}

// actorCompletedAsyncRPC Actor 发起的已完成的异步 RPC 调用.
type actorCompletedAsyncRPC struct {
	payload Buffer       // 响应负载数据.
	err     error        // 错误信息.
	cb      ActorRPCFunc // 回调函数.
}

// actorCore Actor 内部核心实现.
type actorCore struct {
	*actorDefineBase                              // 集成 actorDefineBase.
	id                int64                       // Actor 分类实例ID.
	leaseID           string                      // 租约ID.
	svc               *Service                    // 隶属的 Service.
	messageBox        *gmpsc.Queue[message]       // 信箱.
	messageNotify     chan struct{}               // 信箱通知.
	waitMessage       atomic.Bool                 // 是否等待消息.
	triggeredTimer    chan actorTriggeredTimer    // 已触发的定时器.
	completedAsyncRPC chan actorCompletedAsyncRPC // 已完成的异步 RPC 调用.
	sigStop           chan struct{}               // 停机信号.
	logger            glog.Logger                 // 日志工具.

	mtx              sync.RWMutex // 读写锁.
	state            int8         // 状态.
	flag             int8         // 标志.
	refCount         int          // 引用计数. 当引用计数大于 0 时，Actor 不会被回收.
	recycleTimerId   TimerId      // 回收定时器ID.
	keepAliveTimerId TimerId      // 存续定时器ID.
}

// newActorCore 构造 actorCore.
func newActorCore(ad *actorDefineBase, id int64, leaseId string, svc *Service) *actorCore {
	a := &actorCore{
		actorDefineBase:   ad,
		id:                id,
		leaseID:           leaseId,
		svc:               svc,
		messageBox:        gmpsc.NewQueue[message](256, ad.messageBoxSize),
		messageNotify:     make(chan struct{}, 1),
		triggeredTimer:    make(chan actorTriggeredTimer, ad.maxTimerAmount),
		completedAsyncRPC: make(chan actorCompletedAsyncRPC, ad.maxAsyncRPCAmount),
		sigStop:           make(chan struct{}),
		logger: svc.oriLogger.Named("actor").
			WithFields(lfdCategoryName(ad.name), lfdId(id)),
		state:    actorStateInit,
		refCount: 0,
	}
	return a
}

func (a *actorCore) core() *actorCore {
	return a
}

func (a *actorCore) Category() uint16 {
	return a.category
}

func (a *actorCore) ActorUID() ActorUID {
	return ActorUID{
		Category: a.category,
		ID:       a.id,
	}
}

// lockState 锁定状态.
func (a *actorCore) lockState(needState int8, read bool) error {
	if read {
		a.mtx.RLock()
	} else {
		a.mtx.Lock()
	}

	state := a.state
	if state == needState {
		return nil
	}

	if read {
		a.mtx.RUnlock()
	} else {
		a.mtx.Unlock()
	}

	return actorStateErr(state)
}

// lockNotStopped 锁定未停止状态.
func (a *actorCore) lockNotStopped(read bool) error {
	if read {
		a.mtx.RLock()
	} else {
		a.mtx.Lock()
	}

	state := a.state
	if state == actorStateStarted || state == actorStateStopping {
		return nil
	}

	if read {
		a.mtx.RUnlock()
	} else {
		a.mtx.Unlock()
	}

	return actorStateErr(state)
}

// unlock 解锁.
func (a *actorCore) unlock(read bool) {
	if read {
		a.mtx.RUnlock()
	} else {
		a.mtx.Unlock()
	}
}

// isRunning 返回是否运行中.
func (a *actorCore) isRunning() bool {
	a.mtx.RLock()
	defer a.mtx.RUnlock()
	return a.state == actorStateStarted
}

// checkStarted 检查是运行中.
func (a *actorCore) checkStarted() error {
	if err := a.lockState(actorStateStarted, true); err != nil {
		return err
	}
	a.unlock(true)
	return nil
}

// checkNotStopped 检查是否未停止.
func (a *actorCore) checkNotStopped() error {
	if err := a.lockNotStopped(true); err != nil {
		return err
	}
	a.unlock(true)
	return nil
}

// updateFlag 更新标志
func (a *actorCore) updateFlag(flag int8, set bool) {
	if set {
		a.flag |= flag
	} else {
		a.flag &= ^flag
	}
}

// hasFlag 返回是否有标志.
func (a *actorCore) hasFlag(flag int8) bool {
	return a.flag&flag != 0
}

// getLogger 返回 getLogger.
func (a *actorCore) getLogger() glog.Logger {
	return a.logger
}

// service 返回 Service.
func (a *actorCore) service() *Service {
	return a.svc
}

// start 启动.
func (a *actorCore) start() error {
	if err := a.lockState(actorStateInit, false); err != nil {
		return err
	}
	defer a.unlock(false)

	// 更新状态.
	a.state = actorStateStarted

	return nil
}

// stop 开始停机逻辑.
func (a *actorCore) stop(shutdown bool) error {
	if err := a.lockState(actorStateStarted, false); err != nil {
		return err
	}
	defer a.unlock(false)

	// 不用管引用计数.

	// 开始停机.
	a.state = actorStateStopping
	if shutdown {
		a.updateFlag(actorFlagShutdown, true)
	}
	close(a.sigStop)
	return nil
}

// stopWithErr 因为错误停机.
func (a *actorCore) stopWithErr() error {
	// 锁定状态.
	if err := a.lockState(actorStateStarted, false); err != nil {
		return err
	}
	a.state = actorStateStopping
	a.unlock(false)
	return nil
}

// stopped 最终停机逻辑.
func (a *actorCore) stopped(f func()) {
	if err := a.lockState(actorStateStopping, false); err != nil {
		a.getLogger().ErrorFields("lock state failed when stopped", lfdError(err))
		return
	}
	a.svc = nil
	a.messageBox.Close()
	a.messageBox = nil
	close(a.messageNotify)
	a.messageNotify = nil
	a.sigStop = nil
	if a.completedAsyncRPC != nil {
		close(a.completedAsyncRPC)
		a.completedAsyncRPC = nil
	}
	if f != nil {
		f()
	}
	a.state = actorStateStopped
	a.unlock(false)
}

// ref 引用.
func (a *actorCore) ref() error {
	if err := a.lockState(actorStateStarted, false); err != nil {
		return err
	}
	defer a.unlock(false)

	a.refCount += 1
	return nil
}

// ref 解除引用.
func (a *actorCore) deref() error {
	if err := a.lockNotStopped(false); err != nil {
		return err
	}
	defer a.unlock(false)

	if a.refCount <= 0 {
		return errActorNotBeReferenced
	}

	a.refCount -= 1

	return nil
}

// receiveMessage 接收消息.
func (a *actorCore) receiveMessage(msg message) error {
	if err := a.checkStarted(); err != nil {
		return err
	}

	if !a.messageBox.Enqueue(msg) {
		return ErrCodeActorBusy
	}

	if a.waitMessage.Load() {
		select {
		case a.messageNotify <- struct{}{}:
		default:
		}
	}

	return nil
}

// receiveCompletedAsyncRPC 接收已完成的异步 RPC 调用.
func (a *actorCore) receiveCompletedAsyncRPC(resp *RPCResp, cb ActorRPCFunc) error {
	if err := a.checkNotStopped(); err != nil {
		return err
	}

	asyncCall := actorCompletedAsyncRPC{
		payload: resp.payload,
		err:     resp.err,
		cb:      cb,
	}
	resp.payload.SetBuf(nil)

	const alarmThreshold = time.Millisecond * 50
	begin := time.Now()
	a.completedAsyncRPC <- asyncCall
	if d := time.Now().Sub(begin); d > alarmThreshold {
		a.getLogger().Warnf("receiveCompletedAsyncRPC cost:%dms", d.Milliseconds())
	}
	return nil
}

// resetRecycleTimer 重置回收定时器.
func (a *actorCore) resetRecycleTimer(stop bool) {
	if !a.needRecycle() {
		return
	}

	if a.recycleTimerId != TimerIdNone {
		a.StopTimer(a.recycleTimerId)
		a.recycleTimerId = TimerIdNone
	}

	if !stop {
		a.recycleTimerId = a.StartTimer(a.recycleTime, false, nil, actorOnRecycle)
	}
}

// StartTimer 启动定时器.
func (a *actorCore) StartTimer(d time.Duration, periodic bool, args any, cb ActorTimerFunc) TimerId {
	if a.checkStarted() != nil {
		return TimerIdNone
	}

	return a.svc.startActorTimer(a.ActorUID(), d, periodic, args, cb)
}

// StopTimer 停止定时器.
func (a *actorCore) StopTimer(tid TimerId) {
	a.svc.StopTimer(tid)
}

// receiveTriggerdTimer 接收已触发的定时器.
func (a *actorCore) receiveTriggerdTimer(tid TimerId, args any, cb ActorTimerFunc) {
	if a.checkStarted() != nil {
		return
	}

	const alarmThreshold = time.Millisecond * 50
	timer := actorTriggeredTimer{
		tid:  tid,
		f:    cb,
		args: args,
	}
	begin := time.Now()
	select {
	case a.triggeredTimer <- timer:
		if d := time.Now().Sub(begin); d > alarmThreshold {
			a.getLogger().Warnf("receive triggerd timer cost:%dms", d.Milliseconds())
		}
		return
	case <-a.sigStop:
		return
	}
}

// RPCWithDeadline 发起同步 RPC 调用.
// deadline 为超时时间.
func (a *actorCore) RPCWithDeadline(to ActorUID, params, reply any, deadline time.Time) error {
	return a.svc.rpcWithDeadline(a.ActorUID(), to, params, reply, deadline)
}

// RPCWithTimeout 发起同步 RPC 调用.
// timeout 为超时间隔.
func (a *actorCore) RPCWithTimeout(to ActorUID, params, reply any, timeout time.Duration) error {
	return a.svc.rpcWithTimeout(a.ActorUID(), to, params, reply, timeout)
}

// RPC 发起同步 RPC 调用.
// 使用配置的默认超时间隔.
func (a *actorCore) RPC(to ActorUID, params, reply any) error {
	return a.svc.rpc(a.ActorUID(), to, params, reply)
}

// RPCWithContext 发起同步 RPC 调用.
// 超时 deadline 从 ctx 获取，若未设置, 使用默认超时时间.
func (a *actorCore) RPCWithContext(ctx context.Context, to ActorUID, params, reply any) error {
	return a.svc.rpcWithContext(ctx, a.ActorUID(), to, params, reply)
}

// actorAsyncRPCFunc Actor 异步 RPC 回调封装.
type actorAsyncRPCFunc struct {
	svc *Service
	uid ActorUID
	cb  ActorRPCFunc
}

func (f *actorAsyncRPCFunc) invoke(resp *RPCResp) {
	if actor, err := f.svc.getActor(f.uid); err != nil {
		f.svc.getLogger().ErrorFields("get actor failed inside actorAsyncRPCFunc", f.svc.lfdActorUID("uid", f.uid), lfdError(err))
	} else if actor == nil {
		f.svc.getLogger().WarnFields("actor not found inside actorAsyncRPCFunc", f.svc.lfdActorUID("uid", f.uid))
	} else {
		defer actor.core().deref()
		if err := actor.core().receiveCompletedAsyncRPC(resp, f.cb); err != nil {
			actor.core().getLogger().ErrorFields("receive async rpc call failed", lfdError(err))
		}
	}
}

// AsyncRPCWithDeadline 发起异步 RPC 调用.
// deadline 为超时时间.
func (a *actorCore) AsyncRPCWithDeadline(to ActorUID, params any, cb ActorRPCFunc, deadline time.Time) error {
	if err := a.ref(); err != nil {
		return err
	}

	asyncFunc := &actorAsyncRPCFunc{
		svc: a.svc,
		uid: a.ActorUID(),
		cb:  cb,
	}

	if err := a.svc.asyncRPCWithDeadline(a.ActorUID(), to, params, asyncFunc.invoke, deadline); err != nil {
		_ = a.deref()
		return err
	}

	return nil
}

// AsyncRPCWithTimeout 发起异步 RPC 调用.
// timeout 为超时间隔.
func (a *actorCore) AsyncRPCWithTimeout(to ActorUID, params any, cb ActorRPCFunc, timeout time.Duration) error {
	return a.AsyncRPCWithDeadline(to, params, cb, time.Now().Add(timeout))
}

// AsyncRPC 发起异步 RPC 调用.
// 使用配置的默认超时间隔.
func (a *actorCore) AsyncRPC(to ActorUID, params any, cb ActorRPCFunc) error {
	return a.AsyncRPCWithDeadline(to, params, cb, time.Now().Add(a.svc.cfg.DefRPCTimeout))
}

// AsyncRPCWithContext 发起异步 RPC 调用.
// 超时 deadline 从 ctx 获取，若未设置, 使用默认超时时间.
func (a *actorCore) AsyncRPCWithContext(ctx context.Context, to ActorUID, params any, cb ActorRPCFunc) error {
	if err := a.ref(); err != nil {
		return err
	}

	asyncFunc := &actorAsyncRPCFunc{
		svc: a.svc,
		uid: a.ActorUID(),
		cb:  cb,
	}

	if err := a.svc.asyncRPCWithContext(ctx, a.ActorUID(), to, params, asyncFunc.invoke); err != nil {
		_ = a.deref()
		return err
	}

	return nil
}

// Cast 投递消息.
func (a *actorCore) Cast(to ActorUID, payload any) error {
	return a.svc.cast(a.ActorUID(), to, payload)
}

// actor Actor 内部实现.
type actor struct {
	*actorCore
	behavior ActorBehavior
}

func (a *actor) start() error {
	return actorStart(a)
}

func (a *actor) stopped() {
	if err := a.Behavior().OnStop(); err != nil {
		a.getLogger().ErrorFields("OnStop failed", lfdError(err))
		a.service().monitorActorOnStopErr(a.category)
	} else {
		a.getLogger().DebugFields("OnStop")
	}

	a.actorCore.stopped(a.onStopped)
}

func (a *actor) onStopped() {
	a.behavior = nil
}

func (a *actor) Behavior() ActorBehavior {
	return a.behavior
}

// cactor CActor 内部实现.
type cactor struct {
	*actorCore
	behavior CActorBehavior
	session  ActorSession
}

func (a *cactor) stopped() {
	a.Disconnect()

	if err := a.Behavior().OnStop(); err != nil {
		a.getLogger().ErrorFields("OnStop failed", lfdError(err))
	} else {
		a.getLogger().DebugFields("OnStop")
	}

	a.actorCore.stopped(a.onStopped)
}

func (a *cactor) onStopped() {
	a.behavior = nil
}

func (a *cactor) Behavior() ActorBehavior {
	return a.behavior
}

func (a *cactor) Session() ActorSession {
	return a.session
}

func (a *cactor) start() error {
	return actorStart(a)
}

func (a *cactor) updateSession(session ActorSession) {
	if a.session.IsConnected() {
		if a.session == session {
			return
		}
		a.Disconnect()
	}

	a.getLogger().DebugFields("connect", lfdSession(session))
	a.session = session
	a.behavior.OnConnected()
}

// PushRawMessage 向客户端推送消息.
func (a *cactor) PushRawMessage(payload any) error {
	if a.session.NodeId == "" {
		return ErrCodeActorNotConnected
	}
	ph := newRawPushHead(a.service().genSeq(), a.id, a.session.SID)
	return a.svc.sendRemotePacket(a.session.NodeId, &ph, payload)
}

// Disconnect 端开与客户端的连接.
func (a *cactor) Disconnect() {
	if !a.session.IsConnected() {
		return
	}

	ph := newDisconnectHead(a.svc.genSeq(), a.id, a.session.SID)
	if err := a.svc.sendRemotePacket(a.session.NodeId, &ph, nil); err != nil {
		a.getLogger().ErrorFields("send disconnect packet failed", lfdSession(a.session), lfdError(err))
	} else {
		a.getLogger().DebugFields("disconnect", lfdSession(a.session))
	}

	a.session.reset()
	a.behavior.OnDisconnected()
}

// onDisconnect 处理客户端端开链接.
func (a *cactor) onDisconnect(session ActorSession) {
	if session != a.session {
		return
	}
	a.getLogger().Debug("onDisconnect")
	a.session.reset()
	a.behavior.OnDisconnected()
}
