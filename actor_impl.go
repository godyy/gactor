package gactor

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/godyy/glog"
)

// actorImpl Actor 内部实现接口封装.
type actorImpl interface {
	Actor
	core() *actorCore
	start() error
	stop() error
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

// actorStopped Actor 最终停机逻辑.
func actorStopped(a actorImpl) {
	core := a.core()
	svc := core.service()
	a.stopped()
	svc.onActorStopped(a)
}

// actorBeforeLoop 主循环前置逻辑.
func actorBeforeLoop(a actorImpl) error {
	core := a.core()

	// 启动行为.
	if err := a.Behavior().OnStart(); err != nil {
		core.getLogger().ErrorFields("OnStart failed", lfdError(err))
		return errCodeStartActorFailed
	}

	core.getLogger().DebugFields("OnStart")

	// 重置回收定时器.
	core.resetRecycleTimer(false)

	return nil
}

// actorLoop Actor 主循环逻辑.
func actorLoop(a actorImpl) {
	// recover.
	defer func() {
		if err := recover(); err != nil {
			a.core().getLogger().ErrorFields("loop panic", lfdPanic(err))
			actorStopWithErr(a, errCodeActorLoopError)
		}
	}()

	// 主循环前置逻辑.
	if err := actorBeforeLoop(a); err != nil {
		actorStopWithErr(a, err)
		return
	}

	core := a.core()

	// 消费逻辑.
	for {
		select {
		case msg := <-core.messageBox:
			// 停止回收定时器.
			core.resetRecycleTimer(true)

			// 处理消息.
			if err := msg.handle(a); err != nil {
				core.getLogger().ErrorFields("handle message failed", lfdError(err))
			}
			msg.release()

			// 若信箱已清空, 重置回收定时器.
			if len(core.messageBox) == 0 {
				core.resetRecycleTimer(false)
			}

		case timer := <-core.triggeredTimer:
			// 处理定时器.
			actorExecTimer(a, &timer)

		case call := <-core.completedAsyncRPC:
			// 处理异步 RPC 调用.
			actorInvokeAsyncRPCFunc(a, &call)

		case <-core.sigPrepareStop:
			// 清空信箱.
			actorDrain(a, nil)

			// 尝试 stopping.
			if err := core.prepareStopping(); err != nil {
				if errors.Is(err, ErrActorBeReferenced) || errors.Is(err, ErrActorMessageNotDrained) {
					runtime.Gosched()
					continue
				}
				core.getLogger().ErrorFields("prepare stopping failed", lfdError(err))
			}

			actorStopped(a)
			return
		}
	}
}

// actorOnRecycle Actor 回收定时器回调.
func actorOnRecycle(args *ActorTimerArgs) {
	a := args.Actor.(actorImpl)
	if args.TID != a.core().recycleTimerId {
		return
	}

	if err := a.stop(); err != nil {
		a.core().getLogger().ErrorFields("stop failed on recycle", lfdError(err))
	}
}

// actorDrain Actor 清空消息.
func actorDrain(a actorImpl, err error) {
	core := a.core()
	drained := false
	for !drained {
		select {
		case msg := <-core.messageBox:
			if err == nil {
				if err := msg.handle(a); err != nil {
					core.getLogger().ErrorFields("handle message failed", lfdError(err))
				}
			} else {
				if err := msg.handleError(a, err); err != nil {
					core.getLogger().ErrorFields("handle message error failed", lfdError(err))
				}
			}
			msg.release()
		case call := <-core.completedAsyncRPC:
			actorInvokeAsyncRPCFunc(a, &call)
		default:
			drained = true
		}
	}
}

// actorExecTimer Actor 执行定时器.
func actorExecTimer(a actorImpl, timer *actorTriggeredTimer) {
	core := a.core()
	if !core.isRunning() || !core.service().isRunning() {
		return
	}
	args := ActorTimerArgs{
		Actor: a,
		TID:   timer.tid,
		Args:  timer.args,
	}
	timer.f(&args)
}

// actorInvokeAsyncRPCFunc 调用异步 RPC 回调.
func actorInvokeAsyncRPCFunc(a actorImpl, call *actorCompletedAsyncRPC) {
	resp := RPCResp{
		svc:     a.core().service(),
		payload: call.payload,
		err:     call.err,
	}
	call.cb(a, &resp)
	resp.release()
}

var (

	// ErrActorNotConnected 表示 Actor 未连接.
	ErrActorNotConnected = errors.New("gactor: actor not connected")

	// ErrActorBeReferenced 表示 Actor 被引用.
	ErrActorBeReferenced = errors.New("gactor: actor be referenced")

	// ErrActorNotBeReferenced 表示 Actor 未被引用.
	ErrActorNotBeReferenced = errors.New("gactor: actor not be referenced")

	// ErrNotCActor 表示不是 CActor.
	ErrNotCActor = errors.New("gactor: not CActor")

	// ErrActorNotStarted 表示 Actor 未启动.
	ErrActorNotStarted = errors.New("gactor: actor not started")

	// ErrActorStarted 表示 Actor 已启动.
	ErrActorStarted = errors.New("gactor: actor started")

	// ErrActorPrepareStop 表示 Actor 准备停机.
	ErrActorPrepareStop = errors.New("gactor: actor prepare stop")

	// ErrActorStopping 表示 Actor 正在停机.
	ErrActorStopping = errors.New("gactor: actor stopping")

	// ErrActorStopped 表示 Actor 已停机.
	ErrActorStopped = errors.New("gactor: actor stopped")

	// ErrActorMessageNotDrained 表示 Actor 消息未被处理完.
	ErrActorMessageNotDrained = errors.New("gactor: actor message not drained")
)

const (
	actorStateInit        = 0 // 初始状态.
	actorStateStarted     = 1 // 已启动.
	actorStatePrepareStop = 2 // 准备停机.
	actorStateStopping    = 3 // 正在停机.
	actorStateStopped     = 4 // 已停机.
)

// actorStateErrs Actor State error 映射.
var actorStateErrs = map[int8]error{
	actorStateInit:        ErrActorNotStarted,
	actorStateStarted:     ErrActorStarted,
	actorStatePrepareStop: ErrActorPrepareStop,
	actorStateStopping:    ErrActorStopping,
	actorStateStopped:     ErrActorStopped,
}

func actorStateErr(state int8) error {
	err, ok := actorStateErrs[state]
	if !ok {
		panic(fmt.Sprintf("gactor: invalid actor state %d", state))
	}
	return err
}

// actorTriggeredTimer Actor 已触发定时器.
type actorTriggeredTimer struct {
	tid  TimerId        // 定时器ID.
	f    ActorTimerFunc // 定时器方法.
	args any            // 参数.
}

// actorCompletedAsyncRPC Actor 发起的已完成的异步 RPC 调用.
type actorCompletedAsyncRPC struct {
	payload Packet       // 响应负载数据.
	err     error        // 错误信息.
	cb      ActorRPCFunc // 回调函数.
}

// actorCore Actor 内部核心实现.
type actorCore struct {
	*ActorDefineCommon                             // 集成 ActorDefineCommon.
	id                 int64                       // Actor 分类实例ID.
	svc                *Service                    // 隶属的 Service.
	messageBox         chan message                // 信箱.
	triggeredTimer     chan actorTriggeredTimer    // 已触发的定时器.
	completedAsyncRPC  chan actorCompletedAsyncRPC // 已完成的异步 RPC 调用.
	sigPrepareStop     chan struct{}               // 准备停机信号.
	logger             glog.Logger                 // 日志工具.

	mtx            sync.RWMutex // 读写锁.
	state          int8         // 状态.
	refCount       int          // 引用计数. 当引用计数大于 0 时，Actor 不会被回收.
	recycleTimerId TimerId      // 回收定时器ID.
}

// newActorCore 构造 actorCore.
func newActorCore(ad *ActorDefineCommon, id int64, svc *Service) *actorCore {
	a := &actorCore{
		ActorDefineCommon: ad,
		id:                id,
		svc:               svc,
		messageBox:        make(chan message, ad.MessageBoxSize),
		triggeredTimer:    make(chan actorTriggeredTimer, ad.MaxTriggeredTimerAmount),
		completedAsyncRPC: make(chan actorCompletedAsyncRPC, ad.MaxCompletedAsyncRPCAmount),
		sigPrepareStop:    make(chan struct{}),
		logger: svc.oriLogger.Named("actor").
			WithFields(lfdCategory(ad.Category)).
			WithFields(lfdId(id)),
		state:    actorStateInit,
		refCount: 0,
	}
	return a
}

func (a *actorCore) core() *actorCore {
	return a
}

func (a *actorCore) ActorUID() ActorUID {
	return ActorUID{
		Category: a.Category,
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
func (a *actorCore) stop() error {
	if err := a.lockState(actorStateStarted, false); err != nil {
		return err
	}
	defer a.unlock(false)

	// 若仍被引用, 无法停机.
	if a.refCount > 0 {
		return ErrActorBeReferenced
	}

	// 进入停机状态.
	a.state = actorStatePrepareStop
	close(a.sigPrepareStop)
	return nil
}

// prepareStopping 尝试从 actorStatePrepareStop 进入 actorStateStopping 状态.
func (a *actorCore) prepareStopping() error {
	// 锁定状态, 检查是否未被引用, 且信箱已清空.
	// 上述条件均满足即可完成停机.
	if err := a.lockState(actorStatePrepareStop, false); err != nil {
		a.getLogger().ErrorFields("prepare stop, but lock state failed", lfdError(err))
		return err
	}
	if a.refCount > 0 {
		a.unlock(false)
		return ErrActorBeReferenced
	}
	if len(a.messageBox) > 0 {
		a.unlock(false)
		return ErrActorMessageNotDrained
	}
	a.state = actorStateStopping
	a.unlock(false)

	return nil
}

// stopWithErr 因为错误停机.
func (a *actorCore) stopWithErr() error {
	// 锁定状态.
	if err := a.lockState(actorStateStarted, false); err != nil {
		if err := a.lockState(actorStatePrepareStop, false); err != nil {
			return err
		}
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
	close(a.messageBox)
	a.messageBox = nil
	a.sigPrepareStop = nil
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

// lockRef 锁定可以被引用的状态.
func (a *actorCore) lockRef(read bool) error {
	if read {
		a.mtx.RLock()
	} else {
		a.mtx.Lock()
	}

	state := a.state
	if state == actorStateStarted || state == actorStatePrepareStop {
		return nil
	}

	if read {
		a.mtx.RUnlock()
	} else {
		a.mtx.Unlock()
	}

	return actorStateErr(state)
}

// ref 引用.
func (a *actorCore) ref() error {
	if err := a.lockRef(false); err != nil {
		return err
	}
	defer a.unlock(false)

	a.refCount += 1
	return nil
}

// ref 解除引用.
func (a *actorCore) deref() error {
	if err := a.lockRef(false); err != nil {
		return err
	}
	defer a.unlock(false)

	if a.refCount <= 0 {
		return ErrActorNotBeReferenced
	}

	a.refCount -= 1

	return nil
}

// receiveMessage 接收消息.
func (a *actorCore) receiveMessage(ctx context.Context, msg message) error {
	if err := a.lockRef(true); err != nil {
		return err
	}
	defer a.unlock(true)

	if err := ctx.Err(); err != nil {
		return err
	}

	select {
	case a.messageBox <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// receiveCompletedAsyncRPC 接收已完成的异步 RPC 调用.
func (a *actorCore) receiveCompletedAsyncRPC(ctx context.Context, resp *RPCResp, cb ActorRPCFunc) error {
	if err := a.lockRef(true); err != nil {
		return err
	}
	defer a.unlock(true)

	if err := ctx.Err(); err != nil {
		return err
	}

	asyncCall := actorCompletedAsyncRPC{
		payload: resp.payload,
		err:     resp.err,
		cb:      cb,
	}
	resp.payload = nil

	select {
	case a.completedAsyncRPC <- asyncCall:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// resetRecycleTimer 重置回收定时器.
func (a *actorCore) resetRecycleTimer(stop bool) {
	if a.RecycleTime == 0 {
		return
	}

	if a.recycleTimerId != TimerIdNone {
		a.StopTimer(a.recycleTimerId)
		a.recycleTimerId = TimerIdNone
	}

	if !stop {
		a.recycleTimerId = a.StartTimer(a.RecycleTime, false, nil, actorOnRecycle)
	}
}

// StartTimer 启动定时器.
func (a *actorCore) StartTimer(d time.Duration, repeat bool, args any, cb ActorTimerFunc) TimerId {
	if err := a.lockState(actorStateStarted, true); err != nil {
		return 0
	}
	defer a.unlock(true)

	return a.svc.startActorTimer(a.ActorUID(), d, repeat, args, cb)
}

// StopTimer 停止定时器.
func (a *actorCore) StopTimer(tid TimerId) {
	a.svc.StopTimer(tid)
}

// receiveTriggerdTimer 接收已触发的定时器.
func (a *actorCore) receiveTriggerdTimer(tid TimerId, args any, cb ActorTimerFunc) {
	if err := a.lockState(actorStateStarted, true); err != nil {
		return
	}
	defer a.unlock(true)

	timer := actorTriggeredTimer{
		tid:  tid,
		f:    cb,
		args: args,
	}
	select {
	case a.triggeredTimer <- timer:
		return
	case <-a.sigPrepareStop:
		return
	}
}

// RPC 发起同步 RPC 调用.
func (a *actorCore) RPC(ctx context.Context, to ActorUID, params, reply any) error {
	return a.svc.rpc(ctx, to, params, reply)
}

// actorAsyncRPCFunc Actor 异步 RPC 回调封装.
type actorAsyncRPCFunc struct {
	actor actorImpl
	cb    ActorRPCFunc
}

func (f *actorAsyncRPCFunc) invoke(resp *RPCResp) {
	defer f.actor.core().deref()
	ctx, cancel := context.WithTimeout(context.Background(), f.actor.core().service().getCfg().ActorReceiveCompletedAsyncRPCTimeout)
	defer cancel()
	if err := f.actor.core().receiveCompletedAsyncRPC(ctx, resp, f.cb); err != nil {
		f.actor.core().getLogger().ErrorFields("receive async rpc call failed", lfdError(err))
	}
}

// asyncRPC 异步 RPC 调用核心实现.
func (a *actorCore) asyncRPC(ctx context.Context, impl actorImpl, to ActorUID, params any, cb ActorRPCFunc) error {
	if err := impl.core().ref(); err != nil {
		return err
	}

	asyncFunc := &actorAsyncRPCFunc{
		actor: impl,
		cb:    cb,
	}

	if err := a.svc.asyncRPC(ctx, to, params, asyncFunc.invoke); err != nil {
		_ = impl.core().deref()
		return err
	}

	return nil
}

// Cast 投递消息.
func (a *actorCore) Cast(ctx context.Context, to ActorUID, payload any) error {
	return a.svc.cast(ctx, to, payload)
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

// AsyncRPC 发起异步 RPC 调用.
func (a *actor) AsyncRPC(ctx context.Context, to ActorUID, params any, cb ActorRPCFunc) error {
	return a.asyncRPC(ctx, a, to, params, cb)
}

// cActor CActor 内部实现.
type cActor struct {
	*actorCore
	behavior CActorBehavior
	session  ActorSession
}

func (a *cActor) stopped() {
	a.Disconnect(context.Background())

	if err := a.Behavior().OnStop(); err != nil {
		a.getLogger().ErrorFields("OnStop failed", lfdError(err))
	} else {
		a.getLogger().DebugFields("OnStop")
	}

	a.actorCore.stopped(a.onStopped)
}

func (a *cActor) onStopped() {
	a.behavior = nil
}

func (a *cActor) Behavior() ActorBehavior {
	return a.behavior
}

func (a *cActor) CBehavior() CActorBehavior {
	return a.behavior
}

func (a *cActor) Session() ActorSession {
	return a.session
}

// AsyncRPC 发起异步 RPC 调用.
func (a *cActor) AsyncRPC(ctx context.Context, to ActorUID, params any, cb ActorRPCFunc) error {
	return a.asyncRPC(ctx, a, to, params, cb)
}

func (a *cActor) start() error {
	return actorStart(a)
}

func (a *cActor) updateSession(ctx context.Context, session ActorSession) {
	if a.session == session {
		return
	}

	if a.session.NodeId != session.NodeId {
		a.Disconnect(ctx)
	}

	a.session = session
	a.behavior.OnConnected()
	a.getLogger().Debug("connected")
}

// PushRawMessage 向客户端推送消息.
func (a *cActor) PushRawMessage(ctx context.Context, payload any) error {
	if a.session.NodeId == "" {
		return ErrActorNotConnected
	}
	ph := rawPushPacketHead{fromId: a.ActorUID(), sid: a.session.SID}
	return a.svc.sendPacket(ctx, a.session.NodeId, &ph, payload)
}

// Disconnect 端开与客户端的连接.
func (a *cActor) Disconnect(ctx context.Context) {
	if !a.session.IsConnected() {
		return
	}

	ph := s2sDisconnectedPacketHead{
		uid: a.ActorUID(),
		sid: a.session.SID,
	}
	if err := a.svc.sendPacket(ctx, a.session.NodeId, &ph, nil); err != nil {
		a.getLogger().ErrorFields("send disconnect packet failed", lfdSession(a.session), lfdError(err))
	}

	a.session.reset()
	a.behavior.OnDisconnected()
	a.getLogger().Debug("disconnected")
}

// onDisconnect 处理客户端端开链接.
func (a *cActor) onDisconnect(session ActorSession) {
	if session != a.session {
		return
	}
	a.session.reset()
	a.behavior.OnDisconnected()
	a.getLogger().Debug("disconnected")
}
