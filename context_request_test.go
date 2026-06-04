package gactor

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/zap/zapcore"
)

type contextRequestTestRequest struct {
	rt               RequestType
	from             ActorUID
	deadlineValue    time.Time
	hasDeadline      bool
	replyCount       int
	replyDecodeCount int
	cloneSource      *contextRequestTestRequest
}

func (r *contextRequestTestRequest) requestType() RequestType {
	return r.rt
}

func (r *contextRequestTestRequest) fromActorUID() ActorUID {
	return r.from
}

func (r *contextRequestTestRequest) deadline() (time.Time, bool) {
	return r.deadlineValue, r.hasDeadline
}

func (r *contextRequestTestRequest) isTimeout(now time.Time) bool {
	return false
}

func (r *contextRequestTestRequest) decode(ctx *Context, v any) error {
	return nil
}

func (r *contextRequestTestRequest) reply(ctx *Context, payload any, ec ErrCode) error {
	r.replyCount++
	return nil
}

func (r *contextRequestTestRequest) replyDecodeError(ctx *Context) error {
	r.replyDecodeCount++
	return nil
}

func (r *contextRequestTestRequest) clone(ctx *Context) request {
	cp := *r
	cp.cloneSource = r
	return &cp
}

func (r *contextRequestTestRequest) release(ctx *Context) {}

func (r *contextRequestTestRequest) beforeHandle(ctx *Context) error {
	return nil
}

func (r *contextRequestTestRequest) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	return nil
}

type contextRequestTestActor struct {
	uid ActorUID

	rpcCalled           bool
	rpcDeadlineCalled   bool
	rpcDeadline         time.Time
	asyncCalled         bool
	asyncDeadlineCalled bool
	asyncDeadline       time.Time
	lastAsyncCallback   ActorRPCFunc
	asyncErr            error
	asyncDeadlineErr    error
}

func (a *contextRequestTestActor) core() *actorCore { return nil }
func (a *contextRequestTestActor) start() error     { return nil }
func (a *contextRequestTestActor) stop(shutdown bool) error {
	return nil
}
func (a *contextRequestTestActor) stopped() {}
func (a *contextRequestTestActor) Behavior() ActorBehavior {
	return nil
}
func (a *contextRequestTestActor) Category() uint16 {
	return a.uid.Category
}
func (a *contextRequestTestActor) ActorUID() ActorUID {
	return a.uid
}
func (a *contextRequestTestActor) StartTimer(d time.Duration, periodic bool, args any, cb ActorTimerFunc) TimerId {
	return TimerIdNone
}
func (a *contextRequestTestActor) StopTimer(TimerId) {}
func (a *contextRequestTestActor) RPCWithDeadline(to ActorUID, params, reply any, deadline time.Time) error {
	a.rpcDeadlineCalled = true
	a.rpcDeadline = deadline
	return nil
}
func (a *contextRequestTestActor) RPCWithTimeout(to ActorUID, params, reply any, timeout time.Duration) error {
	return nil
}
func (a *contextRequestTestActor) RPC(to ActorUID, params, reply any) error {
	a.rpcCalled = true
	return nil
}
func (a *contextRequestTestActor) RPCWithContext(ctx context.Context, to ActorUID, params, reply any) error {
	return nil
}
func (a *contextRequestTestActor) AsyncRPCWithDeadline(to ActorUID, params any, cb ActorRPCFunc, deadline time.Time) error {
	a.asyncDeadlineCalled = true
	a.asyncDeadline = deadline
	a.lastAsyncCallback = cb
	return a.asyncDeadlineErr
}
func (a *contextRequestTestActor) AsyncRPCWithTimeout(to ActorUID, params any, cb ActorRPCFunc, timeout time.Duration) error {
	return nil
}
func (a *contextRequestTestActor) AsyncRPC(to ActorUID, params any, cb ActorRPCFunc) error {
	a.asyncCalled = true
	a.lastAsyncCallback = cb
	return a.asyncErr
}
func (a *contextRequestTestActor) AsyncRPCWithContext(ctx context.Context, to ActorUID, params any, cb ActorRPCFunc) error {
	return nil
}
func (a *contextRequestTestActor) Cast(to ActorUID, payload any) error {
	return nil
}

type contextRequestTestCBehavior struct {
	CActor
}

func (b *contextRequestTestCBehavior) OnStart() error  { return nil }
func (b *contextRequestTestCBehavior) OnStop() error   { return nil }
func (b *contextRequestTestCBehavior) OnConnected()    {}
func (b *contextRequestTestCBehavior) OnDisconnected() {}

func newContextRequestTestCActor(t *testing.T) (*Service, *cactor) {
	t.Helper()

	svc, _ := newRPCTestManager(t, 16)
	base := &actorDefineBase{
		name:           "context-request-cactor",
		category:       1,
		priority:       1,
		messageBoxSize: 8,
	}
	a := &cactor{
		actorCore: newActorCore(base, 1, "", svc),
	}
	a.behavior = &contextRequestTestCBehavior{CActor: a}
	return svc, a
}

func TestContextRPCUsesRequestDeadline(t *testing.T) {
	deadline := time.Now().Add(time.Minute)
	req := &contextRequestTestRequest{
		rt:            RequestTypeRPC,
		hasDeadline:   true,
		deadlineValue: deadline,
	}
	actor := &contextRequestTestActor{uid: ActorUID{Category: 1, ID: 1}}
	ctx := &Context{
		req:   req,
		actor: actor,
	}

	if err := ctx.RPC(ActorUID{Category: 1, ID: 2}, "req", nil); err != nil {
		t.Fatalf("context rpc: %v", err)
	}
	if !actor.rpcDeadlineCalled {
		t.Fatal("expected RPCWithDeadline called")
	}
	if !actor.rpcDeadline.Equal(deadline) {
		t.Fatalf("rpc deadline = %v, want %v", actor.rpcDeadline, deadline)
	}
	if actor.rpcCalled {
		t.Fatal("expected RPC not called when context has deadline")
	}
}

func TestContextRPCWithoutDeadlineFallsBackToRPC(t *testing.T) {
	req := &contextRequestTestRequest{
		rt:          RequestTypeRPC,
		hasDeadline: false,
	}
	actor := &contextRequestTestActor{uid: ActorUID{Category: 1, ID: 1}}
	ctx := &Context{
		req:   req,
		actor: actor,
	}

	if err := ctx.RPC(ActorUID{Category: 1, ID: 2}, "req", nil); err != nil {
		t.Fatalf("context rpc: %v", err)
	}
	if !actor.rpcCalled {
		t.Fatal("expected RPC called without inherited deadline")
	}
	if actor.rpcDeadlineCalled {
		t.Fatal("expected RPCWithDeadline not called without deadline")
	}
}

func TestContextAsyncRPCUsesRequestDeadline(t *testing.T) {
	deadline := time.Now().Add(time.Minute)
	req := &contextRequestTestRequest{
		rt:            RequestTypeRPC,
		hasDeadline:   true,
		deadlineValue: deadline,
	}
	actor := &contextRequestTestActor{uid: ActorUID{Category: 1, ID: 1}}
	ctx := &Context{
		req:   req,
		actor: actor,
	}

	if err := ctx.AsyncRPC(ActorUID{Category: 1, ID: 2}, "req", func(ctx *Context, resp *RPCResp) {}); err != nil {
		t.Fatalf("context async rpc: %v", err)
	}
	if !ctx.suspend {
		t.Fatal("expected context suspended after AsyncRPC")
	}
	if !actor.asyncDeadlineCalled {
		t.Fatal("expected AsyncRPCWithDeadline called")
	}
	if !actor.asyncDeadline.Equal(deadline) {
		t.Fatalf("async rpc deadline = %v, want %v", actor.asyncDeadline, deadline)
	}
	if actor.asyncCalled {
		t.Fatal("expected AsyncRPC not called when context has deadline")
	}
}

func TestContextCloneCopiesKVAndRequest(t *testing.T) {
	req := &contextRequestTestRequest{
		rt:            RequestTypeRPC,
		from:          ActorUID{Category: 1, ID: 9},
		hasDeadline:   true,
		deadlineValue: time.Now().Add(time.Minute),
	}
	actor := &contextRequestTestActor{uid: ActorUID{Category: 1, ID: 1}}
	ctx := &Context{
		svc:        &Service{},
		req:        req,
		actor:      actor,
		kv:         map[string]any{"name": "alice"},
		handlerIdx: 3,
	}
	clone := ctx.Clone()

	cloneReq, ok := clone.req.(*contextRequestTestRequest)
	if !ok {
		t.Fatalf("clone req type mismatch: %T", clone.req)
	}
	if cloneReq == req {
		t.Fatal("expected cloned request to be a new instance")
	}
	if cloneReq.cloneSource != req {
		t.Fatal("expected cloned request to record clone source")
	}
	if got, _ := clone.Get("name"); got != "alice" {
		t.Fatalf("clone kv value = %v, want alice", got)
	}

	ctx.Set("name", "bob")
	if got, _ := clone.Get("name"); got != "alice" {
		t.Fatalf("clone kv should be deep copied, got %v", got)
	}
	if clone.handlerIdx != ctx.handlerIdx {
		t.Fatalf("clone handlerIdx = %d, want %d", clone.handlerIdx, ctx.handlerIdx)
	}
}

func TestContextAsyncRPCErrorResetsSuspend(t *testing.T) {
	req := &contextRequestTestRequest{rt: RequestTypeRPC}
	actor := &contextRequestTestActor{
		uid:      ActorUID{Category: 1, ID: 1},
		asyncErr: errors.New("async rpc failed"),
	}
	ctx := &Context{req: req, actor: actor}

	err := ctx.AsyncRPC(ActorUID{Category: 1, ID: 2}, "req", func(ctx *Context, resp *RPCResp) {})
	if err == nil || err.Error() != "async rpc failed" {
		t.Fatalf("context async rpc err = %v, want async rpc failed", err)
	}
	if ctx.suspend {
		t.Fatal("expected context suspend reset after AsyncRPC error")
	}
	if !actor.asyncCalled {
		t.Fatal("expected AsyncRPC called")
	}
}

func TestContextReplyDecodeErrorForwardsToRequest(t *testing.T) {
	req := &contextRequestTestRequest{rt: RequestTypeReq}
	ctx := &Context{req: req}

	if err := ctx.ReplyDecodeError(); err != nil {
		t.Fatalf("reply decode error: %v", err)
	}
	if req.replyDecodeCount != 1 {
		t.Fatalf("replyDecodeCount = %d, want 1", req.replyDecodeCount)
	}
	if req.replyCount != 0 {
		t.Fatalf("replyCount = %d, want 0", req.replyCount)
	}
}

func TestRawRequestBeforeHandleGuardsSession(t *testing.T) {
	svc, cActor := newContextRequestTestCActor(t)

	t.Run("not cactor", func(t *testing.T) {
		req := newRawRequest("node-a", 1, 10, Buffer{}, time.Now().Add(time.Second).UnixMilli())
		ctx := &Context{
			svc: svc,
			req: req,
			actor: &contextRequestTestActor{
				uid: ActorUID{Category: 1, ID: 2},
			},
		}

		err := req.beforeHandle(ctx)
		if err == nil || err.Error() != "not CActor" {
			t.Fatalf("beforeHandle err = %v, want not CActor", err)
		}
	})

	t.Run("not connected", func(t *testing.T) {
		req := newRawRequest("node-a", 1, 10, Buffer{}, time.Now().Add(time.Second).UnixMilli())
		ctx := &Context{
			svc:   svc,
			req:   req,
			actor: cActor,
		}

		err := req.beforeHandle(ctx)
		if !errors.Is(err, errSkipHandleRequest) {
			t.Fatalf("beforeHandle err = %v, want %v", err, errSkipHandleRequest)
		}
	})

	t.Run("connected by another", func(t *testing.T) {
		req := newRawRequest("node-a", 1, 10, Buffer{}, time.Now().Add(time.Second).UnixMilli())
		cActor.session = ActorSession{NodeId: "node-b", SID: 11}
		ctx := &Context{
			svc:   svc,
			req:   req,
			actor: cActor,
		}

		err := req.beforeHandle(ctx)
		if !errors.Is(err, errSkipHandleRequest) {
			t.Fatalf("beforeHandle err = %v, want %v", err, errSkipHandleRequest)
		}
	})

	t.Run("matched session", func(t *testing.T) {
		req := newRawRequest("node-a", 1, 10, Buffer{}, time.Now().Add(time.Second).UnixMilli())
		cActor.session = ActorSession{NodeId: "node-a", SID: 10}
		ctx := &Context{
			svc:   svc,
			req:   req,
			actor: cActor,
		}

		if err := req.beforeHandle(ctx); err != nil {
			t.Fatalf("beforeHandle err = %v, want nil", err)
		}
	})
}
