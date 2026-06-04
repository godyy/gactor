package gactor

import (
	"testing"
	"time"
)

type actorRPCTestState struct{}

type actorRPCTestBehavior struct {
	Actor
	state *actorRPCTestState
}

func (a *actorRPCTestBehavior) OnStart() error { return nil }
func (a *actorRPCTestBehavior) OnStop() error  { return nil }

type actorRPCTestStub struct {
	*actorCore
	behavior ActorBehavior
}

func newActorRPCTestStub(t *testing.T) (*Service, *actorRPCTestStub) {
	t.Helper()

	svc, _ := newRPCTestManager(t, 16)
	base := &actorDefineBase{
		name:              "actor-rpc-unit",
		category:          1,
		priority:          1,
		messageBoxSize:    8,
		maxTimerAmount:    4,
		maxAsyncRPCAmount: 4,
	}
	actor := &actorRPCTestStub{
		actorCore: newActorCore(base, 1, "", svc),
	}
	behavior := &actorRPCTestBehavior{
		Actor: actor,
		state: &actorRPCTestState{},
	}
	actor.behavior = behavior

	if err := actor.start(); err != nil {
		t.Fatalf("start actor stub: %v", err)
	}

	return svc, actor
}

func (a *actorRPCTestStub) core() *actorCore { return a.actorCore }

func (a *actorRPCTestStub) start() error {
	return a.actorCore.start()
}

func (a *actorRPCTestStub) stop(shutdown bool) error {
	return a.actorCore.stop(shutdown)
}

func (a *actorRPCTestStub) stopped() {}

func (a *actorRPCTestStub) Behavior() ActorBehavior {
	return a.behavior
}

func TestActorReceiveCompletedAsyncRPCEnqueuesAndInvokesCallback(t *testing.T) {
	svc, actor := newActorRPCTestStub(t)

	payload := Buffer{}
	payload.SetBuf([]byte("async-reply"))
	resp := &RPCResp{
		svc:     svc,
		payload: payload,
	}
	callbackCh := make(chan string, 1)

	if err := actor.receiveCompletedAsyncRPC(resp, func(a Actor, resp *RPCResp) {
		callbackCh <- string(resp.payload.UnreadData())
	}); err != nil {
		t.Fatalf("receive completed async rpc: %v", err)
	}

	if resp.payload.Data() != nil {
		t.Fatal("expected response payload ownership moved to actor queue")
	}

	select {
	case call := <-actor.completedAsyncRPC:
		actorInvokeAsyncRPCFunc(actor, &call)
	case <-time.After(time.Second):
		t.Fatal("wait async rpc queued timeout")
	}

	select {
	case got := <-callbackCh:
		if got != "async-reply" {
			t.Fatalf("callback payload = %q, want %q", got, "async-reply")
		}
	case <-time.After(time.Second):
		t.Fatal("wait async rpc callback timeout")
	}
}

func TestActorReceiveCompletedAsyncRPCAfterStoppedReturnsError(t *testing.T) {
	_, actor := newActorRPCTestStub(t)

	if err := actor.stop(true); err != nil {
		t.Fatalf("stop actor stub: %v", err)
	}
	actor.actorCore.stopped(nil)

	if err := actor.receiveCompletedAsyncRPC(&RPCResp{}, nil); err != errActorStopped {
		t.Fatalf("receive completed async rpc err = %v, want %v", err, errActorStopped)
	}
}
