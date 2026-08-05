package gactor

import (
	"testing"
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
		priMessageBoxSize: 8,
		messageBoxSize:    8,
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
