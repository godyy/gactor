package gactor

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/godyy/gtimewheel"
)

type actorAsyncCallTestBehavior struct {
	Actor
}

func (a *actorAsyncCallTestBehavior) OnStart() error { return nil }
func (a *actorAsyncCallTestBehavior) OnStop() error  { return nil }

func newActorAsyncCallTestService(t *testing.T) *Service {
	t.Helper()

	if logger == nil {
		if err := initLogger(); err != nil {
			t.Fatalf("init logger: %v", err)
		}
	}

	handler := &testServiceHandler{
		testActorRegistry: &testActorRegistry{
			actorMap: make(map[ActorUID]*testActorLocation),
		},
		testActorRouter: &testActorRouter{
			nodes: []string{"actor-async-call-test"},
		},
		testNetAgent:    &testNetAgent{},
		testPacketCodec: &testPacketCodec{},
		TimeSystem:      DefTimeSystem,
	}

	svc := NewService(&ServiceConfig{
		NodeId: "actor-async-call-test",
		ActorConfig: ActorConfig{
			ActorDefines: []ActorDefine{
				NewActorDefine(ActorDefineConfig{
					Name:              "actor-async-call-test",
					Category:          1,
					Priority:          1,
					PriMessageBoxSize: 16,
					MessageBoxSize:    16,
					BehaviorCreator: func(a Actor) ActorBehavior {
						return &actorAsyncCallTestBehavior{Actor: a}
					},
				}),
			},
			Handler: func(ctx *Context) {
				_ = ctx.Reply(nil)
			},
		},
		TimerConfig: TimerConfig{
			TimeWheelLevels: []gtimewheel.LevelConfig{
				{Name: "10ms", Span: 10 * time.Millisecond, Slots: 10},
				{Name: "100ms", Span: 100 * time.Millisecond, Slots: 10},
			},
			MaxTimerAmount: 64,
		},
		RPCConfig: RPCConfig{
			DefRPCTimeout:    time.Second,
			MaxRPCCallAmount: 16,
		},
		MaxRTT:  50,
		Handler: handler,
	}, WithServiceLogger(logger.Named("actor-async-call-test")))

	if err := svc.Start(); err != nil {
		t.Fatalf("start service: %v", err)
	}

	return svc
}

func TestActorAsyncCallDeliversCallbackOnce(t *testing.T) {
	svc := newActorAsyncCallTestService(t)
	defer func() {
		if err := svc.Stop(); err != nil {
			t.Fatalf("stop service: %v", err)
		}
	}()

	uid := ActorUID{Category: 1, ID: 1001}
	actor, err := svc.startActor(uid, "")
	if err != nil {
		t.Fatalf("start actor: %v", err)
	}
	defer actor.core().deref()

	callbackCh := make(chan struct{}, 2)
	var mu sync.Mutex
	var callCount int
	var gotUID ActorUID
	var gotArgs any
	var gotErr error

	caller, err := actor.AsyncCall(func(a Actor, args any, err error) {
		mu.Lock()
		callCount++
		gotUID = a.ActorUID()
		gotArgs = args
		gotErr = err
		mu.Unlock()
		callbackCh <- struct{}{}
	}, time.Second)
	if err != nil {
		t.Fatalf("async call: %v", err)
	}

	if err := caller("payload", nil); err != nil {
		t.Fatalf("invoke caller: %v", err)
	}

	select {
	case <-callbackCh:
	case <-time.After(time.Second):
		t.Fatal("wait async callback timeout")
	}

	if err := caller("payload-again", errors.New("ignored")); err != nil {
		t.Fatalf("invoke caller again: %v", err)
	}
	assertNoSignal(t, callbackCh, 100*time.Millisecond, "duplicate async callback")

	mu.Lock()
	defer mu.Unlock()
	if callCount != 1 {
		t.Fatalf("callback count = %d, want 1", callCount)
	}
	if gotUID != uid {
		t.Fatalf("callback actor uid = %v, want %v", gotUID, uid)
	}
	if gotArgs != "payload" {
		t.Fatalf("callback args = %v, want payload", gotArgs)
	}
	if gotErr != nil {
		t.Fatalf("callback err = %v, want nil", gotErr)
	}
}

func TestActorAsyncCallTimeoutOnlyInvokesOnce(t *testing.T) {
	svc := newActorAsyncCallTestService(t)
	defer func() {
		if err := svc.Stop(); err != nil {
			t.Fatalf("stop service: %v", err)
		}
	}()

	uid := ActorUID{Category: 1, ID: 1002}
	actor, err := svc.startActor(uid, "")
	if err != nil {
		t.Fatalf("start actor: %v", err)
	}
	defer actor.core().deref()

	callbackCh := make(chan struct{}, 2)
	var mu sync.Mutex
	var callCount int
	var gotErr error

	caller, err := actor.AsyncCall(func(a Actor, args any, err error) {
		mu.Lock()
		callCount++
		gotErr = err
		mu.Unlock()
		callbackCh <- struct{}{}
	}, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("async call: %v", err)
	}

	select {
	case <-callbackCh:
	case <-time.After(time.Second):
		t.Fatal("wait async timeout callback timeout")
	}

	mu.Lock()
	if callCount != 1 {
		mu.Unlock()
		t.Fatalf("callback count = %d, want 1", callCount)
	}
	if !errors.Is(gotErr, ErrTimeout) {
		mu.Unlock()
		t.Fatalf("callback err = %v, want %v", gotErr, ErrTimeout)
	}
	mu.Unlock()

	if err := caller("late-payload", nil); err != nil {
		t.Fatalf("invoke caller after timeout: %v", err)
	}
	assertNoSignal(t, callbackCh, 100*time.Millisecond, "late async callback")
}
