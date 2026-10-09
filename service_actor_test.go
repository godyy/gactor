package gactor

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/godyy/gtimewheel"
)

type startActorSingleflightTestState struct {
	onStartCount atomic.Int32
	onStopCount  atomic.Int32
	onStartDelay time.Duration
	onStopDelay  time.Duration
}

type startActorSingleflightTestBehavior struct {
	Actor
	state *startActorSingleflightTestState
}

func (a *startActorSingleflightTestBehavior) OnStart() error {
	a.state.onStartCount.Add(1)
	if a.state.onStartDelay > 0 {
		time.Sleep(a.state.onStartDelay)
	}
	return nil
}

func (a *startActorSingleflightTestBehavior) OnStop(ActorStopContext) error {
	a.state.onStopCount.Add(1)
	if a.state.onStopDelay > 0 {
		time.Sleep(a.state.onStopDelay)
	}
	return nil
}

type startActorSingleflightTestRegistry struct {
	*testActorRegistry
	registerCount atomic.Int32
}

func (r *startActorSingleflightTestRegistry) RegisterActor(params ActorRegisterParams) (ActorRegisterResult, error) {
	r.registerCount.Add(1)
	return r.testActorRegistry.RegisterActor(params)
}

type startActorSingleflightTestServiceHandler struct {
	registry   ActorRegistry
	router     ActorRouter
	netAgent   NetAgent
	codec      PacketCodec
	timeSystem TimeSystem
}

func (sh *startActorSingleflightTestServiceHandler) GetActorRegistry() ActorRegistry {
	return sh.registry
}

func (sh *startActorSingleflightTestServiceHandler) GetActorRouter() ActorRouter {
	return sh.router
}

func (sh *startActorSingleflightTestServiceHandler) GetNetAgent() NetAgent {
	return sh.netAgent
}

func (sh *startActorSingleflightTestServiceHandler) GetPacketCodec() PacketCodec {
	return sh.codec
}

func (sh *startActorSingleflightTestServiceHandler) GetTimeSystem() TimeSystem {
	return sh.timeSystem
}

func (sh *startActorSingleflightTestServiceHandler) GetMonitor() ServiceMonitor {
	return nil
}

func newStartActorSingleflightTestService(t *testing.T, state *startActorSingleflightTestState, registry *startActorSingleflightTestRegistry) *Service {
	t.Helper()

	if logger == nil {
		if err := initLogger(); err != nil {
			t.Fatalf("init logger: %v", err)
		}
	}

	handler := &startActorSingleflightTestServiceHandler{
		registry: registry,
		router: &testActorRouter{
			nodes: []string{"start-actor-singleflight-test"},
		},
		netAgent:   &testNetAgent{},
		codec:      &testPacketCodec{},
		timeSystem: DefTimeSystem,
	}

	svc := NewService(&ServiceConfig{
		NodeId: "start-actor-singleflight-test",
		ActorConfig: ActorConfig{
			ActorDefines: []ActorDefine{
				NewActorDefine(ActorDefineConfig{
					Name:              "start-actor-singleflight-test",
					Category:          1,
					Priority:          1,
					PriMessageBoxSize: 128,
					MessageBoxSize:    128,
					BehaviorCreator: func(a Actor) ActorBehavior {
						return &startActorSingleflightTestBehavior{
							Actor: a,
							state: state,
						}
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
			MaxTimerAmount: 32,
		},
		RPCConfig: RPCConfig{
			DefRPCTimeout:    time.Second,
			MaxRPCCallAmount: 16,
		},
		MaxRTT:  50,
		Handler: handler,
	}, WithServiceLogger(logger.Named("start-actor-singleflight-test")))
	if err := svc.Start(); err != nil {
		t.Fatalf("start service: %v", err)
	}

	return svc
}

func TestServiceStartActorSingleflightSameUID(t *testing.T) {
	state := &startActorSingleflightTestState{
		onStartDelay: 50 * time.Millisecond,
	}
	registry := &startActorSingleflightTestRegistry{
		testActorRegistry: &testActorRegistry{
			actorMap: make(map[ActorUID]*testActorLocation),
		},
	}
	svc := newStartActorSingleflightTestService(t, state, registry)
	defer func() {
		if err := svc.Stop(); err != nil {
			t.Fatalf("stop service: %v", err)
		}
	}()

	uid := ActorUID{Category: 1, ID: 1001}
	const workerCount = 32

	startCh := make(chan struct{})
	errCh := make(chan error, workerCount)
	var wg sync.WaitGroup
	for range workerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startCh

			actor, err := svc.startActor(uid, "")
			if err == nil {
				err = actor.core().deref()
			}
			errCh <- err
		}()
	}

	close(startCh)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent start actor: %v", err)
		}
	}

	if got := state.onStartCount.Load(); got != 1 {
		t.Fatalf("OnStart count = %d, want 1", got)
	}
	if got := registry.registerCount.Load(); got != 1 {
		t.Fatalf("RegisterActor count = %d, want 1", got)
	}
}

func TestServiceStarterWhileStoppingCanRestart(t *testing.T) {
	state := &startActorSingleflightTestState{}
	registry := &startActorSingleflightTestRegistry{
		testActorRegistry: &testActorRegistry{
			actorMap: make(map[ActorUID]*testActorLocation),
		},
	}
	svc := newStartActorSingleflightTestService(t, state, registry)
	defer func() {
		if err := svc.Stop(); err != nil {
			t.Fatalf("stop service: %v", err)
		}
	}()

	uid := ActorUID{Category: 1, ID: 2001}
	actor, err := svc.startActor(uid, "")
	if err != nil {
		t.Fatalf("start actor: %v", err)
	}

	if err = actor.stop(false); err != nil {
		t.Fatalf("stop actor: %v", err)
	}

	categoryActors := svc.getCategoryActorsByCategory(uid.Category)
	starter := &actorStarter{
		uid:  uid,
		done: make(chan struct{}, 1),
	}
	if actual, loaded := categoryActors.addStarter(uid.ID, starter); loaded {
		t.Fatalf("starter already exists: %+v", actual.uid)
	}

	if err = actor.core().deref(); err != nil {
		t.Fatalf("release actor ref: %v", err)
	}

	select {
	case <-starter.done:
	case <-time.After(time.Second):
		t.Fatal("wait starter restart timeout")
	}

	if starter.err != nil {
		t.Fatalf("starter err: %v", starter.err)
	}
	if starter.actor == nil {
		t.Fatal("starter actor is nil")
	}

	// 释放 starter 内部持有的 Actor 引用，避免影响服务停机。
	if !starter.ref() {
		t.Fatal("starter ref failed after restart")
	}
	starter.deref()

	waitCountEventually(t, state.onStartCount.Load, 2, "restart OnStart")
}
