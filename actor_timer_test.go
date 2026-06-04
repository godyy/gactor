package gactor

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/godyy/gtimewheel"
)

type actorTimerTestState struct {
	startCount int32
	stopCount  int32
}

type actorTimerTestBehavior struct {
	Actor
	state *actorTimerTestState
}

type fakeTimeSystem struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeTimeSystem(now time.Time) *fakeTimeSystem {
	return &fakeTimeSystem{now: now}
}

func (t *fakeTimeSystem) Now() time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.now
}

func (t *fakeTimeSystem) Until(dst time.Time) time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	return dst.Sub(t.now)
}

func (t *fakeTimeSystem) Advance(d time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.now = t.now.Add(d)
}

func (a *actorTimerTestBehavior) OnStart() error {
	atomic.AddInt32(&a.state.startCount, 1)
	return nil
}

func (a *actorTimerTestBehavior) OnStop() error {
	atomic.AddInt32(&a.state.stopCount, 1)
	return nil
}

func newActorTimerTestService(t *testing.T) (*Service, *actorTimerTestState) {
	return newActorTimerTestServiceWithTimeSystem(t, DefTimeSystem)
}

func newActorTimerTestServiceWithTimeSystem(t *testing.T, ts TimeSystem) (*Service, *actorTimerTestState) {
	t.Helper()

	if logger == nil {
		if err := initLogger(); err != nil {
			t.Fatalf("init logger: %v", err)
		}
	}

	state := &actorTimerTestState{}

	handler := &testServiceHandler{
		testActorRegistry: &testActorRegistry{
			actorMap: make(map[ActorUID]*testActorLocation),
		},
		testActorRouter: &testActorRouter{
			nodes: []string{"actor-timer-test"},
		},
		testNetAgent:    &testNetAgent{},
		testPacketCodec: &testPacketCodec{},
		TimeSystem:      ts,
	}

	svc := NewService(&ServiceConfig{
		NodeId: "actor-timer-test",
		ActorConfig: ActorConfig{
			ActorDefines: []ActorDefine{
				NewActorDefine(ActorDefineConfig{
					Name:           "actor-timer-test",
					Category:       1,
					Priority:       1,
					MessageBoxSize: 16,
					BehaviorCreator: func(a Actor) ActorBehavior {
						return &actorTimerTestBehavior{
							Actor: a,
							state: state,
						}
					},
				},
					WithMaxTimerAmount(8),
					WithMaxAsyncRPCAmount(8),
				),
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
	}, WithServiceLogger(logger.Named("actor-timer-test")))

	if err := svc.Start(); err != nil {
		t.Fatalf("start service: %v", err)
	}

	return svc, state
}

func waitEventually(t *testing.T, check func() bool, name string) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("wait %s timeout", name)
}

func assertNoSignal[T any](t *testing.T, ch <-chan T, d time.Duration, name string) {
	t.Helper()

	select {
	case <-ch:
		t.Fatalf("unexpected %s signal", name)
	case <-time.After(d):
	}
}

func waitCountEventually(t *testing.T, load func() int32, want int32, name string) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if got := load(); got == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("wait %s count timeout, got=%d want=%d", name, load(), want)
}

func TestServiceStartActorCreatesActor(t *testing.T) {
	svc, state := newActorTimerTestService(t)
	defer func() {
		if err := svc.Stop(); err != nil {
			t.Fatalf("stop service: %v", err)
		}
	}()

	uid := ActorUID{Category: 1, ID: 1001}
	if err := svc.StartActor(context.Background(), uid); err != nil {
		t.Fatalf("start actor: %v", err)
	}

	waitEventually(t, func() bool {
		actor, err := svc.getActor(uid)
		return err == nil && actor != nil
	}, "actor exists")
	waitCountEventually(t, func() int32 { return atomic.LoadInt32(&state.startCount) }, 1, "actor start")
}

func TestServiceStartActorConcurrentSameUIDStartsOnce(t *testing.T) {
	svc, state := newActorTimerTestService(t)
	defer func() {
		if err := svc.Stop(); err != nil {
			t.Fatalf("stop service: %v", err)
		}
	}()

	uid := ActorUID{Category: 1, ID: 1002}
	start := make(chan struct{})
	errCh := make(chan error, 16)
	var wg sync.WaitGroup

	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errCh <- svc.StartActor(context.Background(), uid)
		}()
	}

	close(start)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent StartActor err = %v", err)
		}
	}

	waitEventually(t, func() bool {
		actor, err := svc.getActor(uid)
		return err == nil && actor != nil
	}, "concurrent actor exists")
	waitCountEventually(t, func() int32 { return atomic.LoadInt32(&state.startCount) }, 1, "concurrent actor start")
}

func TestServiceStopCallsActorOnStop(t *testing.T) {
	svc, state := newActorTimerTestService(t)

	uid := ActorUID{Category: 1, ID: 1003}
	if err := svc.StartActor(context.Background(), uid); err != nil {
		t.Fatalf("start actor: %v", err)
	}
	waitCountEventually(t, func() int32 { return atomic.LoadInt32(&state.startCount) }, 1, "actor start")

	if err := svc.Stop(); err != nil {
		t.Fatalf("stop service: %v", err)
	}

	waitCountEventually(t, func() int32 { return atomic.LoadInt32(&state.stopCount) }, 1, "actor stop")
}

func TestServiceStartTimerAndStopTimer(t *testing.T) {
	fts := newFakeTimeSystem(time.Now())
	svc, _ := newActorTimerTestServiceWithTimeSystem(t, fts)
	defer func() {
		if err := svc.Stop(); err != nil {
			t.Fatalf("stop service: %v", err)
		}
	}()

	firedCh := make(chan struct{}, 1)
	tid := svc.StartTimer(100*time.Millisecond, false, nil, func(args TimerArgs) {
		select {
		case firedCh <- struct{}{}:
		default:
		}
	})
	if tid == TimerIdNone {
		t.Fatal("expected valid timer id")
	}
	fts.Advance(120 * time.Millisecond)
	svc.tickTimeWheel()
	select {
	case <-firedCh:
	case <-time.After(time.Second):
		t.Fatal("wait timer fired timeout")
	}

	canceledCh := make(chan struct{}, 1)
	tid = svc.StartTimer(100*time.Millisecond, false, nil, func(args TimerArgs) {
		select {
		case canceledCh <- struct{}{}:
		default:
		}
	})
	if tid == TimerIdNone {
		t.Fatal("expected valid timer id for canceled timer")
	}
	svc.StopTimer(tid)
	fts.Advance(120 * time.Millisecond)
	svc.tickTimeWheel()
	assertNoSignal(t, canceledCh, 100*time.Millisecond, "canceled timer")
}

func TestServiceStartTimerAfterStopReturnsNone(t *testing.T) {
	fts := newFakeTimeSystem(time.Now())
	svc, _ := newActorTimerTestServiceWithTimeSystem(t, fts)
	if err := svc.Stop(); err != nil {
		t.Fatalf("stop service: %v", err)
	}
	if tid := svc.StartTimer(100*time.Millisecond, false, nil, func(args TimerArgs) {}); tid != TimerIdNone {
		t.Fatalf("timer id = %d, want %d", tid, TimerIdNone)
	}
}