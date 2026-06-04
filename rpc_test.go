package gactor

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/godyy/gactor/internal/utils"
	"github.com/godyy/gtimewheel"
)

type rpcTestCmd struct {
	mu           sync.Mutex
	execCount    int
	releaseCount int
	discardCount int
	execFn       func(*rpcManager)
}

func (c *rpcTestCmd) exec(rm *rpcManager) {
	c.mu.Lock()
	c.execCount++
	fn := c.execFn
	c.mu.Unlock()
	if fn != nil {
		fn(rm)
	}
}

func (c *rpcTestCmd) release() {
	c.mu.Lock()
	c.releaseCount++
	c.mu.Unlock()
}

func (c *rpcTestCmd) discard() {
	c.mu.Lock()
	c.discardCount++
	c.mu.Unlock()
}

func (c *rpcTestCmd) snapshot() (execCount, releaseCount, discardCount int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.execCount, c.releaseCount, c.discardCount
}

func newRPCTestManager(t *testing.T, queueSize int) (*Service, *rpcManager) {
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
			nodes: []string{"rpc-test"},
		},
		testNetAgent:    &testNetAgent{},
		testPacketCodec: &testPacketCodec{},
		TimeSystem:      DefTimeSystem,
	}

	timerLevels := []gtimewheel.LevelConfig{
		{Name: "10ms", Span: 10 * time.Millisecond, Slots: 10},
		{Name: "100ms", Span: 100 * time.Millisecond, Slots: 10},
	}
	wheel, err := gtimewheel.NewTimeWheel(timerLevels, func(f gtimewheel.TimerFunc, args gtimewheel.TimerArgs) {
		f(args)
	})
	if err != nil {
		t.Fatalf("new time wheel: %v", err)
	}

	svc := &Service{
		cfg: &ServiceConfig{
			NodeId: "rpc-test",
			RPCConfig: RPCConfig{
				DefRPCTimeout:    100 * time.Millisecond,
				MaxRPCCallAmount: queueSize,
			},
			TimerConfig: TimerConfig{
				TimeWheelLevels: timerLevels,
				MaxTimerAmount:  queueSize,
			},
			Handler: handler,
		},
		actorDefineSet: &actorDefineSet{
			defineMap:      make(map[uint16]ActorDefine),
			priority2Index: make(map[int]int),
		},
		logger:            logger.Named("rpc-test"),
		oriLogger:         logger,
		stopWait:          utils.NewStopWait(),
		state:             serviceStateStarted,
		timeWheel:         wheel,
		lastTickTimeWheel: handler.TimeSystem.Now(),
		triggeredTimers:   make(chan triggeredTimer, queueSize),
	}

	rm := newRPCManager(svc)
	svc.rpcManager = rm

	return svc, rm
}

func TestRPCManagerDrainCmdsExecutesBufferedCommands(t *testing.T) {
	_, rm := newRPCTestManager(t, 4)

	cmd1 := &rpcTestCmd{}
	cmd2 := &rpcTestCmd{}

	rm.chCmds <- cmd1
	rm.chCmds <- cmd2

	rm.drainCmds()

	exec1, release1, discard1 := cmd1.snapshot()
	exec2, release2, discard2 := cmd2.snapshot()

	if exec1 != 1 || release1 != 1 || discard1 != 0 {
		t.Fatalf("cmd1 unexpected state: exec=%d release=%d discard=%d", exec1, release1, discard1)
	}
	if exec2 != 1 || release2 != 1 || discard2 != 0 {
		t.Fatalf("cmd2 unexpected state: exec=%d release=%d discard=%d", exec2, release2, discard2)
	}
	if len(rm.chCmds) != 0 {
		t.Fatalf("expected chCmds drained, got len=%d", len(rm.chCmds))
	}
}

func TestRPCManagerEnqueueCmdDiscardOnBusyAndStopping(t *testing.T) {
	_, rm := newRPCTestManager(t, 1)

	rm.chCmds <- &rpcTestCmd{}
	busyCmd := &rpcTestCmd{}
	if err := rm.enqueueCmd(busyCmd, false); !errors.Is(err, ErrServiceBusy) {
		t.Fatalf("enqueue busy cmd error = %v, want %v", err, ErrServiceBusy)
	}
	execCount, releaseCount, discardCount := busyCmd.snapshot()
	if execCount != 0 || releaseCount != 0 || discardCount != 1 {
		t.Fatalf("busy cmd unexpected state: exec=%d release=%d discard=%d", execCount, releaseCount, discardCount)
	}

	rm = newRPCManager(rm.svc)
	rm.stopping = true

	stoppingCmd := &rpcTestCmd{}
	if err := rm.enqueueCmd(stoppingCmd, false); !errors.Is(err, errServiceStopped) {
		t.Fatalf("enqueue stopping cmd error = %v, want %v", err, errServiceStopped)
	}
	execCount, releaseCount, discardCount = stoppingCmd.snapshot()
	if execCount != 0 || releaseCount != 0 || discardCount != 1 {
		t.Fatalf("stopping cmd unexpected state: exec=%d release=%d discard=%d", execCount, releaseCount, discardCount)
	}
}

func TestRPCCmdAddDiscardReleasesCall(t *testing.T) {
	call := &rpcCall{
		reqId: 1,
		err:   errors.New("test"),
		cb:    func(*RPCResp) {},
	}
	buf := Buffer{}
	buf.SetBuf([]byte{1, 2, 3})
	call.respPayload = buf

	cmd := newRPCCmdAdd(call)
	cmd.discard()

	if call.cb != nil {
		t.Fatal("expected call callback cleared after discard")
	}
	if call.err != nil {
		t.Fatal("expected call error cleared after discard")
	}
	if data := call.respPayload.Data(); data != nil {
		t.Fatalf("expected call payload cleared after discard, got %v", data)
	}
	if cmd.call != nil {
		t.Fatal("expected rpcCmdAdd.call cleared after discard")
	}
}

func TestRPCManagerStopCompletesPendingCalls(t *testing.T) {
	svc, rm := newRPCTestManager(t, 8)
	rm.start()

	callbackErr := make(chan error, 1)
	markerDone := make(chan struct{})

	from := ActorUID{Category: 1, ID: 1}
	to := ActorUID{Category: 1, ID: 2}

	if _, err := rm.createCall(from, to, time.Now().Add(time.Second), func(resp *RPCResp) {
		callbackErr <- resp.Err()
	}); err != nil {
		t.Fatalf("create call: %v", err)
	}

	marker := &rpcTestCmd{execFn: func(*rpcManager) { close(markerDone) }}
	if err := rm.enqueueCmd(marker, false); err != nil {
		t.Fatalf("enqueue marker: %v", err)
	}

	select {
	case <-markerDone:
	case <-time.After(time.Second):
		t.Fatal("wait add-call marker timeout")
	}

	svc.stopWait.Stop(true)

	select {
	case err := <-callbackErr:
		if !errors.Is(err, errServiceStopped) {
			t.Fatalf("callback err = %v, want %v", err, errServiceStopped)
		}
	case <-time.After(time.Second):
		t.Fatal("wait callback timeout")
	}
}

func TestRPCManagerStopConcurrentEnqueueCmdNoResidual(t *testing.T) {
	svc, rm := newRPCTestManager(t, 128)
	rm.start()

	const cmdCount = 64
	cmds := make([]*rpcTestCmd, cmdCount)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range cmds {
		cmds[i] = &rpcTestCmd{}
		wg.Add(1)
		go func(cmd *rpcTestCmd) {
			defer wg.Done()
			<-start
			_ = rm.enqueueCmd(cmd, true)
		}(cmds[i])
	}

	stopped := make(chan struct{})
	go func() {
		svc.stopWait.Stop(true)
		close(stopped)
	}()
	close(start)
	wg.Wait()

	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("wait stop timeout")
	}

	for i, cmd := range cmds {
		execCount, releaseCount, discardCount := cmd.snapshot()
		if execCount != releaseCount {
			t.Fatalf("cmd[%d] exec/release mismatch: exec=%d release=%d discard=%d", i, execCount, releaseCount, discardCount)
		}
		if releaseCount+discardCount != 1 {
			t.Fatalf("cmd[%d] terminal count mismatch: exec=%d release=%d discard=%d", i, execCount, releaseCount, discardCount)
		}
	}
	if len(rm.chCmds) != 0 {
		t.Fatalf("expected chCmds empty after stop, got len=%d", len(rm.chCmds))
	}
}

func TestRPCManagerHandleResponseConcurrentWithStopCallbackOnce(t *testing.T) {
	svc, rm := newRPCTestManager(t, 32)
	rm.start()

	var callbackCount int32
	callbackErr := make(chan error, 8)
	markerDone := make(chan struct{})
	from := ActorUID{Category: 1, ID: 1}
	to := ActorUID{Category: 1, ID: 2}

	reqId, err := rm.createCall(from, to, time.Now().Add(time.Second), func(resp *RPCResp) {
		atomic.AddInt32(&callbackCount, 1)
		callbackErr <- resp.Err()
	})
	if err != nil {
		t.Fatalf("create call: %v", err)
	}
	if err := rm.enqueueCmd(&rpcTestCmd{execFn: func(*rpcManager) { close(markerDone) }}, false); err != nil {
		t.Fatalf("enqueue marker: %v", err)
	}
	select {
	case <-markerDone:
	case <-time.After(time.Second):
		t.Fatal("wait add-call marker timeout")
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			rm.handleResponse(reqId, to, from, nil, nil)
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		svc.stopWait.Stop(true)
	}()
	close(start)
	wg.Wait()

	select {
	case err := <-callbackErr:
		if err != nil && !errors.Is(err, errServiceStopped) {
			t.Fatalf("callback err = %v, want nil or %v", err, errServiceStopped)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("wait callback timeout")
	}
	if n := atomic.LoadInt32(&callbackCount); n != 1 {
		t.Fatalf("callback count = %d, want 1", n)
	}
}
