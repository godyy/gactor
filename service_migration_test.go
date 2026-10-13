package gactor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/godyy/gtimewheel"
)

func waitForCondition(t *testing.T, timeout time.Duration, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not satisfied before timeout")
}

func TestMigrationConfigDefaults(t *testing.T) {
	svc, _ := newPacketHandlerTestService(t, false, false)
	if got := svc.cfg.Migration.MigrationMaxConcurrentActors; got != 256 {
		t.Fatalf("MigrationMaxConcurrentActors = %d, want 256", got)
	}
	if got := svc.cfg.Migration.MigrationMaxConcurrentForwards; got != 256 {
		t.Fatalf("MigrationMaxConcurrentForwards = %d, want 256", got)
	}
}

type packetHandlerMigrationTestCActor struct {
	CActor
	inboundMigration chan ActorMigrationContext
}

type migrationMonitorTest struct {
	mu        sync.Mutex
	actions   map[MonitorMigrationAction]int
	converged []time.Duration
}

func (m *migrationMonitorTest) MonitorActorAmount(MonitorActorAction, ActorCategory, int64) {}
func (m *migrationMonitorTest) MonitorTimerAmount(MonitorTimerAction, int64)                {}
func (m *migrationMonitorTest) MonitorRPCAction(MonitorCAAction)                            {}
func (m *migrationMonitorTest) MonitorCastAction(MonitorCAAction)                           {}
func (m *migrationMonitorTest) MonitorMigrationAction(action MonitorMigrationAction) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.actions == nil {
		m.actions = make(map[MonitorMigrationAction]int)
	}
	m.actions[action]++
}
func (m *migrationMonitorTest) MonitorMigrationConverged(duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.converged = append(m.converged, duration)
}
func (m *migrationMonitorTest) actionCount(action MonitorMigrationAction) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.actions[action]
}

func (a *packetHandlerMigrationTestCActor) OnStart() error                { return nil }
func (a *packetHandlerMigrationTestCActor) OnStop(ActorStopContext) error { return nil }
func (a *packetHandlerMigrationTestCActor) OnConnected()                  {}
func (a *packetHandlerMigrationTestCActor) OnDisconnected()               {}
func (a *packetHandlerMigrationTestCActor) OnMigrateOut(ActorMigrationContext) error {
	return nil
}
func (a *packetHandlerMigrationTestCActor) OnMigrateIn(ctx ActorMigrationContext) error {
	a.inboundMigration <- ctx
	return nil
}

func newPacketHandlerMigrationCTestService(t *testing.T, started bool, enableAck bool) (*Service, *packetHandlerTestNetAgent) {
	t.Helper()

	if logger == nil {
		if err := initLogger(); err != nil {
			t.Fatalf("init logger: %v", err)
		}
	}

	netAgent := &packetHandlerTestNetAgent{}
	handler := &packetHandlerTestServiceHandler{
		registry: &testActorRegistry{
			actorMap: make(map[ActorUID]*testActorLocation),
		},
		router: &testActorRouter{
			nodes: []string{"svc-node"},
		},
		net:   netAgent,
		codec: &testPacketCodec{},
		ts:    DefTimeSystem,
	}

	cfg := &ServiceConfig{
		NodeId: "svc-node",
		ActorConfig: ActorConfig{
			ActorDefines: []ActorDefine{
				NewCActorDefine(CActorDefineConfig{
					Name:              "packet-handler-migration-cactor-test",
					Category:          2,
					Priority:          1,
					PriMessageBoxSize: 8,
					MessageBoxSize:    8,
					RecycleTime:       time.Minute,
					BehaviorCreator: func(a CActor) CActorBehavior {
						return &packetHandlerMigrationTestCActor{
							CActor:           a,
							inboundMigration: make(chan ActorMigrationContext, 1),
						}
					},
				}),
			},
			ClientActorCategory: 2,
			Handler: func(ctx *Context) {
				_ = ctx.Reply(nil)
			},
		},
		TimerConfig: TimerConfig{
			TimeWheelLevels: []gtimewheel.LevelConfig{
				{Name: "10ms", Span: 10 * time.Millisecond, Slots: 10},
				{Name: "100ms", Span: 100 * time.Millisecond, Slots: 10},
			},
		},
		RPCConfig: RPCConfig{
			DefRPCTimeout:    time.Second,
			MaxRPCCallAmount: 16,
		},
		MaxRTT:  50,
		Handler: handler,
	}

	options := []ServiceOption{
		WithServiceLogger(logger.Named("packet-handler-migration-cactor-test")),
	}
	if enableAck {
		options = append(options, WithServiceAckManager(&AckConfig{
			MaxPacketAmount: 16,
			Timeout:         time.Second,
			MaxRetry:        2,
			TickInterval:    10 * time.Millisecond,
		}))
	}

	svc := NewService(cfg, options...)
	if started {
		if err := svc.Start(); err != nil {
			t.Fatalf("start service: %v", err)
		}
	}

	return svc, netAgent
}

func migrationTestRegistry(t *testing.T, svc *Service) *testActorRegistry {
	t.Helper()
	handler, ok := svc.getCfg().Handler.(*packetHandlerTestServiceHandler)
	if !ok {
		t.Fatal("unexpected service handler type")
	}
	registry, ok := handler.registry.(*testActorRegistry)
	if !ok {
		t.Fatal("unexpected actor registry type")
	}
	return registry
}

func seedMigrationSourceLease(t *testing.T, registry *testActorRegistry, uid ActorUID, sourceNodeID, sourceLeaseID string) {
	t.Helper()
	if _, err := registry.RegisterActor(ActorRegisterParams{
		UID:     uid,
		NodeId:  sourceNodeID,
		LeaseId: sourceLeaseID,
		TTL:     60,
	}); err != nil {
		t.Fatalf("seed source actor lease: %v", err)
	}
}

func TestServiceStartMigrationSendsMigrateStartPacket(t *testing.T) {
	svc, netAgent := newPacketHandlerTestService(t, true, true)
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
	_ = actor.core().deref()

	if err := svc.StartMigration("target-node", nil); err != nil {
		t.Fatalf("StartMigration: %v", err)
	}

	waitForCondition(t, time.Second, func() bool {
		return len(netAgent.snapshot()) > 0 && actor.core().isMigratingOut()
	})

	sends := netAgent.snapshot()
	found := false
	for _, send := range sends {
		pt, buf := decodePacketType(t, send.data)
		if pt != PacketTypeS2SMigrateStart {
			continue
		}
		var head s2sMigrateStartPacketHead
		if err := head.decode(&buf); err != nil {
			t.Fatalf("decode migrate start: %v", err)
		}
		if send.nodeId != "target-node" {
			t.Fatalf("migrate start node = %s, want target-node", send.nodeId)
		}
		if head.uid != uid {
			t.Fatalf("migrate start uid = %+v, want %+v", head.uid, uid)
		}
		found = true
		break
	}
	if !found {
		t.Fatal("migrate start packet not sent")
	}
}

func TestOnMigrateOutReceivesMigrationContext(t *testing.T) {
	svc, _ := newPacketHandlerTestService(t, true, true)
	defer func() {
		if err := svc.Stop(); err != nil {
			t.Fatalf("stop service: %v", err)
		}
	}()

	uid := ActorUID{Category: 1, ID: 1005}
	actor, err := svc.startActor(uid, "")
	if err != nil {
		t.Fatalf("start actor: %v", err)
	}
	defer actor.core().deref()
	behavior, ok := actor.Behavior().(*packetHandlerTestActor)
	if !ok {
		t.Fatal("unexpected actor behavior")
	}
	if err := svc.StartMigration("target-node", nil); err != nil {
		t.Fatalf("StartMigration: %v", err)
	}

	select {
	case ctx := <-behavior.outboundMigration:
		if ctx.UID != uid || ctx.SourceNodeID != svc.nodeId() || ctx.SourceLeaseID == "" ||
			ctx.TargetNodeID != "target-node" || ctx.TargetLeaseID != "" || ctx.StartSeq == 0 {
			t.Fatalf("unexpected outbound migration context: %+v", ctx)
		}
	case <-time.After(time.Second):
		t.Fatal("OnMigrateOut did not receive migration context")
	}
}

func TestMigrationSlotWaitAndMonitorObservability(t *testing.T) {
	svc, _ := newPacketHandlerTestService(t, true, true)
	defer func() {
		if err := svc.Stop(); err != nil {
			t.Fatalf("stop service: %v", err)
		}
	}()

	handler := svc.getCfg().Handler.(*packetHandlerTestServiceHandler)
	monitor := &migrationMonitorTest{}
	handler.monitor = monitor
	svc.cfg.Migration.MigrationMaxConcurrentActors = 1

	uid1 := ActorUID{Category: 1, ID: 1006}
	uid2 := ActorUID{Category: 1, ID: 1007}
	actor1, err := svc.startActor(uid1, "")
	if err != nil {
		t.Fatalf("start actor1: %v", err)
	}
	_ = actor1.core().deref()
	actor2, err := svc.startActor(uid2, "")
	if err != nil {
		t.Fatalf("start actor2: %v", err)
	}
	_ = actor2.core().deref()

	if err := svc.StartMigration("target-node", nil); err != nil {
		t.Fatalf("StartMigration: %v", err)
	}
	waitForCondition(t, time.Second, func() bool {
		return svc.MigrationSnapshot().WaitingSlotActorCount == 1
	})
	snapshot := svc.MigrationSnapshot()
	if snapshot.RetryCount != 0 {
		t.Fatalf("slot wait was counted as retry: %d", snapshot.RetryCount)
	}
	if monitor.actionCount(MonitorMigrationStart) != 1 || monitor.actionCount(MonitorMigrationWaitForSlot) == 0 {
		t.Fatalf("migration monitor events missing: start=%d wait=%d",
			monitor.actionCount(MonitorMigrationStart), monitor.actionCount(MonitorMigrationWaitForSlot))
	}
}

func TestMigrationForwardBackpressure(t *testing.T) {
	svc, _ := newPacketHandlerTestService(t, true, true)
	defer func() {
		if err := svc.Stop(); err != nil {
			t.Fatalf("stop service: %v", err)
		}
	}()

	svc.cfg.Migration.MigrationMaxConcurrentForwards = 1
	if err := svc.StartMigration("target-node", nil); err != nil {
		t.Fatalf("StartMigration: %v", err)
	}
	if !svc.tryAcquireMigrationForwardSlot() {
		t.Fatal("first forward slot was not acquired")
	}
	if svc.tryAcquireMigrationForwardSlot() {
		t.Fatal("second forward slot bypassed configured limit")
	}
	svc.recordMigrationForwardBusy()
	snapshot := svc.MigrationSnapshot()
	if snapshot.ForwardBusyCount != 1 || snapshot.ForwardInFlightCount != 1 {
		t.Fatalf("unexpected forward backpressure snapshot: %+v", snapshot)
	}
	svc.releaseMigrationForwardSlot()
}

func TestHandlePacketS2SMigrateResultMarksActorMigrated(t *testing.T) {
	svc, netAgent := newPacketHandlerTestService(t, true, true)
	defer func() {
		if err := svc.Stop(); err != nil {
			t.Fatalf("stop service: %v", err)
		}
	}()

	uid := ActorUID{Category: 1, ID: 1002}
	actor, startErr := svc.startActor(uid, "")
	if startErr != nil {
		t.Fatalf("start actor: %v", startErr)
	}
	_ = actor.core().deref()

	if err := svc.StartMigration("target-node", nil); err != nil {
		t.Fatalf("StartMigration: %v", err)
	}

	waitForCondition(t, time.Second, func() bool {
		return actor.core().isMigratingOut()
	})

	startSeq := actor.core().migrationRuntime().startSeq
	resultHead := newS2SMigrateResultHead(svc.genSeq(), uid, actor.core().leaseID, startSeq, ErrCodeOK)
	packet, err := svc.encodePacket(&resultHead, nil)
	if err != nil {
		t.Fatalf("encode migrate result: %v", err)
	}
	if err := svc.HandlePacket("target-node", packet); err != nil {
		t.Fatalf("handle migrate result: %v", err)
	}

	waitForCondition(t, time.Second, func() bool {
		return actor.core().isMigrated()
	})

	if actor.core().keepAliveTimerId != TimerIdNone {
		t.Fatal("keepAliveTimerId should be cleared after migrated")
	}

	_ = netAgent
}

func TestMigrationResultTimeoutTriggersRetry(t *testing.T) {
	svc, netAgent := newPacketHandlerTestService(t, true, true)
	defer func() {
		if err := svc.Stop(); err != nil {
			t.Fatalf("stop service: %v", err)
		}
	}()

	uid := ActorUID{Category: 1, ID: 1004}
	actor, startErr := svc.startActor(uid, "")
	if startErr != nil {
		t.Fatalf("start actor: %v", startErr)
	}
	_ = actor.core().deref()

	if err := svc.StartMigration("target-node", nil); err != nil {
		t.Fatalf("StartMigration: %v", err)
	}

	waitForCondition(t, time.Second, func() bool {
		return actor.core().isMigratingOut()
	})

	initialStartSeq := actor.core().migrationRuntime().startSeq
	actor.core().receiveTriggerdTimer(TimerIdNone, migrationResultTimerArgs{StartSeq: initialStartSeq}, actorOnMigrationResultTimeout)

	waitForCondition(t, 3*time.Second, func() bool {
		migration := actor.core().migrationRuntime()
		return migration != nil && actor.core().isMigratingOut() && migration.startSeq != 0 && migration.startSeq != initialStartSeq
	})

	migrateStartCount := 0
	for _, send := range netAgent.snapshot() {
		pt, _ := decodePacketType(t, send.data)
		if pt == PacketTypeS2SMigrateStart && send.nodeId == "target-node" {
			migrateStartCount++
		}
	}
	if migrateStartCount < 2 {
		t.Fatalf("migrate start packet count = %d, want >= 2", migrateStartCount)
	}
}

func TestServiceMigrationSnapshot(t *testing.T) {
	svc, _ := newPacketHandlerTestService(t, true, true)
	defer func() {
		if err := svc.Stop(); err != nil {
			t.Fatalf("stop service: %v", err)
		}
	}()

	uid := ActorUID{Category: 1, ID: 1005}
	actor, startErr := svc.startActor(uid, "")
	if startErr != nil {
		t.Fatalf("start actor: %v", startErr)
	}
	_ = actor.core().deref()

	if err := svc.StartMigration("target-node", nil); err != nil {
		t.Fatalf("StartMigration: %v", err)
	}

	waitForCondition(t, time.Second, func() bool {
		snapshot := svc.MigrationSnapshot()
		return snapshot.Started && snapshot.TargetNodeID == "target-node" && snapshot.MigratingOutCount > 0
	})

	snapshot := svc.MigrationSnapshot()
	if !snapshot.Started {
		t.Fatal("snapshot.Started = false, want true")
	}
	if snapshot.TargetNodeID != "target-node" {
		t.Fatalf("snapshot.TargetNodeID = %s, want target-node", snapshot.TargetNodeID)
	}
	if snapshot.TrackedActorCount == 0 {
		t.Fatal("snapshot.TrackedActorCount = 0, want > 0")
	}
	if snapshot.MigratingOutCount == 0 {
		t.Fatal("snapshot.MigratingOutCount = 0, want > 0")
	}

	found := false
	for _, actorSnapshot := range snapshot.Actors {
		if actorSnapshot.UID != uid {
			continue
		}
		found = true
		if actorSnapshot.Phase != ActorMigrationPhaseMigratingOut {
			t.Fatalf("actor phase = %s, want %s", actorSnapshot.Phase, ActorMigrationPhaseMigratingOut)
		}
		if actorSnapshot.SourceLeaseID != actor.core().leaseID {
			t.Fatalf("actor source lease = %s, want %s", actorSnapshot.SourceLeaseID, actor.core().leaseID)
		}
		break
	}
	if !found {
		t.Fatal("actor snapshot not found")
	}
}

func TestMigrationActorPhaseDoesNotRollbackFromMigrated(t *testing.T) {
	svc, _ := newPacketHandlerTestService(t, true, true)
	defer func() {
		if err := svc.Stop(); err != nil {
			t.Fatalf("stop service: %v", err)
		}
	}()

	uid := ActorUID{Category: 1, ID: 1007}
	svc.mtxMigration.Lock()
	svc.migration = &serviceMigrationRuntime{
		migrationStarted:      true,
		migrationTargetNodeID: "target-node",
	}
	svc.mtxMigration.Unlock()
	svc.initMigrationPlan([]ActorUID{uid})

	svc.markMigrationActorDraining(uid)
	svc.updateMigrationStart(uid, "source-lease", 1)
	svc.markMigrationActorMigrated(uid)
	svc.markMigrationActorRetrying(uid, ErrTimeout) // 模拟迟到的失败事件。

	snapshot := svc.MigrationSnapshot()
	if !snapshot.Converged {
		t.Fatal("snapshot.Converged = false, want true")
	}
	if snapshot.MigratedActorCount != 1 {
		t.Fatalf("snapshot.MigratedActorCount = %d, want 1", snapshot.MigratedActorCount)
	}
	if snapshot.RetryingActorCount != 0 {
		t.Fatalf("snapshot.RetryingActorCount = %d, want 0", snapshot.RetryingActorCount)
	}
}

func TestMigrationSnapshotActorsAreSorted(t *testing.T) {
	svc, _ := newPacketHandlerTestService(t, true, true)
	defer func() {
		if err := svc.Stop(); err != nil {
			t.Fatalf("stop service: %v", err)
		}
	}()

	svc.mtxMigration.Lock()
	svc.migration = &serviceMigrationRuntime{
		migrationStarted:      true,
		migrationTargetNodeID: "target-node",
	}
	svc.mtxMigration.Unlock()
	svc.initMigrationPlan([]ActorUID{
		{Category: 2, ID: 1},
		{Category: 1, ID: 3},
		{Category: 1, ID: 2},
	})

	actors := svc.MigrationSnapshot().Actors
	want := []ActorUID{
		{Category: 1, ID: 2},
		{Category: 1, ID: 3},
		{Category: 2, ID: 1},
	}
	if len(actors) != len(want) {
		t.Fatalf("actor snapshot count = %d, want %d", len(actors), len(want))
	}
	for i, uid := range want {
		if actors[i].UID != uid {
			t.Fatalf("actor snapshot[%d] uid = %+v, want %+v", i, actors[i].UID, uid)
		}
	}
}

func TestWaitMigrationConverged(t *testing.T) {
	svc, _ := newPacketHandlerTestService(t, true, true)
	defer func() {
		if err := svc.Stop(); err != nil {
			t.Fatalf("stop service: %v", err)
		}
	}()

	if err := svc.WaitMigrationConverged(context.Background()); err != ErrCodeMigrationNotStarted {
		t.Fatalf("wait before migration error = %v, want %v", err, ErrCodeMigrationNotStarted)
	}

	uid := ActorUID{Category: 1, ID: 1008}
	svc.mtxMigration.Lock()
	svc.migration = &serviceMigrationRuntime{
		migrationStarted:      true,
		migrationTargetNodeID: "target-node",
	}
	svc.mtxMigration.Unlock()
	svc.initMigrationPlan([]ActorUID{uid})

	done := make(chan error, 1)
	go func() {
		done <- svc.WaitMigrationConverged(context.Background())
	}()

	svc.markMigrationActorDraining(uid)
	svc.updateMigrationStart(uid, "source-lease", 1)
	svc.markMigrationActorMigrated(uid)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("WaitMigrationConverged: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitMigrationConverged did not return after convergence")
	}
}

func TestWaitMigrationConvergedReturnsWhenServiceStops(t *testing.T) {
	svc, _ := newPacketHandlerTestService(t, true, true)

	uid := ActorUID{Category: 1, ID: 1010}
	svc.mtxMigration.Lock()
	svc.migration = &serviceMigrationRuntime{
		migrationStarted:      true,
		migrationTargetNodeID: "target-node",
		migrationConvergedCh:  make(chan struct{}),
		migrationStoppedCh:    make(chan struct{}),
	}
	svc.mtxMigration.Unlock()
	svc.initMigrationPlan([]ActorUID{uid})

	done := make(chan error, 1)
	go func() {
		done <- svc.WaitMigrationConverged(context.Background())
	}()

	if err := svc.Stop(); err != nil {
		t.Fatalf("stop service: %v", err)
	}
	select {
	case err := <-done:
		if err != ErrCodeServiceStopped {
			t.Fatalf("WaitMigrationConverged after stop = %v, want %v", err, ErrCodeServiceStopped)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitMigrationConverged did not return after service stopped")
	}
}

func TestMigrationRejectsNewLocalActor(t *testing.T) {
	svc, _ := newPacketHandlerTestService(t, true, true)
	defer func() {
		if err := svc.Stop(); err != nil {
			t.Fatalf("stop service: %v", err)
		}
	}()

	if err := svc.StartMigration("target-node", nil); err != nil {
		t.Fatalf("StartMigration: %v", err)
	}
	err := svc.StartActor(context.Background(), ActorUID{Category: 1, ID: 1011})
	if err != ErrCodeServiceMigrating {
		t.Fatalf("StartActor during migration error = %v, want %v", err, ErrCodeServiceMigrating)
	}
}

func TestMigrationStartRetryRecordsDiagnostics(t *testing.T) {
	svc, _ := newPacketHandlerTestService(t, true, true)
	defer func() {
		if err := svc.Stop(); err != nil {
			t.Fatalf("stop service: %v", err)
		}
	}()

	uid := ActorUID{Category: 1, ID: 1012}
	svc.mtxMigration.Lock()
	svc.migration = &serviceMigrationRuntime{
		migrationStarted:      true,
		migrationTargetNodeID: "target-node",
		migrationConvergedCh:  make(chan struct{}),
		migrationStoppedCh:    make(chan struct{}),
	}
	svc.mtxMigration.Unlock()
	svc.initMigrationPlan([]ActorUID{uid})

	svc.scheduleMigrationStartRetry(uid, ErrTimeout)
	svc.scheduleMigrationStartRetry(uid, ErrCodeActorBusy)

	snapshot := svc.MigrationSnapshot()
	if len(snapshot.Actors) != 1 {
		t.Fatalf("actor snapshot count = %d, want 1", len(snapshot.Actors))
	}
	actor := snapshot.Actors[0]
	if actor.RetryAttempts != 1 {
		t.Fatalf("retry attempts = %d, want 1", actor.RetryAttempts)
	}
	if actor.LastRetryAt.IsZero() {
		t.Fatal("last retry time is zero")
	}
	if actor.LastRetryErr != ErrTimeout.Error() {
		t.Fatalf("last retry error = %q, want %q", actor.LastRetryErr, ErrTimeout.Error())
	}
	if actor.NextRetryAt.IsZero() {
		t.Fatal("next retry time is zero")
	}
	if snapshot.RetryCount != 1 || snapshot.MaxRetryAttempts != 1 {
		t.Fatalf("retry metrics = (%d, %d), want (1, 1)", snapshot.RetryCount, snapshot.MaxRetryAttempts)
	}
}

func TestMigrationConvergedCallbackPanicIsIsolated(t *testing.T) {
	svc, _ := newPacketHandlerTestService(t, true, true)
	defer func() {
		if err := svc.Stop(); err != nil {
			t.Fatalf("stop service: %v", err)
		}
	}()

	callbackStarted := make(chan struct{}, 1)
	uid := ActorUID{Category: 1, ID: 1009}
	svc.mtxMigration.Lock()
	svc.migration = &serviceMigrationRuntime{
		migrationStarted:      true,
		migrationTargetNodeID: "target-node",
		migrationConvergedCh:  make(chan struct{}),
		migrationConvergedCb: func(ServiceMigrationSnapshot) {
			callbackStarted <- struct{}{}
			panic("test migration callback panic")
		},
	}
	svc.mtxMigration.Unlock()
	svc.initMigrationPlan([]ActorUID{uid})

	svc.markMigrationActorDraining(uid)
	svc.updateMigrationStart(uid, "source-lease", 1)
	svc.markMigrationActorMigrated(uid)

	select {
	case <-callbackStarted:
	case <-time.After(time.Second):
		t.Fatal("migration converged callback was not called")
	}

	if err := svc.WaitMigrationConverged(context.Background()); err != nil {
		t.Fatalf("WaitMigrationConverged after callback panic: %v", err)
	}
}

func TestMigrationConvergedCallbackFiresOnceAfterRetryRecovered(t *testing.T) {
	svc, _ := newPacketHandlerTestService(t, true, true)
	defer func() {
		if err := svc.Stop(); err != nil {
			t.Fatalf("stop service: %v", err)
		}
	}()

	callbackCh := make(chan ServiceMigrationSnapshot, 2)
	uid := ActorUID{Category: 1, ID: 1006}
	actor, startErr := svc.startActor(uid, "")
	if startErr != nil {
		t.Fatalf("start actor: %v", startErr)
	}
	_ = actor.core().deref()

	if err := svc.StartMigration("target-node", func(snapshot ServiceMigrationSnapshot) {
		callbackCh <- snapshot
	}); err != nil {
		t.Fatalf("StartMigration: %v", err)
	}

	waitForCondition(t, time.Second, func() bool {
		return actor.core().isMigratingOut()
	})

	initialStartSeq := actor.core().migrationRuntime().startSeq
	actor.core().receiveTriggerdTimer(TimerIdNone, migrationResultTimerArgs{StartSeq: initialStartSeq}, actorOnMigrationResultTimeout)

	waitForCondition(t, 3*time.Second, func() bool {
		snapshot := svc.MigrationSnapshot()
		return snapshot.RetryingActorCount == 1 && snapshot.Converged == false
	})

	select {
	case snapshot := <-callbackCh:
		t.Fatalf("unexpected converged callback before success: %+v", snapshot)
	case <-time.After(200 * time.Millisecond):
	}

	waitForCondition(t, 3*time.Second, func() bool {
		migration := actor.core().migrationRuntime()
		return migration != nil && actor.core().isMigratingOut() && migration.startSeq != 0 && migration.startSeq != initialStartSeq
	})

	startSeq := actor.core().migrationRuntime().startSeq
	resultHead := newS2SMigrateResultHead(svc.genSeq(), uid, actor.core().leaseID, startSeq, ErrCodeOK)
	packet, err := svc.encodePacket(&resultHead, nil)
	if err != nil {
		t.Fatalf("encode migrate result: %v", err)
	}
	if err := svc.HandlePacket("target-node", packet); err != nil {
		t.Fatalf("handle migrate result: %v", err)
	}

	var snapshot ServiceMigrationSnapshot
	select {
	case snapshot = <-callbackCh:
	case <-time.After(2 * time.Second):
		t.Fatal("migration converged callback not fired")
	}

	if !snapshot.Started || !snapshot.Converged {
		t.Fatalf("callback snapshot = %+v, want started and converged", snapshot)
	}
	if snapshot.TargetNodeID != "target-node" {
		t.Fatalf("callback target node = %s, want target-node", snapshot.TargetNodeID)
	}
	if snapshot.MigratedActorCount != 1 {
		t.Fatalf("callback migrated actor count = %d, want 1", snapshot.MigratedActorCount)
	}

	select {
	case extra := <-callbackCh:
		t.Fatalf("unexpected duplicate converged callback: %+v", extra)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestMigrateResultFromUnexpectedNodeIsIgnored(t *testing.T) {
	svc, _ := newPacketHandlerTestService(t, true, true)
	defer func() {
		if err := svc.Stop(); err != nil {
			t.Fatalf("stop service: %v", err)
		}
	}()

	uid := ActorUID{Category: 1, ID: 1013}
	actor, err := svc.startActor(uid, "")
	if err != nil {
		t.Fatalf("start actor: %v", err)
	}
	defer actor.core().deref()

	if err := svc.StartMigration("target-node", nil); err != nil {
		t.Fatalf("StartMigration: %v", err)
	}
	waitForCondition(t, time.Second, func() bool {
		return actor.core().isMigratingOut()
	})

	migration := actor.core().migrationRuntime()
	head := newS2SMigrateResultHead(svc.genSeq(), uid, actor.core().leaseID, migration.startSeq, ErrCodeOK)
	packet, encodeErr := svc.encodePacket(&head, nil)
	if encodeErr != nil {
		t.Fatalf("encode migrate result: %v", encodeErr)
	}
	if handleErr := svc.HandlePacket("unexpected-node", packet); handleErr != nil {
		t.Fatalf("handle migrate result: %v", handleErr)
	}

	if !actor.core().isMigratingOut() || actor.core().isMigrated() {
		t.Fatal("result from unexpected node changed actor migration state")
	}
}

func TestMigrationSlotsLimitConcurrentActors(t *testing.T) {
	svc, _ := newPacketHandlerTestService(t, true, true)
	defer func() {
		if err := svc.Stop(); err != nil {
			t.Fatalf("stop service: %v", err)
		}
	}()

	svc.cfg.Migration.MigrationMaxConcurrentActors = 1
	uid1 := ActorUID{Category: 1, ID: 1014}
	uid2 := ActorUID{Category: 1, ID: 1015}
	if err := svc.StartMigration("target-node", nil); err != nil {
		t.Fatalf("StartMigration: %v", err)
	}

	if !svc.tryAcquireMigrationSlot(uid1) {
		t.Fatal("first migration slot was not acquired")
	}
	if svc.tryAcquireMigrationSlot(uid2) {
		t.Fatal("second migration slot bypassed configured limit")
	}
	svc.releaseMigrationSlot(uid1)
	if !svc.tryAcquireMigrationSlot(uid2) {
		t.Fatal("released migration slot was not reused")
	}
}

func TestFinishMigrationForwarding(t *testing.T) {
	svc, _ := newPacketHandlerTestService(t, true, true)
	defer func() {
		if err := svc.Stop(); err != nil {
			t.Fatalf("stop service: %v", err)
		}
	}()

	if err := svc.StartMigration("target-node", nil); err != nil {
		t.Fatalf("StartMigration: %v", err)
	}
	if err := svc.FinishMigrationForwarding(); err != nil {
		t.Fatalf("FinishMigrationForwarding: %v", err)
	}
	if svc.isMigrationForwarding() {
		t.Fatal("migration forwarding remains enabled after finish")
	}
	if err := svc.FinishMigrationForwarding(); err != nil {
		t.Fatalf("repeated FinishMigrationForwarding: %v", err)
	}
	if err := svc.FinalizeMigration(context.Background()); err != nil {
		t.Fatalf("FinalizeMigration: %v", err)
	}
	if svc.MigrationSnapshot().Started {
		t.Fatal("migration state remains after finalization")
	}
	if err := svc.StartMigration("next-target-node", nil); err != nil {
		t.Fatalf("start next migration after finalization: %v", err)
	}
}

func TestFinalizeMigrationStopsMigratedActors(t *testing.T) {
	svc, _ := newPacketHandlerTestService(t, true, true)
	defer func() {
		if err := svc.Stop(); err != nil {
			t.Fatalf("stop service: %v", err)
		}
	}()

	uid := ActorUID{Category: 1, ID: 1013}
	actor, err := svc.startActor(uid, "")
	if err != nil {
		t.Fatalf("start actor: %v", err)
	}
	behavior := actor.Behavior().(*packetHandlerTestActor)
	// startActor 返回的引用属于调用方；释放后 FinalizeMigration 才能完成 Actor 停机。
	if releaseErr := actor.core().deref(); releaseErr != nil {
		t.Fatalf("release actor reference: %v", releaseErr)
	}
	if migrateErr := svc.StartMigration("target-node", nil); migrateErr != nil {
		t.Fatalf("StartMigration: %v", migrateErr)
	}
	waitForCondition(t, time.Second, func() bool {
		return actor.core().isMigratingOut()
	})
	startSeq := actor.core().migrationRuntime().startSeq
	result := newS2SMigrateResultHead(svc.genSeq(), uid, actor.core().leaseID, startSeq, ErrCodeOK)
	packet, err := svc.encodePacket(&result, nil)
	if err != nil {
		t.Fatalf("encode migration result: %v", err)
	}
	if handleErr := svc.HandlePacket("target-node", packet); handleErr != nil {
		t.Fatalf("handle migration result: %v", handleErr)
	}
	waitForCondition(t, time.Second, func() bool {
		return actor.core().isMigrated()
	})

	if err := svc.FinishMigrationForwarding(); err != nil {
		t.Fatalf("FinishMigrationForwarding: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := svc.FinalizeMigration(ctx); err != nil {
		t.Fatalf("FinalizeMigration: %v", err)
	}
	if stoppedActor, getErr := svc.getActor(uid); getErr != nil || stoppedActor != nil {
		t.Fatalf("source actor remains after finalization: actor=%v, err=%v", stoppedActor, getErr)
	}
	if svc.MigrationSnapshot().Started {
		t.Fatal("migration state remains after finalizing migrated actor")
	}
	select {
	case stopCtx := <-behavior.stopContext:
		if stopCtx.Reason != ActorStopReasonMigrated {
			t.Fatalf("stop reason = %v, want migrated", stopCtx.Reason)
		}
	case <-time.After(time.Second):
		t.Fatal("OnStop did not receive migrated stop reason")
	}
}

func TestMigratedCActorStopDoesNotDisconnectSession(t *testing.T) {
	svc, netAgent := newPacketHandlerMigrationCTestService(t, true, false)
	defer func() {
		if err := svc.Stop(); err != nil {
			t.Fatalf("stop service: %v", err)
		}
	}()

	uid := ActorUID{Category: 2, ID: 1017}
	started, err := svc.startActor(uid, "")
	if err != nil {
		t.Fatalf("start actor: %v", err)
	}
	actor, ok := started.(*cactor)
	if !ok {
		t.Fatalf("started actor type = %T, want *cactor", started)
	}
	actor.session = ActorSession{NodeId: "agent-node", SID: 1}
	actor.core().updateFlag(actorFlagMigrated, true)
	if err := actor.core().deref(); err != nil {
		t.Fatalf("release actor reference: %v", err)
	}
	if err := actor.core().stop(false); err != nil {
		t.Fatalf("stop migrated actor: %v", err)
	}

	waitForCondition(t, time.Second, func() bool {
		stopped, getErr := svc.getActor(uid)
		return getErr == nil && stopped == nil
	})
	for _, send := range netAgent.snapshot() {
		packetType, _ := decodePacketType(t, send.data)
		if packetType == PacketTypeDisconnect {
			t.Fatal("migrated CActor emitted a disconnect packet")
		}
	}
}

func TestMigrateStartWithMismatchedSourceNodeIsIgnored(t *testing.T) {
	svc, _ := newPacketHandlerTestService(t, true, true)
	defer func() {
		if err := svc.Stop(); err != nil {
			t.Fatalf("stop service: %v", err)
		}
	}()

	uid := ActorUID{Category: 1, ID: 1016}
	head := newS2SMigrateStartHead(1, uid, "declared-source", "source-lease", false, ActorSession{})
	packet, err := svc.encodePacket(&head, nil)
	if err != nil {
		t.Fatalf("encode migrate start: %v", err)
	}
	if err := svc.HandlePacket("actual-source", packet); err != nil {
		t.Fatalf("handle migrate start: %v", err)
	}

	if actor, err := svc.getActor(uid); err != nil || actor != nil {
		t.Fatalf("mismatched-source migrate start created actor = %v, err = %v", actor, err)
	}
	if svc.MigrationSnapshot().ControlDeniedCount != 1 {
		t.Fatalf("control denied count = %d, want 1", svc.MigrationSnapshot().ControlDeniedCount)
	}
}

func TestStartActorConcurrentWithMigrationIsPlannedOrRejected(t *testing.T) {
	svc, _ := newPacketHandlerTestService(t, true, true)
	defer func() {
		if err := svc.Stop(); err != nil {
			t.Fatalf("stop service: %v", err)
		}
	}()

	uid := ActorUID{Category: 1, ID: 1017}
	start := make(chan struct{})
	startActorDone := make(chan error, 1)
	startMigrationDone := make(chan error, 1)
	go func() {
		<-start
		startActorDone <- svc.StartActor(context.Background(), uid)
	}()
	go func() {
		<-start
		startMigrationDone <- svc.StartMigration("target-node", nil)
	}()
	close(start)

	if err := <-startMigrationDone; err != nil {
		t.Fatalf("StartMigration: %v", err)
	}
	if err := <-startActorDone; err != nil && err != ErrCodeServiceMigrating {
		t.Fatalf("StartActor: %v, want nil or %v", err, ErrCodeServiceMigrating)
	}

	actor, actorErr := svc.getActor(uid)
	if actorErr != nil {
		t.Fatalf("get actor: %v", actorErr)
	}
	if actor != nil {
		found := false
		for _, snapshot := range svc.MigrationSnapshot().Actors {
			if snapshot.UID == uid {
				found = true
				break
			}
		}
		if !found {
			t.Fatal("actor created concurrently with migration is missing from frozen plan")
		}
	}
}

func TestDuplicateMigrateStartReusesInboundTakeover(t *testing.T) {
	svc, netAgent := newPacketHandlerTestService(t, true, true)
	defer func() {
		if err := svc.Stop(); err != nil {
			t.Fatalf("stop service: %v", err)
		}
	}()

	uid := ActorUID{Category: 1, ID: 1018}
	registry := migrationTestRegistry(t, svc)
	seedMigrationSourceLease(t, registry, uid, "source-node", "source-lease")
	head := newS2SMigrateStartHead(1, uid, "source-node", "source-lease", false, ActorSession{})
	packet, err := svc.encodePacket(&head, nil)
	if err != nil {
		t.Fatalf("encode migrate start: %v", err)
	}
	if handleErr := svc.HandlePacket("source-node", packet); handleErr != nil {
		t.Fatalf("handle first migrate start: %v", handleErr)
	}
	packet, err = svc.encodePacket(&head, nil)
	if err != nil {
		t.Fatalf("encode duplicate migrate start: %v", err)
	}
	if handleErr := svc.HandlePacket("source-node", packet); handleErr != nil {
		t.Fatalf("handle duplicate migrate start: %v", handleErr)
	}

	waitForCondition(t, time.Second, func() bool {
		actor, _ := svc.getActor(uid)
		return actor != nil
	})
	if calls := registry.takeOverCalls(uid); calls != 1 {
		t.Fatalf("takeover call count = %d, want 1", calls)
	}
	waitForCondition(t, time.Second, func() bool {
		for _, send := range netAgent.snapshot() {
			pt, _ := decodePacketType(t, send.data)
			if pt == PacketTypeS2SMigrateResult && send.nodeId == "source-node" {
				return true
			}
		}
		return false
	})
}

func TestMigrateStartForDifferentLeaseGetsImmediateFailure(t *testing.T) {
	svc, netAgent := newPacketHandlerMigrationCTestService(t, true, true)
	defer func() {
		if err := svc.Stop(); err != nil {
			t.Fatalf("stop service: %v", err)
		}
	}()

	uid := svc.makeClientActorUID(1022)
	registry := migrationTestRegistry(t, svc)
	seedMigrationSourceLease(t, registry, uid, "source-node", "current-lease")
	current := newS2SMigrateStartHead(1, uid, "source-node", "current-lease", true, ActorSession{})
	packet, err := svc.encodePacket(&current, nil)
	if err != nil {
		t.Fatalf("encode current migrate start: %v", err)
	}
	if handleErr := svc.HandlePacket("source-node", packet); handleErr != nil {
		t.Fatalf("handle current migrate start: %v", handleErr)
	}
	waitForCondition(t, time.Second, func() bool {
		actor, _ := svc.getActor(uid)
		return actor != nil
	})

	stale := newS2SMigrateStartHead(2, uid, "source-node", "stale-lease", true, ActorSession{})
	packet, err = svc.encodePacket(&stale, nil)
	if err != nil {
		t.Fatalf("encode stale migrate start: %v", err)
	}
	if handleErr := svc.HandlePacket("source-node", packet); handleErr != nil {
		t.Fatalf("handle stale migrate start: %v", handleErr)
	}
	waitForCondition(t, time.Second, func() bool {
		for _, send := range netAgent.snapshot() {
			pt, buf := decodePacketType(t, send.data)
			if pt != PacketTypeS2SMigrateResult || send.nodeId != "source-node" {
				continue
			}
			var result s2sMigrateResultPacketHead
			if result.decode(&buf) == nil && result.startSeq == stale.seq && result.errCode == ErrCodeMigrationInProgress {
				return true
			}
		}
		return false
	})
}

func TestDuplicateAndOldMigrateResultDoNotRollbackMigratedActor(t *testing.T) {
	svc, _ := newPacketHandlerTestService(t, true, true)
	defer func() {
		if err := svc.Stop(); err != nil {
			t.Fatalf("stop service: %v", err)
		}
	}()

	uid := ActorUID{Category: 1, ID: 1019}
	actor, err := svc.startActor(uid, "")
	if err != nil {
		t.Fatalf("start actor: %v", err)
	}
	defer actor.core().deref()
	if migrateErr := svc.StartMigration("target-node", nil); migrateErr != nil {
		t.Fatalf("StartMigration: %v", migrateErr)
	}
	waitForCondition(t, time.Second, func() bool {
		return actor.core().isMigratingOut()
	})

	startSeq := actor.core().migrationRuntime().startSeq
	success := newS2SMigrateResultHead(svc.genSeq(), uid, actor.core().leaseID, startSeq, ErrCodeOK)
	packet, err := svc.encodePacket(&success, nil)
	if err != nil {
		t.Fatalf("encode success result: %v", err)
	}
	if handleErr := svc.HandlePacket("target-node", packet); handleErr != nil {
		t.Fatalf("handle success result: %v", handleErr)
	}
	waitForCondition(t, time.Second, func() bool {
		return actor.core().isMigrated()
	})

	late := newS2SMigrateResultHead(svc.genSeq(), uid, "old-lease", startSeq, ErrCodeInternalError)
	packet, err = svc.encodePacket(&late, nil)
	if err != nil {
		t.Fatalf("encode late result: %v", err)
	}
	if err := svc.HandlePacket("target-node", packet); err != nil {
		t.Fatalf("handle late result: %v", err)
	}
	if !actor.core().isMigrated() {
		t.Fatal("duplicate or old migration result rolled back migrated actor")
	}
}

func TestTargetRestartReturnsFailureInsteadOfTakingOverNewLease(t *testing.T) {
	svc, netAgent := newPacketHandlerTestService(t, true, true)
	defer func() {
		if err := svc.Stop(); err != nil {
			t.Fatalf("stop service: %v", err)
		}
	}()

	uid := ActorUID{Category: 1, ID: 1020}
	registry := migrationTestRegistry(t, svc)
	seedMigrationSourceLease(t, registry, uid, svc.nodeId(), "target-lease")
	head := newS2SMigrateStartHead(1, uid, "source-node", "source-lease", false, ActorSession{})
	packet, err := svc.encodePacket(&head, nil)
	if err != nil {
		t.Fatalf("encode migrate start: %v", err)
	}
	if err := svc.HandlePacket("source-node", packet); err != nil {
		t.Fatalf("handle migrate start: %v", err)
	}

	waitForCondition(t, time.Second, func() bool {
		for _, send := range netAgent.snapshot() {
			pt, buf := decodePacketType(t, send.data)
			if pt != PacketTypeS2SMigrateResult || send.nodeId != "source-node" {
				continue
			}
			var result s2sMigrateResultPacketHead
			if result.decode(&buf) == nil && result.errCode != ErrCodeOK {
				return true
			}
		}
		return false
	})
	if actor, _ := svc.getActor(uid); actor != nil {
		t.Fatal("target restart with a newer lease unexpectedly created an actor")
	}
}

func TestInboundCActorMigrationRestoresSession(t *testing.T) {
	svc, _ := newPacketHandlerMigrationCTestService(t, true, true)
	defer func() {
		if err := svc.Stop(); err != nil {
			t.Fatalf("stop service: %v", err)
		}
	}()

	uid := svc.makeClientActorUID(1021)
	registry := migrationTestRegistry(t, svc)
	seedMigrationSourceLease(t, registry, uid, "source-node", "source-lease")
	session := ActorSession{NodeId: "gateway-node", SID: 77}
	head := newS2SMigrateStartHead(1, uid, "source-node", "source-lease", true, session)
	packet, err := svc.encodePacket(&head, nil)
	if err != nil {
		t.Fatalf("encode migrate start: %v", err)
	}
	if err := svc.HandlePacket("source-node", packet); err != nil {
		t.Fatalf("handle migrate start: %v", err)
	}

	waitForCondition(t, time.Second, func() bool {
		actor, _ := svc.getActor(uid)
		ca, ok := actor.(*cactor)
		return ok && ca.session == session
	})
	actor, _ := svc.getActor(uid)
	if actor == nil {
		t.Fatal("inbound cactor was not started")
	}
	migration := actor.core().migrationRuntime()
	if migration == nil || migration.acceptedSourceNodeID != "source-node" || migration.acceptedSourceLeaseID != "source-lease" {
		t.Fatal("inbound cactor migration source binding is incomplete")
	}
	behavior, ok := actor.Behavior().(*packetHandlerMigrationTestCActor)
	if !ok {
		t.Fatal("unexpected inbound cactor behavior")
	}
	select {
	case ctx := <-behavior.inboundMigration:
		if ctx.UID != uid || ctx.SourceNodeID != "source-node" || ctx.SourceLeaseID != "source-lease" ||
			ctx.TargetNodeID != svc.nodeId() || ctx.TargetLeaseID == "" || ctx.StartSeq != head.seq || ctx.Session != session {
			t.Fatalf("unexpected inbound migration context: %+v", ctx)
		}
	case <-time.After(time.Second):
		t.Fatal("OnMigrateIn did not receive migration context")
	}
}

func TestHandleMigrateForwardRawReqRepliesToOriginNode(t *testing.T) {
	svc, netAgent := newPacketHandlerMigrationCTestService(t, true, true)
	defer func() {
		if err := svc.Stop(); err != nil {
			t.Fatalf("stop service: %v", err)
		}
	}()

	uid := svc.makeClientActorUID(1003)
	actor, startErr := svc.startActor(uid, "")
	if startErr != nil {
		t.Fatalf("start actor: %v", startErr)
	}
	migration := actor.core().ensureMigrationRuntime()
	migration.acceptedSourceNodeID = "source-node"
	migration.acceptedSourceLeaseID = "lease-source-1"
	if err := svc.send2LocalActor(uid, newMessageConnect("origin-node", 7), ""); err != nil {
		t.Fatalf("connect cactor: %v", err)
	}
	waitForCondition(t, time.Second, func() bool {
		ca, ok := actor.(*cactor)
		return ok && ca.session == (ActorSession{NodeId: "origin-node", SID: 7})
	})
	_ = actor.core().deref()

	innerHead := newRawReqHead(100, uid.ID, 7, 500)
	innerPacket, err := svc.encodeRawPacket(&innerHead, nil)
	if err != nil {
		t.Fatalf("encode inner raw req: %v", err)
	}
	outerHead := newS2SMigrateForwardHead(svc.genSeq(), uid, "lease-source-1", "origin-node", PacketTypeRawReq)
	outerPacket, err := svc.encodeRawPacket(&outerHead, innerPacket)
	if err != nil {
		t.Fatalf("encode migrate forward packet: %v", err)
	}

	if err := svc.HandlePacket("source-node", outerPacket); err != nil {
		t.Fatalf("handle migrate forward packet: %v", err)
	}

	waitForCondition(t, time.Second, func() bool {
		sends := netAgent.snapshot()
		for _, send := range sends {
			pt, _ := decodePacketType(t, send.data)
			if pt == PacketTypeRawResp && send.nodeId == "origin-node" {
				return true
			}
		}
		return false
	})
}
