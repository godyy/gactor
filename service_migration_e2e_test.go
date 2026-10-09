package gactor

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/godyy/gtimewheel"
)

// migrationE2ENetAgent 在测试中直接把数据包投递到对应 Service。
// drop 用于注入一次性网络丢包，验证迁移控制包的重试与幂等逻辑。
type migrationE2ENetAgent struct {
	nodeID  string
	network *migrationE2ENetwork
}

func (a *migrationE2ENetAgent) Send2Node(nodeID string, b []byte) error {
	return a.network.deliver(a.nodeID, nodeID, b)
}

type migrationE2ENetwork struct {
	mu        sync.Mutex
	nodes     map[string]*Service
	drop      func(from, to string, data []byte) bool
	delivered map[PacketType]int
}

func migrationE2EPacketType(data []byte) PacketType {
	var buf Buffer
	buf.SetBuf(data)
	pt, _ := buf.readPacketType()
	return pt
}

func (n *migrationE2ENetwork) deliver(from, to string, b []byte) error {
	n.mu.Lock()
	target := n.nodes[to]
	drop := n.drop
	n.mu.Unlock()
	if target == nil {
		return fmt.Errorf("migration test node %q not found", to)
	}

	cp := append([]byte(nil), b...)
	pt := migrationE2EPacketType(cp)
	if drop != nil && drop(from, to, cp) {
		return nil
	}
	n.mu.Lock()
	n.delivered[pt]++
	n.mu.Unlock()
	return target.HandlePacket(from, cp)
}

func newMigrationE2EService(t *testing.T, nodeID string, registry ActorRegistry, network *migrationE2ENetwork) *Service {
	t.Helper()
	if logger == nil {
		if err := initLogger(); err != nil {
			t.Fatalf("init logger: %v", err)
		}
	}

	handler := &packetHandlerTestServiceHandler{
		registry: registry,
		router:   &testActorRouter{nodes: []string{nodeID}},
		net:      &migrationE2ENetAgent{nodeID: nodeID, network: network},
		codec:    &testPacketCodec{},
		ts:       DefTimeSystem,
	}
	svc := NewService(&ServiceConfig{
		NodeId: nodeID,
		ActorConfig: ActorConfig{
			ActorDefines: []ActorDefine{
				NewActorDefine(ActorDefineConfig{
					Name:              "migration-e2e",
					Category:          1,
					Priority:          1,
					PriMessageBoxSize: 8,
					MessageBoxSize:    8,
					BehaviorCreator: func(a Actor) ActorBehavior {
						return &packetHandlerTestActor{
							Actor:             a,
							outboundMigration: make(chan ActorMigrationContext, 1),
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
		},
		RPCConfig: RPCConfig{
			DefRPCTimeout:    time.Second,
			MaxRPCCallAmount: 16,
		},
		MaxRTT:  50,
		Handler: handler,
	}, WithServiceLogger(logger.Named("migration-e2e-"+nodeID)))
	if err := svc.Start(); err != nil {
		t.Fatalf("start %s: %v", nodeID, err)
	}
	network.mu.Lock()
	network.nodes[nodeID] = svc
	network.mu.Unlock()
	return svc
}

func TestMigrationEndToEndAcrossServices(t *testing.T) {
	network := &migrationE2ENetwork{
		nodes:     make(map[string]*Service),
		delivered: make(map[PacketType]int),
	}
	registry := &testActorRegistry{actorMap: make(map[ActorUID]*testActorLocation)}
	source := newMigrationE2EService(t, "source-node", registry, network)
	target := newMigrationE2EService(t, "target-node", registry, network)
	defer func() {
		_ = source.Stop()
		_ = target.Stop()
	}()

	uid := ActorUID{Category: 1, ID: 7001}
	actor, err := source.startActor(uid, "")
	if err != nil {
		t.Fatalf("start source actor: %v", err)
	}
	if releaseErr := actor.core().deref(); releaseErr != nil {
		t.Fatalf("release source actor reference: %v", releaseErr)
	}

	if migrateErr := source.StartMigration("target-node", nil); migrateErr != nil {
		t.Fatalf("start migration: %v", migrateErr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if waitErr := source.WaitMigrationConverged(ctx); waitErr != nil {
		t.Fatalf("wait migration convergence: %v", waitErr)
	}
	if !actor.core().isMigrated() {
		t.Fatal("source actor did not enter migrated forwarding state")
	}

	waitForCondition(t, time.Second, func() bool {
		targetActor, _ := target.getActor(uid)
		return targetActor != nil
	})
	location, err := registry.GetActorLocation(uid)
	if err != nil {
		t.Fatalf("get migrated actor location: %v", err)
	}
	if location.NodeId != "target-node" {
		t.Fatalf("registry node = %q, want target-node", location.NodeId)
	}

	if err := source.FinishMigrationForwarding(); err != nil {
		t.Fatalf("finish forwarding: %v", err)
	}
	if err := source.FinalizeMigration(ctx); err != nil {
		t.Fatalf("finalize migration: %v", err)
	}
	if source.MigrationSnapshot().Started {
		t.Fatal("source migration state remains after finalization")
	}
}

func TestMigrationEndToEndRecoversFromDroppedResult(t *testing.T) {
	var dropped atomic.Bool
	network := &migrationE2ENetwork{
		nodes:     make(map[string]*Service),
		delivered: make(map[PacketType]int),
		drop: func(from, to string, data []byte) bool {
			pt := migrationE2EPacketType(data)
			if from == "target-node" && to == "source-node" && pt == PacketTypeS2SMigrateResult &&
				dropped.CompareAndSwap(false, true) {
				return true
			}
			return false
		},
	}
	registry := &testActorRegistry{actorMap: make(map[ActorUID]*testActorLocation)}
	source := newMigrationE2EService(t, "source-node", registry, network)
	target := newMigrationE2EService(t, "target-node", registry, network)
	defer func() {
		_ = source.Stop()
		_ = target.Stop()
	}()

	uid := ActorUID{Category: 1, ID: 7002}
	actor, err := source.startActor(uid, "")
	if err != nil {
		t.Fatalf("start source actor: %v", err)
	}
	if err := actor.core().deref(); err != nil {
		t.Fatalf("release source actor reference: %v", err)
	}
	if err := source.StartMigration("target-node", nil); err != nil {
		t.Fatalf("start migration: %v", err)
	}
	waitForCondition(t, time.Second, func() bool {
		return dropped.Load() && actor.core().isMigratingOut()
	})

	startSeq := actor.core().migrationRuntime().startSeq
	actor.core().receiveTriggerdTimer(
		TimerIdNone,
		migrationResultTimerArgs{StartSeq: startSeq},
		actorOnMigrationResultTimeout,
	)
	waitForCondition(t, 3*time.Second, func() bool {
		return actor.core().isMigrated()
	})
	if registry.takeOverCalls(uid) != 1 {
		t.Fatalf("takeover calls = %d, want 1 after duplicate start", registry.takeOverCalls(uid))
	}
}

func TestMigrationPacketHandlersRejectMalformedPackets(t *testing.T) {
	svc, _ := newPacketHandlerTestService(t, true, false)
	defer func() { _ = svc.Stop() }()

	for _, pt := range []PacketType{
		PacketTypeS2SMigrateStart,
		PacketTypeS2SMigrateResult,
		PacketTypeS2SMigrateForward,
	} {
		t.Run(fmt.Sprintf("packet_%d", pt), func(t *testing.T) {
			packet := make([]byte, sizeOfPacketType+1)
			var buf Buffer
			buf.SetBuf(packet)
			if err := buf.writePacketType(pt); err != nil {
				t.Fatalf("write packet type: %v", err)
			}
			if err := svc.HandlePacket("source-node", packet); err == nil {
				t.Fatal("malformed migration packet unexpectedly succeeded")
			}
		})
	}
}

func FuzzMigrationPacketHandlers(f *testing.F) {
	f.Add([]byte{0, byte(PacketTypeS2SMigrateStart)})
	f.Add([]byte{0, byte(PacketTypeS2SMigrateResult), 1, 2, 3})
	f.Add([]byte{0, byte(PacketTypeS2SMigrateForward), 1, 2, 3, 4})

	f.Fuzz(func(t *testing.T, packet []byte) {
		svc, _ := newPacketHandlerTestService(t, true, false)
		defer func() { _ = svc.Stop() }()
		_ = svc.HandlePacket("source-node", append([]byte(nil), packet...))
	})
}
