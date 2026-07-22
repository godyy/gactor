package gactor

import (
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/godyy/gtimewheel"
)

type forwardTestSend struct {
	from   string
	nodeId string
	data   []byte
}

type forwardTestNetwork struct {
	mu       sync.Mutex
	handlers map[string]func(string, []byte) error
	sends    []forwardTestSend
}

func newForwardTestNetwork() *forwardTestNetwork {
	return &forwardTestNetwork{
		handlers: make(map[string]func(string, []byte) error),
	}
}

func (n *forwardTestNetwork) register(nodeId string, handler func(string, []byte) error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[nodeId] = handler
}

func (n *forwardTestNetwork) send(from, nodeId string, b []byte) error {
	cp := append([]byte(nil), b...)

	n.mu.Lock()
	n.sends = append(n.sends, forwardTestSend{
		from:   from,
		nodeId: nodeId,
		data:   cp,
	})
	handler := n.handlers[nodeId]
	n.mu.Unlock()

	if handler == nil {
		return nil
	}
	return handler(from, append([]byte(nil), cp...))
}

func (n *forwardTestNetwork) snapshot() []forwardTestSend {
	n.mu.Lock()
	defer n.mu.Unlock()

	out := make([]forwardTestSend, len(n.sends))
	copy(out, n.sends)
	return out
}

type forwardTestNetAgent struct {
	nodeId  string
	network *forwardTestNetwork
}

func (na *forwardTestNetAgent) Send2Node(nodeId string, b []byte) error {
	return na.network.send(na.nodeId, nodeId, b)
}

type forwardTestActor struct {
	Actor
}

func (a *forwardTestActor) OnStart() error { return nil }
func (a *forwardTestActor) OnStop() error  { return nil }

type forwardTestCActor struct {
	CActor
}

func (a *forwardTestCActor) OnStart() error  { return nil }
func (a *forwardTestCActor) OnStop() error   { return nil }
func (a *forwardTestCActor) OnConnected()    {}
func (a *forwardTestCActor) OnDisconnected() {}

type forwardTestServiceHandler struct {
	registry ActorRegistry
	router   ActorRouter
	net      NetAgent
	codec    PacketCodec
	ts       TimeSystem
}

func (h *forwardTestServiceHandler) GetActorRegistry() ActorRegistry { return h.registry }
func (h *forwardTestServiceHandler) GetActorRouter() ActorRouter     { return h.router }
func (h *forwardTestServiceHandler) GetNetAgent() NetAgent           { return h.net }
func (h *forwardTestServiceHandler) GetPacketCodec() PacketCodec     { return h.codec }
func (h *forwardTestServiceHandler) GetTimeSystem() TimeSystem       { return h.ts }
func (h *forwardTestServiceHandler) GetMonitor() ServiceMonitor      { return nil }

func newForwardTestService(t *testing.T, nodeId string, registry *testActorRegistry, network *forwardTestNetwork) *Service {
	return newForwardTestServiceWithCodec(t, nodeId, registry, network, &testPacketCodec{})
}

type forwardTestCountingCodec struct {
	testPacketCodec
	mu       sync.Mutex
	putCount int
}

func (c *forwardTestCountingCodec) PutBytes(b []byte) {
	c.mu.Lock()
	c.putCount++
	c.mu.Unlock()
}

func (c *forwardTestCountingCodec) snapshotPutCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.putCount
}

func newForwardTestServiceWithCodec(t *testing.T, nodeId string, registry *testActorRegistry, network *forwardTestNetwork, codec PacketCodec) *Service {
	t.Helper()

	if logger == nil {
		if err := initLogger(); err != nil {
			t.Fatalf("init logger: %v", err)
		}
	}

	handler := &forwardTestServiceHandler{
		registry: registry,
		router: &testActorRouter{
			nodes: []string{nodeId},
		},
		net: &forwardTestNetAgent{
			nodeId:  nodeId,
			network: network,
		},
		codec: codec,
		ts:    DefTimeSystem,
	}

	cfg := &ServiceConfig{
		NodeId: nodeId,
		ActorConfig: ActorConfig{
			ActorDefines: []ActorDefine{
				NewActorDefine(ActorDefineConfig{
					Name:           "forward-test-actor",
					Category:       1,
					Priority:       1,
					MessageBoxSize: 8,
					BehaviorCreator: func(a Actor) ActorBehavior {
						return &forwardTestActor{Actor: a}
					},
				}),
				NewCActorDefine(CActorDefineConfig{
					Name:           "forward-test-cactor",
					Category:       2,
					Priority:       1,
					MessageBoxSize: 8,
					RecycleTime:    time.Minute,
					BehaviorCreator: func(a CActor) CActorBehavior {
						return &forwardTestCActor{CActor: a}
					},
				}),
			},
			ClientActorCategory: 2,
			RegistryTTL:         30,
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
			DefRPCTimeout:    200 * time.Millisecond,
			MaxRPCCallAmount: 16,
		},
		MaxRTT:  50,
		Handler: handler,
	}

	svc := NewService(cfg, WithServiceLogger(logger.Named("forward-test-"+nodeId)))
	if err := svc.Start(); err != nil {
		t.Fatalf("start service: %v", err)
	}

	network.register(nodeId, func(from string, b []byte) error {
		return svc.HandlePacket(from, b)
	})

	return svc
}

func waitForwardTestCondition(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal(msg)
}

func decodeRawPushPacketHead(t *testing.T, data []byte) (rawPushPacketHead, []byte) {
	t.Helper()

	pt, buf := decodePacketType(t, data)
	if pt != PacketTypeRawPush {
		t.Fatalf("packet type = %d, want %d", pt, PacketTypeRawPush)
	}

	var head rawPushPacketHead
	if err := head.decode(&buf); err != nil {
		t.Fatalf("decode raw push head: %v", err)
	}

	return head, append([]byte(nil), buf.UnreadData()...)
}

func newForwardTestMessage(msg string) *testS2SMessage {
	return &testS2SMessage{
		msgId:   msgIdCast,
		payload: &testMessageCast{Msg: msg},
	}
}

func decodeForwardTestMessage(t *testing.T, payload []byte) testMessageCast {
	t.Helper()

	var buf Buffer
	buf.SetBuf(payload)

	msgId, err := buf.ReadUint32()
	if err != nil {
		t.Fatalf("read forward msg id: %v", err)
	}
	if int(msgId) != msgIdCast {
		t.Fatalf("forward msg id = %d, want %d", msgId, msgIdCast)
	}

	var msg testMessageCast
	if err = json.Unmarshal(buf.UnreadData(), &msg); err != nil {
		t.Fatalf("decode forward payload: %v", err)
	}
	return msg
}

func assertForwardTestAckOnly(t *testing.T, sends []forwardTestSend, from, to string) {
	t.Helper()

	for _, send := range sends {
		if send.from != from || send.nodeId != to {
			continue
		}
		pt, _ := decodePacketType(t, send.data)
		if pt != PacketTypeAck {
			t.Fatalf("packet type from %s to %s = %d, want ack only", from, to, pt)
		}
	}
}

func startForwardTestCActor(t *testing.T, svc *Service, uid ActorUID) {
	t.Helper()

	actor, err := svc.startActor(uid, "")
	if err != nil {
		t.Fatalf("start actor: %v", err)
	}
	_ = actor.core().deref()
}

func connectForwardTestCActor(t *testing.T, svc *Service, uid ActorUID, sid uint32) {
	t.Helper()

	startForwardTestCActor(t, svc, uid)
	if err := svc.send2LocalActor(uid, newMessageConnect("client-node", sid), ""); err != nil {
		t.Fatalf("connect actor: %v", err)
	}

	waitForwardTestCondition(t, time.Second, func() bool {
		actor, err := svc.getActor(uid)
		if err != nil || actor == nil {
			return false
		}
		ca, ok := actor.(*cactor)
		return ok && ca.session.NodeId == "client-node" && ca.session.SID == sid
	}, "wait actor connected timeout")
}

func TestServiceForwardRejectsUnsupportedActor(t *testing.T) {
	registry := &testActorRegistry{actorMap: make(map[ActorUID]*testActorLocation)}
	network := newForwardTestNetwork()
	svc := newForwardTestService(t, "node-a", registry, network)
	defer func() { _ = svc.Stop() }()

	err := svc.Forward(ActorUID{Category: 1, ID: 1}, newForwardTestMessage("payload"))
	if !errors.Is(err, ErrCodeActorForwardUnsupported) {
		t.Fatalf("forward err = %v, want %v", err, ErrCodeActorForwardUnsupported)
	}
}

func TestServiceForwardLocalConnectedActorPushesRawPayload(t *testing.T) {
	registry := &testActorRegistry{actorMap: make(map[ActorUID]*testActorLocation)}
	network := newForwardTestNetwork()
	svc := newForwardTestService(t, "node-a", registry, network)
	defer func() { _ = svc.Stop() }()

	uid := ActorUID{Category: 2, ID: 1001}
	connectForwardTestCActor(t, svc, uid, 77)

	payload := newForwardTestMessage("raw-forward-local")
	if err := svc.Forward(uid, payload); err != nil {
		t.Fatalf("forward: %v", err)
	}

	waitForwardTestCondition(t, time.Second, func() bool {
		for _, send := range network.snapshot() {
			if send.nodeId == "client-node" {
				return true
			}
		}
		return false
	}, "wait local push timeout")

	var push forwardTestSend
	for _, send := range network.snapshot() {
		if send.nodeId == "client-node" {
			push = send
			break
		}
	}

	head, gotPayload := decodeRawPushPacketHead(t, push.data)
	if head.fromId != uid.ID || head.sid != 77 {
		t.Fatalf("unexpected raw push head: %+v", head)
	}
	gotMsg := decodeForwardTestMessage(t, gotPayload)
	if gotMsg.Msg != "raw-forward-local" {
		t.Fatalf("push payload msg = %q, want %q", gotMsg.Msg, "raw-forward-local")
	}
}

func TestServiceForwardDisconnectedActorIgnoresPayload(t *testing.T) {
	registry := &testActorRegistry{actorMap: make(map[ActorUID]*testActorLocation)}
	network := newForwardTestNetwork()
	svc := newForwardTestService(t, "node-a", registry, network)
	defer func() { _ = svc.Stop() }()

	uid := ActorUID{Category: 2, ID: 1002}
	startForwardTestCActor(t, svc, uid)

	if err := svc.Forward(uid, newForwardTestMessage("ignore-me")); err != nil {
		t.Fatalf("forward: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	for _, send := range network.snapshot() {
		if send.nodeId == "client-node" {
			t.Fatalf("unexpected push to client: %+v", send)
		}
	}
}

func TestServiceForwardDoesNotWakeOfflineLocalActor(t *testing.T) {
	registry := &testActorRegistry{actorMap: make(map[ActorUID]*testActorLocation)}
	network := newForwardTestNetwork()
	svc := newForwardTestService(t, "node-a", registry, network)
	defer func() { _ = svc.Stop() }()

	uid := ActorUID{Category: 2, ID: 1003}
	registry.actorMap[uid] = &testActorLocation{
		nodeId:   "node-a",
		leaseId:  "stale-lease",
		expireAt: time.Now().Add(time.Minute).Unix(),
	}

	err := svc.Forward(uid, newForwardTestMessage("stale"))
	if !errors.Is(err, ErrCodeActorNotExists) {
		t.Fatalf("forward err = %v, want %v", err, ErrCodeActorNotExists)
	}

	actor, getErr := svc.getActor(uid)
	if getErr != nil {
		t.Fatalf("get actor: %v", getErr)
	}
	if actor != nil {
		t.Fatalf("actor = %v, want nil", actor)
	}
}

func TestServiceForwardRemoteSuccessPushesRawPayload(t *testing.T) {
	registry := &testActorRegistry{actorMap: make(map[ActorUID]*testActorLocation)}
	network := newForwardTestNetwork()
	src := newForwardTestService(t, "node-a", registry, network)
	defer func() { _ = src.Stop() }()
	dst := newForwardTestService(t, "node-b", registry, network)
	defer func() { _ = dst.Stop() }()

	uid := ActorUID{Category: 2, ID: 2001}
	connectForwardTestCActor(t, dst, uid, 88)

	payload := newForwardTestMessage("raw-forward-remote")
	if err := src.Forward(uid, payload); err != nil {
		t.Fatalf("forward: %v", err)
	}

	waitForwardTestCondition(t, time.Second, func() bool {
		for _, send := range network.snapshot() {
			if send.from == "node-b" && send.nodeId == "client-node" {
				return true
			}
		}
		return false
	}, "wait remote push timeout")

	var push forwardTestSend
	for _, send := range network.snapshot() {
		if send.from == "node-b" && send.nodeId == "client-node" {
			push = send
			break
		}
	}

	head, gotPayload := decodeRawPushPacketHead(t, push.data)
	if head.fromId != uid.ID || head.sid != 88 {
		t.Fatalf("unexpected raw push head: %+v", head)
	}
	gotMsg := decodeForwardTestMessage(t, gotPayload)
	if gotMsg.Msg != "raw-forward-remote" {
		t.Fatalf("push payload msg = %q, want %q", gotMsg.Msg, "raw-forward-remote")
	}

	assertForwardTestAckOnly(t, network.snapshot(), "node-b", "node-a")
}

func TestMessageForwardReleaseUsesActorService(t *testing.T) {
	registry := &testActorRegistry{actorMap: make(map[ActorUID]*testActorLocation)}
	network := newForwardTestNetwork()
	codec := &forwardTestCountingCodec{}
	svc := newForwardTestServiceWithCodec(t, "node-a", registry, network, codec)
	defer func() { _ = svc.Stop() }()

	uid := ActorUID{Category: 2, ID: 2003}
	startForwardTestCActor(t, svc, uid)

	actor, err := svc.getActor(uid)
	if err != nil {
		t.Fatalf("get actor: %v", err)
	}
	ca, ok := actor.(*cactor)
	if !ok {
		t.Fatalf("actor type = %T, want *cactor", actor)
	}

	before := codec.snapshotPutCount()
	buf := Buffer{}
	buf.SetBuf([]byte("release-buffer"))
	actorHandleMsg(ca, newMessageForward(buf))
	if got := codec.snapshotPutCount(); got != before+1 {
		t.Fatalf("put count = %d, want %d", got, before+1)
	}
}

func TestServiceForwardRemoteOfflineActorReturnsNil(t *testing.T) {
	registry := &testActorRegistry{actorMap: make(map[ActorUID]*testActorLocation)}
	network := newForwardTestNetwork()
	src := newForwardTestService(t, "node-a", registry, network)
	defer func() { _ = src.Stop() }()
	dst := newForwardTestService(t, "node-b", registry, network)
	defer func() { _ = dst.Stop() }()

	uid := ActorUID{Category: 2, ID: 2002}
	registry.actorMap[uid] = &testActorLocation{
		nodeId:   "node-b",
		leaseId:  "stale-remote",
		expireAt: time.Now().Add(-time.Minute).Unix(),
	}

	if err := src.Forward(uid, newForwardTestMessage("remote-stale")); err != nil {
		t.Fatalf("forward err = %v, want nil", err)
	}

	time.Sleep(50 * time.Millisecond)

	foundRemoteForward := false
	for _, send := range network.snapshot() {
		if send.from != "node-a" || send.nodeId != "node-b" {
			continue
		}
		pt, _ := decodePacketType(t, send.data)
		if pt == PacketTypeS2SForward {
			foundRemoteForward = true
			break
		}
	}
	if !foundRemoteForward {
		t.Fatal("expected forward request sent from source node to remote node")
	}

	assertForwardTestAckOnly(t, network.snapshot(), "node-b", "node-a")

	actor, getErr := dst.getActor(uid)
	if getErr != nil {
		t.Fatalf("get actor: %v", getErr)
	}
	if actor != nil {
		t.Fatalf("actor = %v, want nil", actor)
	}
}
