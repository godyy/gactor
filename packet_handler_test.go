package gactor

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/godyy/gtimewheel"
)

type packetHandlerTestSend struct {
	nodeId string
	data   []byte
}

type packetHandlerTestNetAgent struct {
	mu    sync.Mutex
	sends []packetHandlerTestSend
}

func (na *packetHandlerTestNetAgent) Send2Node(nodeId string, b []byte) error {
	na.mu.Lock()
	defer na.mu.Unlock()

	cp := append([]byte(nil), b...)
	na.sends = append(na.sends, packetHandlerTestSend{
		nodeId: nodeId,
		data:   cp,
	})
	return nil
}

func (na *packetHandlerTestNetAgent) snapshot() []packetHandlerTestSend {
	na.mu.Lock()
	defer na.mu.Unlock()

	out := make([]packetHandlerTestSend, len(na.sends))
	copy(out, na.sends)
	return out
}

type packetHandlerTestServiceHandler struct {
	registry ActorRegistry
	router   ActorRouter
	net      NetAgent
	codec    PacketCodec
	ts       TimeSystem
}

func (h *packetHandlerTestServiceHandler) GetActorRegistry() ActorRegistry {
	return h.registry
}

func (h *packetHandlerTestServiceHandler) GetActorRouter() ActorRouter {
	return h.router
}

func (h *packetHandlerTestServiceHandler) GetNetAgent() NetAgent {
	return h.net
}

func (h *packetHandlerTestServiceHandler) GetPacketCodec() PacketCodec {
	return h.codec
}

func (h *packetHandlerTestServiceHandler) GetTimeSystem() TimeSystem {
	return h.ts
}

func (h *packetHandlerTestServiceHandler) GetMonitor() ServiceMonitor {
	return nil
}

type packetHandlerTestActor struct {
	Actor
}

func (a *packetHandlerTestActor) OnStart() error { return nil }
func (a *packetHandlerTestActor) OnStop() error  { return nil }

func newPacketHandlerTestService(t *testing.T, started bool, enableAck bool) (*Service, *packetHandlerTestNetAgent) {
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
				NewActorDefine(ActorDefineConfig{
					Name:           "packet-handler-test",
					Category:       1,
					Priority:       1,
					MessageBoxSize: 8,
					BehaviorCreator: func(a Actor) ActorBehavior {
						return &packetHandlerTestActor{Actor: a}
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
	}

	options := []ServiceOption{
		WithServiceLogger(logger.Named("packet-handler-test")),
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

type packetHandlerTestBytesManager struct{}

func (m *packetHandlerTestBytesManager) GetBytes(cap int) []byte {
	return make([]byte, 0, cap)
}

func (m *packetHandlerTestBytesManager) PutBytes(b []byte) {}

type packetHandlerTestClientResponse struct {
	id      ActorID
	sid     uint32
	errCode ErrCode
	payload []byte
}

type packetHandlerTestClientPush struct {
	id      ActorID
	sid     uint32
	payload []byte
}

type packetHandlerTestClientDisconnect struct {
	id  ActorID
	sid uint32
}

type packetHandlerTestClientHandler struct {
	registry ActorRegistry
	net      NetAgent
	bytes    BytesManager

	mu          sync.Mutex
	responses   []packetHandlerTestClientResponse
	pushes      []packetHandlerTestClientPush
	disconnects []packetHandlerTestClientDisconnect
}

func (h *packetHandlerTestClientHandler) GetActorRegistry() ActorRegistry {
	return h.registry
}

func (h *packetHandlerTestClientHandler) GetNetAgent() NetAgent {
	return h.net
}

func (h *packetHandlerTestClientHandler) GetBytesManager() BytesManager {
	return h.bytes
}

func (h *packetHandlerTestClientHandler) HandleResponse(resp ClientResponse) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.responses = append(h.responses, packetHandlerTestClientResponse{
		id:      resp.ID,
		sid:     resp.SID,
		errCode: resp.ErrCode,
		payload: append([]byte(nil), resp.Payload.UnreadData()...),
	})
}

func (h *packetHandlerTestClientHandler) HandlePush(push ClientPush) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.pushes = append(h.pushes, packetHandlerTestClientPush{
		id:      push.ID,
		sid:     push.SID,
		payload: append([]byte(nil), push.Payload.UnreadData()...),
	})
}

func (h *packetHandlerTestClientHandler) HandleDisconnect(id ActorID, sid uint32) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.disconnects = append(h.disconnects, packetHandlerTestClientDisconnect{id: id, sid: sid})
}

func (h *packetHandlerTestClientHandler) snapshotResponses() []packetHandlerTestClientResponse {
	h.mu.Lock()
	defer h.mu.Unlock()

	out := make([]packetHandlerTestClientResponse, len(h.responses))
	copy(out, h.responses)
	return out
}

func (h *packetHandlerTestClientHandler) snapshotPushes() []packetHandlerTestClientPush {
	h.mu.Lock()
	defer h.mu.Unlock()

	out := make([]packetHandlerTestClientPush, len(h.pushes))
	copy(out, h.pushes)
	return out
}

func (h *packetHandlerTestClientHandler) snapshotDisconnects() []packetHandlerTestClientDisconnect {
	h.mu.Lock()
	defer h.mu.Unlock()

	out := make([]packetHandlerTestClientDisconnect, len(h.disconnects))
	copy(out, h.disconnects)
	return out
}

func newPacketHandlerTestClient(t *testing.T) (*Client, *packetHandlerTestClientHandler) {
	t.Helper()

	handler := &packetHandlerTestClientHandler{
		registry: &testActorRegistry{
			actorMap: make(map[ActorUID]*testActorLocation),
		},
		net:   &packetHandlerTestNetAgent{},
		bytes: &packetHandlerTestBytesManager{},
	}

	cli := NewClient(&ClientConfig{
		NodeId:        "cli-node",
		ActorCategory: 1,
		Handler:       handler,
	}, WithClientLogger(logger.Named("packet-handler-client-test")))

	return cli, handler
}

func decodePacketType(t *testing.T, data []byte) (PacketType, Buffer) {
	t.Helper()

	var buf Buffer
	buf.SetBuf(data)

	pt, err := buf.readPacketType()
	if err != nil {
		t.Fatalf("read packet type: %v", err)
	}
	return pt, buf
}

func decodeAckPacketHead(t *testing.T, data []byte) ackPacketHead {
	t.Helper()

	pt, buf := decodePacketType(t, data)
	if pt != PacketTypeAck {
		t.Fatalf("packet type = %d, want %d", pt, PacketTypeAck)
	}

	var head ackPacketHead
	if err := head.decode(&buf); err != nil {
		t.Fatalf("decode ack head: %v", err)
	}
	return head
}

func decodeS2SRpcRespPacketHead(t *testing.T, data []byte) s2sRpcRespPacketHead {
	t.Helper()

	pt, buf := decodePacketType(t, data)
	if pt != PacketTypeS2SRpcResp {
		t.Fatalf("packet type = %d, want %d", pt, PacketTypeS2SRpcResp)
	}

	var head s2sRpcRespPacketHead
	if err := head.decode(&buf); err != nil {
		t.Fatalf("decode s2s rpc resp head: %v", err)
	}
	return head
}

func decodeRawRespPacketHead(t *testing.T, data []byte) rawRespPacketHead {
	t.Helper()

	pt, buf := decodePacketType(t, data)
	if pt != PacketTypeRawResp {
		t.Fatalf("packet type = %d, want %d", pt, PacketTypeRawResp)
	}

	var head rawRespPacketHead
	if err := head.decode(&buf); err != nil {
		t.Fatalf("decode raw resp head: %v", err)
	}
	return head
}

func encodeIncomingPacket(t *testing.T, ph packetHead, payload []byte) []byte {
	t.Helper()

	var buf Buffer
	buf.SetBuf(make([]byte, 0, sizeOfPacketType+ph.getSize()+len(payload)))
	if err := buf.writePacketType(ph.getPt()); err != nil {
		t.Fatalf("write packet type: %v", err)
	}
	if err := ph.encode(&buf); err != nil {
		t.Fatalf("encode packet head: %v", err)
	}
	if len(payload) > 0 {
		if _, err := buf.Write(payload); err != nil {
			t.Fatalf("write packet payload: %v", err)
		}
	}
	return buf.Data()
}

func TestServiceHandlePacketS2SRpcTimeoutDropsExpiredRequest(t *testing.T) {
	svc, netAgent := newPacketHandlerTestService(t, true, true)
	defer func() {
		if err := svc.Stop(); err != nil {
			t.Fatalf("stop service: %v", err)
		}
	}()

	head := newS2SRpcHead(
		1,
		2,
		ActorUID{Category: 1, ID: 101},
		ActorUID{Category: 1, ID: 202},
		10,
	)
	payload := &testS2SMessage{
		msgId:   msgIdPing,
		payload: &testMessagePing{Time: time.Now()},
	}

	b, err := svc.encodePacket(&head, payload)
	if err != nil {
		t.Fatalf("encode packet: %v", err)
	}

	if err := svc.HandlePacket("remote-node", b); err != nil {
		t.Fatalf("handle packet: %v", err)
	}

	sends := netAgent.snapshot()
	if len(sends) != 1 {
		t.Fatalf("send count = %d, want 1", len(sends))
	}
	if sends[0].nodeId != "remote-node" {
		t.Fatalf("send nodeId = %s, want remote-node", sends[0].nodeId)
	}

	ackHead := decodeAckPacketHead(t, sends[0].data)
	if ackHead.ackPt != PacketTypeS2SRpc {
		t.Fatalf("ack packet type = %d, want %d", ackHead.ackPt, PacketTypeS2SRpc)
	}
	if ackHead.ackSeq != head.seq {
		t.Fatalf("ack seq = %d, want %d", ackHead.ackSeq, head.seq)
	}
}

func TestServiceHandlePacketS2SRpcWhenNotStartedReturnsStoppedResp(t *testing.T) {
	svc, netAgent := newPacketHandlerTestService(t, false, false)

	head := newS2SRpcHead(
		11,
		22,
		ActorUID{Category: 1, ID: 301},
		ActorUID{Category: 1, ID: 302},
		1000,
	)
	payload := &testS2SMessage{
		msgId:   msgIdPing,
		payload: &testMessagePing{Time: time.Now()},
	}

	b, err := svc.encodePacket(&head, payload)
	if err != nil {
		t.Fatalf("encode packet: %v", err)
	}

	if err := svc.HandlePacket("remote-node", b); err != nil {
		t.Fatalf("handle packet: %v", err)
	}

	sends := netAgent.snapshot()
	if len(sends) != 1 {
		t.Fatalf("send count = %d, want 1", len(sends))
	}
	if sends[0].nodeId != "remote-node" {
		t.Fatalf("send nodeId = %s, want remote-node", sends[0].nodeId)
	}

	respHead := decodeS2SRpcRespPacketHead(t, sends[0].data)
	if respHead.reqId != head.reqId {
		t.Fatalf("resp reqId = %d, want %d", respHead.reqId, head.reqId)
	}
	if respHead.fromId != head.toId {
		t.Fatalf("resp fromId = %v, want %v", respHead.fromId, head.toId)
	}
	if respHead.toId != head.fromId {
		t.Fatalf("resp toId = %v, want %v", respHead.toId, head.fromId)
	}
	if respHead.errCode != ErrCodeServiceStopped {
		t.Fatalf("resp errCode = %v, want %v", respHead.errCode, ErrCodeServiceStopped)
	}
}

func TestClientHandlePacketRawRespDispatchesResponse(t *testing.T) {
	if logger == nil {
		if err := initLogger(); err != nil {
			t.Fatalf("init logger: %v", err)
		}
	}

	cli, handler := newPacketHandlerTestClient(t)
	defer cli.Stop()

	head := newRawRespHead(7, 9001, 88, ErrCodeOK)
	payload := []byte("hello-response")

	b, err := cli.encodePacket(&head, payload)
	if err != nil {
		t.Fatalf("encode packet: %v", err)
	}

	if err := cli.HandlePacket("remote-node", b); err != nil {
		t.Fatalf("handle packet: %v", err)
	}

	responses := handler.snapshotResponses()
	if len(responses) != 1 {
		t.Fatalf("response count = %d, want 1", len(responses))
	}

	resp := responses[0]
	if resp.id != 9001 {
		t.Fatalf("response id = %d, want 9001", resp.id)
	}
	if resp.sid != 88 {
		t.Fatalf("response sid = %d, want 88", resp.sid)
	}
	if resp.errCode != ErrCodeOK {
		t.Fatalf("response errCode = %v, want %v", resp.errCode, ErrCodeOK)
	}
	if string(resp.payload) != "hello-response" {
		t.Fatalf("response payload = %q, want %q", string(resp.payload), "hello-response")
	}
}

func TestServiceHandlePacketRawReqTimeoutDropsExpiredRequest(t *testing.T) {
	svc, netAgent := newPacketHandlerTestService(t, true, true)
	defer func() { _ = svc.Stop() }()

	head := newRawReqHead(31, 401, 99, 10)
	b := encodeIncomingPacket(t, &head, []byte("raw-timeout"))

	if err := svc.HandlePacket("remote-node", b); err != nil {
		t.Fatalf("handle packet: %v", err)
	}

	sends := netAgent.snapshot()
	if len(sends) != 1 {
		t.Fatalf("send count = %d, want 1", len(sends))
	}
	ackHead := decodeAckPacketHead(t, sends[0].data)
	if ackHead.ackPt != PacketTypeRawReq || ackHead.ackSeq != head.seq {
		t.Fatalf("unexpected ack head: %+v", ackHead)
	}
}

func TestServiceHandlePacketRawReqWhenNotStartedReturnsStoppedResp(t *testing.T) {
	svc, netAgent := newPacketHandlerTestService(t, false, false)

	head := newRawReqHead(41, 501, 77, 1000)
	b := encodeIncomingPacket(t, &head, []byte("raw-request"))

	if err := svc.HandlePacket("remote-node", b); err != nil {
		t.Fatalf("handle packet: %v", err)
	}

	sends := netAgent.snapshot()
	if len(sends) != 1 {
		t.Fatalf("send count = %d, want 1", len(sends))
	}
	respHead := decodeRawRespPacketHead(t, sends[0].data)
	if respHead.fromId != head.toId || respHead.sid != head.sid || respHead.errCode != ErrCodeServiceStopped {
		t.Fatalf("unexpected raw resp head: %+v", respHead)
	}
}

func TestClientHandlePacketAfterStopReturnsErrClientStopped(t *testing.T) {
	cli, _ := newPacketHandlerTestClient(t)
	cli.Stop()

	head := newRawRespHead(9, 1001, 66, ErrCodeOK)
	b, err := cli.encodePacket(&head, []byte("after-stop"))
	if err != nil {
		t.Fatalf("encode packet: %v", err)
	}

	if err := cli.HandlePacket("remote-node", b); !errors.Is(err, ErrClientStopped) {
		t.Fatalf("handle packet err = %v, want %v", err, ErrClientStopped)
	}
}

func TestServiceHandlePacketS2SRpcRespDispatchesCallback(t *testing.T) {
	svc, netAgent := newPacketHandlerTestService(t, true, true)
	defer func() {
		if err := svc.Stop(); err != nil {
			t.Fatalf("stop service: %v", err)
		}
	}()

	from := ActorUID{Category: 1, ID: 701}
	to := ActorUID{Category: 1, ID: 702}
	callbackCh := make(chan struct {
		err       error
		decodeErr error
		reply     testS2SMessage
	}, 1)
	markerDone := make(chan struct{})

	reqID, err := svc.rpcManager.createCall(from, to, time.Now().Add(time.Second), func(resp *RPCResp) {
		var reply testS2SMessage
		callbackCh <- struct {
			err       error
			decodeErr error
			reply     testS2SMessage
		}{
			err:       resp.Err(),
			decodeErr: resp.DecodeReply(&reply),
			reply:     reply,
		}
	})
	if err != nil {
		t.Fatalf("create call: %v", err)
	}
	if err := svc.rpcManager.enqueueCmd(&rpcTestCmd{execFn: func(*rpcManager) { close(markerDone) }}, false); err != nil {
		t.Fatalf("enqueue marker: %v", err)
	}
	select {
	case <-markerDone:
	case <-time.After(time.Second):
		t.Fatal("wait add-call marker timeout")
	}

	head := newS2SRpcRespHead(81, reqID, to, from, ErrCodeOK)
	payload := &testS2SMessage{msgId: msgIdPong, payload: &testMessagePong{Time: time.Now()}}
	b, err := svc.encodePacket(&head, payload)
	if err != nil {
		t.Fatalf("encode packet: %v", err)
	}
	if err := svc.HandlePacket("remote-node", b); err != nil {
		t.Fatalf("handle packet: %v", err)
	}

	select {
	case result := <-callbackCh:
		if result.err != nil {
			t.Fatalf("callback err = %v, want nil", result.err)
		}
		if result.decodeErr != nil {
			t.Fatalf("decode reply err = %v", result.decodeErr)
		}
		if result.reply.msgId != msgIdPong {
			t.Fatalf("reply msgId = %d, want %d", result.reply.msgId, msgIdPong)
		}
	case <-time.After(time.Second):
		t.Fatal("wait callback timeout")
	}

	sends := netAgent.snapshot()
	if len(sends) != 1 {
		t.Fatalf("send count = %d, want 1", len(sends))
	}
	ackHead := decodeAckPacketHead(t, sends[0].data)
	if ackHead.ackPt != PacketTypeS2SRpcResp || ackHead.ackSeq != head.seq {
		t.Fatalf("unexpected ack head: %+v", ackHead)
	}
}

func TestClientHandlePacketRawPushDispatchesPush(t *testing.T) {
	cli, handler := newPacketHandlerTestClient(t)
	defer cli.Stop()

	head := newRawPushHead(13, 2001, 77)
	b, err := cli.encodePacket(&head, []byte("push-data"))
	if err != nil {
		t.Fatalf("encode packet: %v", err)
	}
	if err := cli.HandlePacket("remote-node", b); err != nil {
		t.Fatalf("handle packet: %v", err)
	}

	pushes := handler.snapshotPushes()
	if len(pushes) != 1 {
		t.Fatalf("push count = %d, want 1", len(pushes))
	}
	if pushes[0].id != 2001 || pushes[0].sid != 77 || string(pushes[0].payload) != "push-data" {
		t.Fatalf("unexpected push: %+v", pushes[0])
	}
}

func TestClientHandlePacketDisconnectDispatchesDisconnect(t *testing.T) {
	cli, handler := newPacketHandlerTestClient(t)
	defer cli.Stop()

	head := newDisconnectHead(15, 3001, 88)
	b, err := cli.encodePacket(&head, nil)
	if err != nil {
		t.Fatalf("encode packet: %v", err)
	}
	if err := cli.HandlePacket("remote-node", b); err != nil {
		t.Fatalf("handle packet: %v", err)
	}

	disconnects := handler.snapshotDisconnects()
	if len(disconnects) != 1 {
		t.Fatalf("disconnect count = %d, want 1", len(disconnects))
	}
	if disconnects[0].id != 3001 || disconnects[0].sid != 88 {
		t.Fatalf("unexpected disconnect: %+v", disconnects[0])
	}
}
