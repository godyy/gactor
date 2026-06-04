package gactor

import (
	"sync"
	"testing"
	"time"
)

type clientAPITestSend struct {
	nodeId string
	data   []byte
}

type clientAPITestNetAgent struct {
	mu    sync.Mutex
	sends []clientAPITestSend
}

func (na *clientAPITestNetAgent) Send2Node(nodeId string, b []byte) error {
	na.mu.Lock()
	defer na.mu.Unlock()

	cp := append([]byte(nil), b...)
	na.sends = append(na.sends, clientAPITestSend{
		nodeId: nodeId,
		data:   cp,
	})
	return nil
}

func (na *clientAPITestNetAgent) snapshot() []clientAPITestSend {
	na.mu.Lock()
	defer na.mu.Unlock()

	out := make([]clientAPITestSend, len(na.sends))
	copy(out, na.sends)
	return out
}

type clientAPITestBytesManager struct{}

func (m *clientAPITestBytesManager) GetBytes(cap int) []byte {
	return make([]byte, 0, cap)
}

func (m *clientAPITestBytesManager) PutBytes(b []byte) {}

type clientAPITestHandler struct {
	registry ActorRegistry
	net      NetAgent
	bytes    BytesManager
}

func (h *clientAPITestHandler) GetActorRegistry() ActorRegistry {
	return h.registry
}

func (h *clientAPITestHandler) GetNetAgent() NetAgent {
	return h.net
}

func (h *clientAPITestHandler) GetBytesManager() BytesManager {
	return h.bytes
}

func (h *clientAPITestHandler) HandleResponse(resp ClientResponse) {}

func (h *clientAPITestHandler) HandlePush(push ClientPush) {}

func (h *clientAPITestHandler) HandleDisconnect(id int64, sid uint32) {}

func newClientAPITestClient(t *testing.T) (*Client, *clientAPITestNetAgent) {
	t.Helper()

	if logger == nil {
		if err := initLogger(); err != nil {
			t.Fatalf("init logger: %v", err)
		}
	}

	netAgent := &clientAPITestNetAgent{}
	handler := &clientAPITestHandler{
		registry: &testActorRegistry{
			actorMap: map[ActorUID]*testActorLocation{
				{Category: 1, ID: 1001}: {nodeId: "node-1001"},
				{Category: 1, ID: 1002}: {nodeId: "node-1002"},
			},
		},
		net:   netAgent,
		bytes: &clientAPITestBytesManager{},
	}

	cli := NewClient(&ClientConfig{
		NodeId:            "client-node",
		ActorCategory:     1,
		DefRequestTimeout: 5 * time.Second,
		Handler:           handler,
	}, WithClientLogger(logger.Named("client-api-test")))

	return cli, netAgent
}

func decodeClientPacketType(t *testing.T, data []byte) (PacketType, Buffer) {
	t.Helper()

	var buf Buffer
	buf.SetBuf(data)

	pt, err := buf.readPacketType()
	if err != nil {
		t.Fatalf("read packet type: %v", err)
	}
	return pt, buf
}

func decodeConnectPacketHead(t *testing.T, data []byte) connectPacketHead {
	t.Helper()

	pt, buf := decodeClientPacketType(t, data)
	if pt != PacketTypeConnect {
		t.Fatalf("packet type = %d, want %d", pt, PacketTypeConnect)
	}

	var head connectPacketHead
	if err := head.decode(&buf); err != nil {
		t.Fatalf("decode connect head: %v", err)
	}
	return head
}

func decodeDisconnectPacketHead(t *testing.T, data []byte) disconnectPacketHead {
	t.Helper()

	pt, buf := decodeClientPacketType(t, data)
	if pt != PacketTypeDisconnect {
		t.Fatalf("packet type = %d, want %d", pt, PacketTypeDisconnect)
	}

	var head disconnectPacketHead
	if err := head.decode(&buf); err != nil {
		t.Fatalf("decode disconnect head: %v", err)
	}
	return head
}

func decodeRawReqPacketHead(t *testing.T, data []byte) rawReqPacketHead {
	t.Helper()

	pt, buf := decodeClientPacketType(t, data)
	if pt != PacketTypeRawReq {
		t.Fatalf("packet type = %d, want %d", pt, PacketTypeRawReq)
	}

	var head rawReqPacketHead
	if err := head.decode(&buf); err != nil {
		t.Fatalf("decode raw req head: %v", err)
	}
	return head
}

func TestClientConnectSendsConnectPacket(t *testing.T) {
	cli, netAgent := newClientAPITestClient(t)
	defer cli.Stop()

	if err := cli.Connect(1001, 11); err != nil {
		t.Fatalf("connect: %v", err)
	}

	sends := netAgent.snapshot()
	if len(sends) != 1 {
		t.Fatalf("send count = %d, want 1", len(sends))
	}
	if sends[0].nodeId != "node-1001" {
		t.Fatalf("send nodeId = %s, want node-1001", sends[0].nodeId)
	}

	head := decodeConnectPacketHead(t, sends[0].data)
	if head.id != 1001 {
		t.Fatalf("connect id = %d, want 1001", head.id)
	}
	if head.sid != 11 {
		t.Fatalf("connect sid = %d, want 11", head.sid)
	}
}

func TestClientDisconnectSendsDisconnectPacket(t *testing.T) {
	cli, netAgent := newClientAPITestClient(t)
	defer cli.Stop()

	if err := cli.Disconnect(1002, 22); err != nil {
		t.Fatalf("disconnect: %v", err)
	}

	sends := netAgent.snapshot()
	if len(sends) != 1 {
		t.Fatalf("send count = %d, want 1", len(sends))
	}
	if sends[0].nodeId != "node-1002" {
		t.Fatalf("send nodeId = %s, want node-1002", sends[0].nodeId)
	}

	head := decodeDisconnectPacketHead(t, sends[0].data)
	if head.id != 1002 {
		t.Fatalf("disconnect id = %d, want 1002", head.id)
	}
	if head.sid != 22 {
		t.Fatalf("disconnect sid = %d, want 22", head.sid)
	}
}

func TestClientSendRequestUsesDefaultTimeout(t *testing.T) {
	cli, netAgent := newClientAPITestClient(t)
	defer cli.Stop()

	if err := cli.SendRequest(ClientRequest{
		ID:      1001,
		SID:     33,
		Timeout: 0,
		Payload: []byte("hello"),
	}); err != nil {
		t.Fatalf("send request: %v", err)
	}

	sends := netAgent.snapshot()
	if len(sends) != 1 {
		t.Fatalf("send count = %d, want 1", len(sends))
	}
	if sends[0].nodeId != "node-1001" {
		t.Fatalf("send nodeId = %s, want node-1001", sends[0].nodeId)
	}

	head := decodeRawReqPacketHead(t, sends[0].data)
	if head.toId != 1001 {
		t.Fatalf("request toId = %d, want 1001", head.toId)
	}
	if head.sid != 33 {
		t.Fatalf("request sid = %d, want 33", head.sid)
	}
	if head.timeout != uint32((5 * time.Second).Milliseconds()) {
		t.Fatalf("timeout = %d, want %d", head.timeout, uint32((5 * time.Second).Milliseconds()))
	}
}

func TestClientAPIsReturnErrClientStoppedAfterStop(t *testing.T) {
	cli, _ := newClientAPITestClient(t)
	cli.Stop()

	if err := cli.Connect(1001, 1); err != ErrClientStopped {
		t.Fatalf("connect err = %v, want %v", err, ErrClientStopped)
	}
	if err := cli.Disconnect(1001, 1); err != ErrClientStopped {
		t.Fatalf("disconnect err = %v, want %v", err, ErrClientStopped)
	}
	if err := cli.SendRequest(ClientRequest{
		ID:  1001,
		SID: 1,
	}); err != ErrClientStopped {
		t.Fatalf("send request err = %v, want %v", err, ErrClientStopped)
	}
}
