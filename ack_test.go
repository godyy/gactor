package gactor

import (
	"sync"
	"testing"
	"time"

	"github.com/godyy/gactor/internal/utils"
	"github.com/godyy/glog"
)

type ackTestEvent struct {
	packet ackPacket
	reason ackOverReason
}

type ackTestHandler struct {
	logger   glog.Logger
	stopWait *utils.StopWait

	mu      sync.Mutex
	retries []ackPacket
	overs   []ackTestEvent
	retryCh chan ackPacket
	overCh  chan ackTestEvent
}

func newAckTestHandler(t *testing.T) *ackTestHandler {
	t.Helper()

	if logger == nil {
		if err := initLogger(); err != nil {
			t.Fatalf("init logger: %v", err)
		}
	}

	return &ackTestHandler{
		logger:   logger.Named("ack-test"),
		stopWait: utils.NewStopWait(),
		retryCh:  make(chan ackPacket, 8),
		overCh:   make(chan ackTestEvent, 8),
	}
}

func (h *ackTestHandler) onAckRetry(ap ackPacket) {
	h.mu.Lock()
	h.retries = append(h.retries, ap)
	h.mu.Unlock()
	h.retryCh <- ap
}

func (h *ackTestHandler) onAckOver(ap ackPacket, reason ackOverReason) {
	event := ackTestEvent{packet: ap, reason: reason}
	h.mu.Lock()
	h.overs = append(h.overs, event)
	h.mu.Unlock()
	h.overCh <- event
}

func (h *ackTestHandler) getLogger() glog.Logger {
	return h.logger
}

func (h *ackTestHandler) getStopWait() *utils.StopWait {
	return h.stopWait
}

func (h *ackTestHandler) snapshot() (retries []ackPacket, overs []ackTestEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()

	retries = append(retries, h.retries...)
	overs = append(overs, h.overs...)
	return
}

func newAckTestManager(t *testing.T, cfg *AckConfig) (*ackManager, *ackTestHandler) {
	t.Helper()

	handler := newAckTestHandler(t)
	mgr := newAckManager(cfg, handler)
	return mgr, handler
}

func newAckTestPacket2Ack(ap ackPacket) *packet2Ack {
	return &packet2Ack{ackPacket: ap}
}

func waitAckOver(t *testing.T, h *ackTestHandler) ackTestEvent {
	t.Helper()

	select {
	case event := <-h.overCh:
		return event
	case <-time.After(time.Second):
		t.Fatal("wait ack over timeout")
		return ackTestEvent{}
	}
}

func assertAckManagerEmpty(t *testing.T, m *ackManager) {
	t.Helper()

	m.mtx.Lock()
	defer m.mtx.Unlock()

	if m.l.Len() != 0 {
		t.Fatalf("expected ack list empty, got len=%d", m.l.Len())
	}
	if len(m.m) != 0 {
		t.Fatalf("expected ack map empty, got len=%d", len(m.m))
	}
}

func expireFrontPacket(t *testing.T, m *ackManager) {
	t.Helper()

	m.mtx.Lock()
	defer m.mtx.Unlock()

	if m.l.Len() != 1 {
		t.Fatalf("expected exactly one packet in list, got %d", m.l.Len())
	}
	p := m.l.Front().Value.(*packet2Ack)
	p.expireAt = time.Now().Add(-time.Millisecond).UnixNano()
}

func TestAckManagerAddPacketReceiveAck(t *testing.T) {
	mgr, handler := newAckTestManager(t, &AckConfig{
		MaxPacketAmount: 8,
		Timeout:         time.Second,
		MaxRetry:        2,
		TickInterval:    10 * time.Millisecond,
	})
	mgr.start()
	defer handler.stopWait.Stop(true)

	packet := ackPacket{
		pt:     PacketTypeS2SRpc,
		seq:    1,
		b:      []byte{1, 2, 3},
		nodeId: "node-1",
	}

	mgr.addPacket(packet)
	mgr.receiveAck(packet.pt, packet.seq)

	event := waitAckOver(t, handler)
	if event.reason != ackOverReasonOK {
		t.Fatalf("ack over reason = %v, want %v", event.reason, ackOverReasonOK)
	}
	if event.packet.pt != packet.pt || event.packet.seq != packet.seq || event.packet.nodeId != packet.nodeId {
		t.Fatalf("unexpected ack over packet: %+v", event.packet)
	}

	retries, overs := handler.snapshot()
	if len(retries) != 0 {
		t.Fatalf("unexpected retries: %d", len(retries))
	}
	if len(overs) != 1 {
		t.Fatalf("ack over count = %d, want 1", len(overs))
	}

	assertAckManagerEmpty(t, mgr)
}

func TestAckManagerRemPacketNotifiesRemoved(t *testing.T) {
	mgr, handler := newAckTestManager(t, &AckConfig{
		MaxPacketAmount: 8,
		Timeout:         time.Second,
		MaxRetry:        2,
		TickInterval:    10 * time.Millisecond,
	})
	mgr.start()
	defer handler.stopWait.Stop(true)

	packet := ackPacket{
		pt:     PacketTypeS2SCast,
		seq:    2,
		b:      []byte{4, 5, 6},
		nodeId: "node-2",
	}

	mgr.addPacket(packet)
	mgr.remPacket(packet.pt, packet.seq)

	event := waitAckOver(t, handler)
	if event.reason != ackOverReasonRem {
		t.Fatalf("ack over reason = %v, want %v", event.reason, ackOverReasonRem)
	}
	if event.packet.pt != packet.pt || event.packet.seq != packet.seq || event.packet.nodeId != packet.nodeId {
		t.Fatalf("unexpected removed packet: %+v", event.packet)
	}

	retries, overs := handler.snapshot()
	if len(retries) != 0 {
		t.Fatalf("unexpected retries: %d", len(retries))
	}
	if len(overs) != 1 {
		t.Fatalf("ack over count = %d, want 1", len(overs))
	}

	assertAckManagerEmpty(t, mgr)
}

func TestAckManagerUpdateExpirePacketsRetryThenRetryLimit(t *testing.T) {
	mgr, handler := newAckTestManager(t, &AckConfig{
		MaxPacketAmount: 8,
		Timeout:         20 * time.Millisecond,
		MaxRetry:        2,
		TickInterval:    10 * time.Millisecond,
	})

	packet := ackPacket{
		pt:     PacketTypeRawReq,
		seq:    3,
		b:      []byte{7, 8, 9},
		nodeId: "node-3",
	}

	mgr.addPacketInternal(newAckTestPacket2Ack(packet))

	expireFrontPacket(t, mgr)
	mgr.updateExpirePackets()

	expireFrontPacket(t, mgr)
	mgr.updateExpirePackets()

	retries, overs := handler.snapshot()
	if len(retries) != 2 {
		t.Fatalf("retry count = %d, want 2", len(retries))
	}
	if len(overs) != 0 {
		t.Fatalf("unexpected ack over count before retry limit: %d", len(overs))
	}

	expireFrontPacket(t, mgr)
	mgr.updateExpirePackets()

	retries, overs = handler.snapshot()
	if len(retries) != 2 {
		t.Fatalf("retry count = %d, want 2", len(retries))
	}
	if len(overs) != 1 {
		t.Fatalf("ack over count = %d, want 1", len(overs))
	}
	if overs[0].reason != ackOverReasonRetryLimit {
		t.Fatalf("ack over reason = %v, want %v", overs[0].reason, ackOverReasonRetryLimit)
	}
	if overs[0].packet.pt != packet.pt || overs[0].packet.seq != packet.seq || overs[0].packet.nodeId != packet.nodeId {
		t.Fatalf("unexpected retry-limit packet: %+v", overs[0].packet)
	}

	assertAckManagerEmpty(t, mgr)
}
