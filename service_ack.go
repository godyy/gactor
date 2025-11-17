package gactor

import (
	"context"
	"errors"
)

// startAckManager 启动 ackManager.
func (s *Service) startAckManager() {
	if s.ackManager == nil {
		return
	}

	s.ackManager.start()
}

// ackEnabled Ack 管理器是否启用.
func (s *Service) ackEnabled() bool {
	return s.ackManager != nil
}

// addPacket2Ack 添加待确认数据包.
func (s *Service) addPacket2Ack(nodeId string, pt PacketType, seq uint32, b []byte) {
	if !s.ackEnabled() {
		return
	}

	s.ackManager.addPacket(ackPacket{
		nodeId: nodeId,
		pt:     pt,
		seq:    seq,
		b:      b,
	})
}

// remPacket2Ack 移除待确认数据包.
func (s *Service) remPacket2Ack(pt PacketType, seq uint32) {
	if !s.ackEnabled() {
		return
	}

	s.ackManager.remPacket(pt, seq)
}

// receivePacketAck 收到数据包确认.
func (s *Service) receivePacketAck(pt PacketType, seq uint32) {
	if !s.ackEnabled() {
		return
	}

	s.ackManager.receiveAck(pt, seq)
}

// sendAckPacket 发送 Ack 数据包.
func (s *Service) sendAckPacket(nodeId string, ph packetHead) error {
	if !s.ackEnabled() {
		return nil
	}

	// 不能向本地发送 Ack.
	if nodeId == s.nodeId() {
		return errors.New("send ack packet to local")
	}

	// 编码数据包.
	head := ackPacketHead{
		ackPt:  ph.getPt(),
		ackSeq: ph.getSeq(),
	}
	b, err := s.encodePacket(&head, nil)
	if err != nil {
		s.logger.ErrorFields("encode ack packet failed", lfdPacketTypeSeq(ph), lfdError(err))
		return errCodeEncodePacketFailed
	}

	// 发送数据.
	ctx, cancel := context.WithTimeout(context.Background(), s.ackManager.getCfg().Timeout)
	defer cancel()
	return s.send(ctx, nodeId, b)
}

// onAckRetry 处理 Ack 重试.
func (s *Service) onAckRetry(ap ackPacket) {
	if err := s.lockNotStopped(true); err != nil {
		return
	}
	defer s.unlockState(true)

	ctx, cancel := context.WithTimeout(context.Background(), s.ackManager.getCfg().Timeout)
	defer cancel()

	if err := s.send(ctx, ap.nodeId, ap.b); err != nil {
		s.logger.ErrorFields("retry to send packet failed", lfdRemoteNodeId(ap.nodeId), lfdPacketType(ap.pt), lfdSeq(ap.seq), lfdError(err))
	} else {
		s.logger.WarnFields("retry to send packet", lfdRemoteNodeId(ap.nodeId), lfdPacketType(ap.pt), lfdSeq(ap.seq))
	}
}

// onAckFailed 处理 Ack 失败.
func (s *Service) onAckOver(ap ackPacket, reason ackOverReason) {
	if err := s.lockNotStopped(true); err != nil {
		return
	}
	defer s.unlockState(true)

	if reason.isFailed() {
		s.logger.ErrorFields("packet to ack failed", lfdRemoteNodeId(ap.nodeId), lfdPacketType(ap.pt), lfdSeq(ap.seq))
	}
	s.putBytes(ap.b)
}
