package gactor

import (
	"context"
	"errors"
)

// startAckManager 启动Ack管理器.
func (c *Client) startAckManager() {
	if c.ackManager != nil {
		c.ackManager.start()
	}
}

// ackEnabled 是否启用 ACK 机制.
func (c *Client) ackEnabled() bool {
	return c.ackManager != nil
}

// addPacket2Ack 添加待确认数据包.
func (c *Client) addPacket2Ack(nodeId string, pt PacketType, seq uint32, b []byte) {
	if !c.ackEnabled() {
		return
	}

	c.ackManager.addPacket(ackPacket{
		nodeId: nodeId,
		pt:     pt,
		seq:    seq,
		b:      b,
	})
}

// remPacket2Ack 移除待确认数据包.
func (c *Client) remPacket2Ack(pt PacketType, seq uint32) {
	if !c.ackEnabled() {
		return
	}

	c.ackManager.remPacket(pt, seq)
}

// receivePacketAck 收到数据包确认.
func (c *Client) receivePacketAck(pt PacketType, seq uint32) {
	if !c.ackEnabled() {
		return
	}

	c.ackManager.receiveAck(pt, seq)
}

// sendAckPacket 发送 Ack 数据包.
func (c *Client) sendAckPacket(nodeId string, ph packetHead) error {
	if !c.ackEnabled() {
		return nil
	}

	// 不能向本地发送 Ack.
	if nodeId == c.nodeId() {
		return errors.New("send ack packet to local")
	}

	// 编码数据包.
	head := ackPacketHead{
		ackPt:  ph.pt(),
		ackSeq: ph.seq(),
	}
	b, err := c.encodePacket(&head, nil)
	if err != nil {
		c.logger.ErrorFields("encode ack packet failed", lfdPacketTypeSeq(ph), lfdError(err))
		return errCodeEncodePacketFailed
	}

	// 发送数据.
	ctx, cancel := context.WithTimeout(context.Background(), c.ackManager.getCfg().Timeout)
	defer cancel()
	return c.send(ctx, nodeId, b)
}

// onAckRetry 处理 ACK 重试.
func (c *Client) onAckRetry(ap ackPacket) {
	if err := c.lockRunning(true); err != nil {
		return
	}
	defer c.unlockState(true)

	ctx, cancel := context.WithTimeout(context.Background(), c.ackManager.getCfg().Timeout)
	defer cancel()

	if err := c.send(ctx, ap.nodeId, ap.b); err != nil {
		c.logger.ErrorFields("retry to send packet failed", lfdRemoteNodeId(ap.nodeId), lfdPacketType(ap.pt), lfdPacketSeq(ap.seq), lfdError(err))
	} else {
		c.logger.WarnFields("retry to send packet", lfdRemoteNodeId(ap.nodeId), lfdPacketType(ap.pt), lfdPacketSeq(ap.seq))
	}
}

// onAckOver 处理 ACK 结束.
func (c *Client) onAckOver(ap ackPacket, reason ackOverReason) {
	if err := c.lockRunning(true); err != nil {
		return
	}
	defer c.unlockState(true)

	if reason.isFailed() {
		c.logger.ErrorFields("packet to ack failed", lfdRemoteNodeId(ap.nodeId), lfdPacketType(ap.pt), lfdPacketSeq(ap.seq))
	}
	c.putBytes(ap.b)
}
