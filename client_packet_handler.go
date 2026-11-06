package gactor

import (
	"fmt"

	pkgerrors "github.com/pkg/errors"
)

// cliHandlePacketAck PacketTypeAck 处理器.
func cliHandlePacketAck(c *Client, params packetHandlerParams) error {
	head := params.head.(*ackPacketHead)

	c.getLogger().DebugFields("[HandlePacketAck]",
		lfdRemoteNodeId(params.nodeId), lfdPacketType(head.ackPt), lfdSeq(head.ackSeq))

	// 收到数据包确认.
	c.receivePacketAck(head.ackPt, head.ackSeq)

	return nil
}

// cliHandlePacketRawResp PacketTypeRawResp 处理器.
func cliHandlePacketRawResp(c *Client, params packetHandlerParams) error {
	head := params.head.(*rawRespPacketHead)

	c.getLogger().DebugFields("[HandlePacketRawResp]",
		lfdRemoteNodeId(params.nodeId), lfdSeq(head.getSeq()), lfdId(head.fromId), lfdErrCode(head.errCode))

	// 发送 Ack 确认.
	if err := c.sendAckPacket(params.nodeId, head); err != nil {
		c.getLogger().ErrorFields("[HandlePacketRawResp] send ack packet failed",
			lfdRemoteNodeId(params.nodeId), lfdSeq(head.getSeq()), lfdId(head.fromId), lfdErrCode(head.errCode), lfdError(err))
	}

	// 处理响应.
	resp := ClientResponse{
		ID:      head.fromId,
		SID:     head.sid,
		ErrCode: head.errCode,
	}
	if head.errCode == ErrCodeOK {
		resp.Payload = params.buf
	}
	c.handleResponse(resp)

	return nil
}

// cliHandlePacketRawPush PacketTypeRawPush 处理器.
func cliHandlePacketRawPush(c *Client, params packetHandlerParams) error {
	head := params.head.(*rawPushPacketHead)

	c.getLogger().DebugFields("[HandlePacketRawPush]",
		lfdRemoteNodeId(params.nodeId), lfdSeq(head.getSeq()), lfdId(head.fromId))

	// 发送 Ack 确认.
	if err := c.sendAckPacket(params.nodeId, head); err != nil {
		c.getLogger().ErrorFields("[HandlePacketRawPush] send ack packet failed",
			lfdRemoteNodeId(params.nodeId), lfdSeq(head.getSeq()), lfdId(head.fromId), lfdError(err))
	}

	// 处理推送.
	c.handlePush(ClientPush{
		ID:      head.fromId,
		SID:     head.sid,
		Payload: params.buf,
	})

	return nil
}

// cliHandlePacketDisconnect PacketTypeDisconnect 处理器.
func cliHandlePacketDisconnect(c *Client, params packetHandlerParams) error {
	head := params.head.(*disconnectPacketHead)

	c.getLogger().DebugFields("[HandlePacketDisconnect]",
		lfdRemoteNodeId(params.nodeId), lfdSeq(head.getSeq()), lfdId(head.id), lfdSid(head.sid))

	// 发送 Ack 确认.
	if err := c.sendAckPacket(params.nodeId, head); err != nil {
		c.getLogger().ErrorFields("[HandlePacketDisconnect] send ack packet failed",
			lfdRemoteNodeId(params.nodeId), lfdSeq(head.getSeq()), lfdId(head.id), lfdSid(head.sid), lfdError(err))
	}

	// 处理断开连接.
	c.handleDisconnect(head.id, head.sid)

	return nil
}

// clientPacketHandlers Client 数据包处理器.
var clientPacketHandlers = map[PacketType]func(c *Client, params packetHandlerParams) error{
	PacketTypeAck:        cliHandlePacketAck,
	PacketTypeDisconnect: cliHandlePacketDisconnect,
	PacketTypeRawResp:    cliHandlePacketRawResp,
	PacketTypeRawPush:    cliHandlePacketRawPush,
}

// HandlePacket 处理网络 Packet.
// 若返回非空 error, 则 p 需要用户自行回收. 否则内部工作流会自动回收.
func (c *Client) HandlePacket(nodeId string, b []byte) error {
	// 设置缓冲区.
	buf := NewBuffer(b)
	defer c.freeBuffer(buf)

	// 解码包头.
	pt, err := buf.ReadPacketType()
	if err != nil {
		return pkgerrors.WithMessage(err, "read packet type")
	}
	ph := createPacketByType(pt)
	if ph == nil {
		return fmt.Errorf("packet type %d not support", pt)
	}
	defer ph.release()
	if err := ph.decode(buf); err != nil {
		return pkgerrors.WithMessagef(err, "decode packet head of type %d", pt)
	}

	// 获取处理器.
	handler := clientPacketHandlers[pt]
	if handler == nil {
		return fmt.Errorf("packet type %d handler not found", pt)
	}

	if err := c.lockRunning(true); err != nil {
		return err
	}
	defer c.unlockState(true)

	return handler(c, packetHandlerParams{nodeId: nodeId, head: ph, buf: buf})
}
