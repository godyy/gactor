package gactor

import (
	"errors"
	"fmt"

	pkgerrors "github.com/pkg/errors"
)

// cliHandlePacketAck PacketTypeAck 处理器.
func cliHandlePacketAck(c *Client, nodeId string, b *Buffer) error {
	// 解码包头.
	var head ackPacketHead
	if err := head.decode(b); err != nil {
		return pkgerrors.WithMessage(err, "gactor: cliHandlePacketAck: decode request Head")
	}

	// 释放缓冲区.
	c.freeBuffer(b)

	// 收到数据包确认.
	c.receivePacketAck(head.ackPt, head.ackSeq)

	return nil
}

// cliHandlePacketRawResp PacketTypeRawResp 处理器.
func cliHandlePacketRawResp(c *Client, nodeId string, b *Buffer) error {
	// 解码包头.
	var head rawRespPacketHead
	if err := head.decode(b); err != nil {
		return pkgerrors.WithMessage(err, "gactor: cliHandlePacketRawResp: decode request Head")
	}

	// 发送 Ack 确认.
	if err := c.sendAckPacket(nodeId, &head); err != nil {
		c.getLogger().ErrorFields("cliHandlePacketRawResp: send ack packet failed", lfdId(head.fromId), lfdError(err))
	}

	// 发生错误.
	if !errors.Is(head.errCode, errCodeOK) {
		c.freeBuffer(b)
		c.handleResponse(ClientResponse{
			ID:  head.fromId,
			SID: head.sid,
			Err: head.errCode,
		})
		return nil
	}

	// 处理响应.
	c.handleResponse(ClientResponse{
		ID:      head.fromId,
		SID:     head.sid,
		Payload: *b,
	})

	return nil
}

// cliHandlePacketRawPush PacketTypeRawPush 处理器.
func cliHandlePacketRawPush(c *Client, nodeId string, b *Buffer) error {
	// 解码包头.
	var head rawPushPacketHead
	if err := head.decode(b); err != nil {
		return pkgerrors.WithMessage(err, "gactor: cliHandlePacketRawPush: decode request Head")
	}

	// 发送 Ack 确认.
	if err := c.sendAckPacket(nodeId, &head); err != nil {
		c.getLogger().ErrorFields("cliHandlePacketRawPush: send ack packet failed", lfdId(head.fromId), lfdError(err))
	}

	// 处理推送.
	c.handlePush(ClientPush{
		ID:      head.fromId,
		SID:     head.sid,
		Payload: *b,
	})

	return nil
}

// cliHandlePacketDisconnect PacketTypeDisconnect 处理器.
func cliHandlePacketDisconnect(c *Client, nodeId string, b *Buffer) error {
	// 解码包头.
	var head disconnectPacketHead
	if err := head.decode(b); err != nil {
		return pkgerrors.WithMessage(err, "gactor: cliHandlePacketDisconnect: decode request Head")
	}

	// 发送 Ack 确认.
	if err := c.sendAckPacket(nodeId, &head); err != nil {
		c.getLogger().ErrorFields("cliHandlePacketDisconnect: send ack packet failed", lfdId(head.id), lfdError(err))
	}

	// 释放缓冲区.
	c.freeBuffer(b)

	// 处理断开连接.
	c.handleDisconnect(head.id, head.sid)

	return nil
}

// clientPacketHandlers Client 数据包处理器.
var clientPacketHandlers = map[PacketType]func(c *Client, nodeId string, b *Buffer) error{
	PacketTypeAck:        cliHandlePacketAck,
	PacketTypeDisconnect: cliHandlePacketDisconnect,
	PacketTypeRawResp:    cliHandlePacketRawResp,
	PacketTypeRawPush:    cliHandlePacketRawPush,
}

// HandlePacket 处理网络 Packet.
// 若返回非空 error, 则 p 需要用户自行回收. 否则内部工作流会自动回收.
func (c *Client) HandlePacket(nodeId string, b []byte) error {
	// 设置缓冲区.
	var buf Buffer
	buf.SetBuf(b)

	// 读取数据包类型.
	pt, err := buf.readPacketType()
	if err != nil {
		return pkgerrors.WithMessage(err, "read packet type")
	}

	// 获取处理器.
	handler := clientPacketHandlers[pt]
	if handler == nil {
		return fmt.Errorf("packet type %d not support", pt)
	}

	if err := c.lockRunning(true); err != nil {
		return err
	}
	defer c.unlockState(true)

	return handler(c, nodeId, &buf)
}
