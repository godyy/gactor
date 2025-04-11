package gactor

import (
	"errors"
	"fmt"

	pkgerrors "github.com/pkg/errors"
)

// cliHandlePacketRawResp PacketTypeRawResp 处理器.
func cliHandlePacketRawResp(c *Client, nodeId string, p Packet) error {
	// 解码包头.
	var head rawRespPacketHead
	if err := head.decode(p); err != nil {
		return pkgerrors.WithMessage(err, "gactor: cliHandlePacketRawResp: decode request Head")
	}

	if !errors.Is(head.errCode, errCodeOK) {
		c.putPacket(p)
		c.handleResponse(ClientResponse{
			UID: head.fromId,
			SID: head.sid,
			Err: head.errCode,
		})
		return nil
	}

	c.handleResponse(ClientResponse{
		UID:     head.fromId,
		SID:     head.sid,
		Payload: p,
	})

	return nil
}

// cliHandlePacketRawPush PacketTypeRawPush 处理器.
func cliHandlePacketRawPush(c *Client, nodeId string, p Packet) error {
	// 解码包头.
	var head rawPushPacketHead
	if err := head.decode(p); err != nil {
		return pkgerrors.WithMessage(err, "gactor: cliHandlePacketRawPush: decode request Head")
	}

	c.handlePush(ClientPush{
		UID:     head.fromId,
		SID:     head.sid,
		Payload: p,
	})

	return nil
}

// cliHandlePacketS2SDisconnect PacketTypeS2SDisconnected 处理器.
func cliHandlePacketS2SDisconnect(c *Client, nodeId string, p Packet) error {
	// 解码包头.
	var head s2sDisconnectedPacketHead
	if err := head.decode(p); err != nil {
		return pkgerrors.WithMessage(err, "gactor: cliHandlePacketS2SDisconnect: decode request Head")
	}

	c.putPacket(p)

	c.handleDisconnect(head.uid, head.sid)

	return nil
}

// clientPacketHandlers Client 数据包处理器.
var clientPacketHandlers = map[PacketType]func(c *Client, nodeId string, p Packet) error{
	PacketTypeRawResp:         cliHandlePacketRawResp,
	PacketTypeRawPush:         cliHandlePacketRawPush,
	PacketTypeS2SDisconnected: cliHandlePacketS2SDisconnect,
}

// HandlePacket 处理网络 Packet.
// 若返回非空 error, 则 p 需要用户自行回收. 否则内部工作流会自动回收.
func (c *Client) HandlePacket(nodeId string, p Packet) error {
	pt, err := packetIOHelper.readPacketType(p)
	if err != nil {
		return err
	}

	handler := clientPacketHandlers[pt]
	if handler == nil {
		return fmt.Errorf("packet type %d not support", pt)
	}

	return handler(c, nodeId, p)
}
