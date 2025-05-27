package gactor

import (
	"context"
	"errors"
	"fmt"
	"time"

	pkgerrors "github.com/pkg/errors"
)

// svcHandlePacketRawReq PacketTypeRawReq 处理器.
func svcHandlePacketRawReq(s *Service, nodeId string, p Packet) error {
	if nodeId == s.nodeId() {
		return errors.New("gactor: receive PacketTypeRawReq from local")
	}

	// 解码包头
	var head rawReqPacketHead
	if err := head.decode(p); err != nil {
		return pkgerrors.WithMessage(err, "gactor: svcHandlePacketRawReq: decode request Head")
	}

	// 检查是否已经超时.
	timeout := time.Duration(int(head.timeout)-s.getCfg().MaxRTT) * time.Millisecond
	if timeout <= 0 {
		s.putPacket(p)
		s.getLogger().WarnFields("svcHandlePacketRawReq: already timeout", lfdActorUID(head.toId))
		return nil
	}

	// 创建并发送请求.
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	request := newContext(ctx, cancel, s, newRawRequest(nodeId, head, p))
	if err := s.send2Actor(ctx, head.toId, request); err != nil {
		request.release()
		s.getLogger().ErrorFields("svcHandlePacketRawReq: send request to actor failed", lfdActorUID(head.toId), lfdError(err))

		// 编码发送错误响应数据包
		var respHead rawRespPacketHead
		respHead.fromId = head.toId
		respHead.sid = head.sid
		if !errors.As(err, &respHead.errCode) {
			respHead.errCode = errCodeInternalError
		}
		if err := s.sendPacket(ctx, nodeId, &respHead, nil); err != nil {
			s.getLogger().ErrorFields("svcHandlePacketRawReq: send errcode response to actor failed", lfdActorUID(head.toId), lfdError(err))
		}
	}

	return nil
}

// svcHandlePacketS2SRpc PacketTypeS2SRpc 处理器.
func svcHandlePacketS2SRpc(s *Service, nodeId string, p Packet) error {
	if nodeId == s.nodeId() {
		return errors.New("gactor: receive PacketTypeRawReq from local")
	}

	// 解码包头.
	var head s2sRpcPacketHead
	if err := head.decode(p); err != nil {
		return pkgerrors.WithMessage(err, "gactor: svcHandlePacketS2SRpc: decode request Head")
	}

	// 检查是否已经超时.
	timeout := time.Duration(int(head.timeout)-s.getCfg().MaxRTT) * time.Millisecond
	if timeout <= 0 {
		s.putPacket(p)
		s.getLogger().WarnFields("svcHandlePacketS2SRpc: already timeout", lfdActorUID(head.toId))
		return nil
	}

	// 创建并发送请求.
	// 检查是否已经超时.
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	request := newContext(ctx, cancel, s, newRPCRequest(nodeId, head, p))
	if err := s.send2Actor(ctx, head.toId, request); err != nil {
		request.release()
		s.getLogger().ErrorFields("svcHandlePacketS2SRpc: send request to actor failed", lfdActorUID(head.toId), lfdError(err))

		// 编码发送错误响应数据包.
		var respHead s2sRpcRespPacketHead
		respHead.reqId = head.reqId
		respHead.fromId = head.toId
		if !errors.As(err, &respHead.errCode) {
			respHead.errCode = errCodeInternalError
		}
		if err := s.sendPacket(ctx, nodeId, &respHead, nil); err != nil {
			s.getLogger().ErrorFields("svcHandlePacketS2SRpc: send errcode response to actor failed", lfdActorUID(head.toId), lfdError(err))
		}
	}

	return nil
}

// svcHandlePacketS2SRpcResp PacketTypeS2SRpcResp 处理器.
func svcHandlePacketS2SRpcResp(s *Service, _ string, p Packet) error {
	// 解码包头.
	var head s2sRpcRespPacketHead
	if err := head.decode(p); err != nil {
		return pkgerrors.WithMessage(err, "gactor: svcHandlePacketS2SRpcResp: decode request Head")
	}

	if !s.rpcManager.handleResponse(head.reqId, p, head.errCode) {
		s.getLogger().ErrorFields("svcHandlePacketS2SRpcResp: call not found", lfdReqId(head.reqId))
	}

	return nil
}

// svcHandlePacketS2SCast PacketTypeS2SCast 处理器.
func svcHandlePacketS2SCast(s *Service, _ string, p Packet) error {
	// 解码包头.
	var head s2sCastPacketHead
	if err := head.decode(p); err != nil {
		return pkgerrors.WithMessage(err, "gactor: svcHandlePacketS2SCast: decode request Head")
	}

	// 创建并发送请求.
	ctx := context.Background()
	request := newContext(ctx, nil, s, newCastRequest(head, p))
	if err := s.send2Actor(ctx, head.toId, request); err != nil {
		request.release()
		s.getLogger().ErrorFields("svcHandlePacketS2SCast: send request to actor failed", lfdActorUID(head.toId), lfdError(err))
	}

	return nil
}

// svcHandlePacketS2SDisconnected PacketTypeS2SDisconnected 处理器.
func svcHandlePacketS2SDisconnected(s *Service, nodeId string, p Packet) error {
	// 解码包头.
	var head s2sDisconnectedPacketHead
	if err := head.decode(p); err != nil {
		return pkgerrors.WithMessage(err, "gactor: svcHandlePacketS2SDisconnected: decode request Head")
	}

	s.putPacket(p)

	if err := s.send2Actor(context.Background(), head.uid, newMessageDisconnected(nodeId, head.sid)); err != nil {
		s.getLogger().ErrorFields("svcHandlePacketS2SDisconnected: send message to actor failed", lfdActorUID(head.uid), lfdError(err))
	}

	return nil
}

// serviceLocalPacketHandlers Service 本地 Packet 处理器注册.
var serviceLocalPacketHandlers = map[PacketType]func(s *Service, nodeId string, p Packet) error{
	PacketTypeS2SRpcResp: svcHandlePacketS2SRpcResp,
}

// onLocalPacket 处理本地 Packet.
func (s *Service) onLocalPacket(p Packet) error {
	pt, err := packetIOHelper.readPacketType(p)
	if err != nil {
		return err
	}

	handler := serviceLocalPacketHandlers[pt]
	if handler == nil {
		return fmt.Errorf("local packet type %d not support", pt)
	}

	s.mtxState.RLock()
	defer s.mtxState.RUnlock()
	if s.state != serviceStateStarted && s.state != serviceStateStopping {
		return serviceStateErr(s.state)
	}

	return handler(s, s.nodeId(), p)
}

// servicePacketHandlers Service Packet 处理器注册.
var servicePacketHandlers = map[PacketType]func(s *Service, nodeId string, p Packet) error{
	PacketTypeRawReq:          svcHandlePacketRawReq,
	PacketTypeS2SRpc:          svcHandlePacketS2SRpc,
	PacketTypeS2SRpcResp:      svcHandlePacketS2SRpcResp,
	PacketTypeS2SCast:         svcHandlePacketS2SCast,
	PacketTypeS2SDisconnected: svcHandlePacketS2SDisconnected,
}

// HandlePacket 处理网络 Packet.
// 若返回非空 error, 则 p 需要用户自行回收. 否则内部工作流会自动回收.
func (s *Service) HandlePacket(nodeId string, p Packet) error {
	pt, err := packetIOHelper.readPacketType(p)
	if err != nil {
		return err
	}

	handler := servicePacketHandlers[pt]
	if handler == nil {
		return fmt.Errorf("packet type %d not support", pt)
	}

	s.mtxState.RLock()
	defer s.mtxState.RUnlock()
	if s.state != serviceStateStarted && s.state != serviceStateStopping {
		return serviceStateErr(s.state)
	}

	return handler(s, nodeId, p)
}
