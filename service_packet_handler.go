package gactor

import (
	"context"
	"errors"
	"fmt"
	"time"

	pkgerrors "github.com/pkg/errors"
)

// svcHandlePacketAck PacketTypeAck 处理器.
func svcHandlePacketAck(s *Service, nodeId string, b *Buffer) error {
	// 解码包头.
	var head ackPacketHead
	if err := head.decode(b); err != nil {
		return pkgerrors.WithMessage(err, "gactor: [HandlePacketAck] decode request Head")
	}

	s.getLogger().DebugFields("[HandlePacketAck]",
		lfdRemoteNodeId(nodeId),
		lfdPacketType(head.ackPt),
		lfdSeq(head.ackSeq),
	)

	// 释放缓冲区.
	s.freeBuffer(b)

	// 收到数据包确认.
	s.receivePacketAck(head.ackPt, head.ackSeq)

	return nil
}

// svcHandlePacketConnect PacketTypeConnect 处理器.
func svcHandlePacketConnect(s *Service, nodeId string, b *Buffer) error {
	// 解码包头.
	var head connectPacketHead
	if err := head.decode(b); err != nil {
		return pkgerrors.WithMessage(err, "gactor: [HandlePacketConnect] decode request Head")
	}

	uid := s.makeClientActorUID(head.id)

	s.getLogger().DebugFields("[HandlePacketConnect]",
		lfdRemoteNodeId(nodeId),
		lfdSeq(head.getSeq()),
		lfdId(head.id),
		lfdSid(head.sid),
	)

	// 回复确认数据包.
	if err := s.sendAckPacket(nodeId, &head); err != nil {
		s.getLogger().ErrorFields("[HandlePacketConnect] send ack packet failed",
			lfdRemoteNodeId(nodeId),
			lfdSeq(head.getSeq()),
			lfdId(head.id),
			lfdSid(head.sid),
			lfdError(err))
	}

	// 释放缓冲区.
	s.freeBuffer(b)

	// 发送消息.
	if err := s.send2LocalActor(context.Background(), uid, newMessageConnect(nodeId, head.sid), true); err != nil {
		s.getLogger().ErrorFields("[HandlePacketConnect] send message to actor failed",
			lfdRemoteNodeId(nodeId),
			lfdSeq(head.getSeq()),
			lfdId(head.id),
			lfdSid(head.sid),
			lfdError(err))
	}

	return nil
}

// svcHandlePacketDisconnect PacketTypeDisconnect 处理器.
func svcHandlePacketDisconnect(s *Service, nodeId string, b *Buffer) error {
	// 解码包头.
	var head disconnectPacketHead
	if err := head.decode(b); err != nil {
		return pkgerrors.WithMessage(err, "gactor: [HandlePacketDisconnect] decode request Head")
	}

	uid := s.makeClientActorUID(head.id)

	s.getLogger().DebugFields("[HandlePacketDisconnect]",
		lfdRemoteNodeId(nodeId),
		lfdSeq(head.getSeq()),
		lfdId(head.id),
		lfdSid(head.sid),
	)

	// 回复确认数据包.
	if err := s.sendAckPacket(nodeId, &head); err != nil {
		s.getLogger().ErrorFields("[HandlePacketDisconnect] send ack packet failed",
			lfdRemoteNodeId(nodeId),
			lfdSeq(head.getSeq()),
			lfdId(head.id),
			lfdSid(head.sid),
			lfdError(err))

	}

	s.freeBuffer(b)

	if err := s.send2LocalActor(context.Background(), uid, newMessageDisconnected(nodeId, head.sid), true); err != nil {
		s.getLogger().ErrorFields("[HandlePacketDisconnect] send message to actor failed",
			lfdRemoteNodeId(nodeId),
			lfdSeq(head.getSeq()),
			lfdId(head.id),
			lfdSid(head.sid),
			lfdError(err))
	}

	return nil
}

// svcHandlePacketRawReq PacketTypeRawReq 处理器.
func svcHandlePacketRawReq(s *Service, nodeId string, b *Buffer) error {
	if nodeId == s.nodeId() {
		return errors.New("gactor: [HandlePacketRawReq] receive from local")
	}

	// 解码包头
	var head rawReqPacketHead
	if err := head.decode(b); err != nil {
		return pkgerrors.WithMessage(err, "gactor: [HandlePacketRawReq] decode request Head")
	}

	uid := s.makeClientActorUID(head.toId)

	s.getLogger().DebugFields("[HandlePacketRawReq]",
		lfdRemoteNodeId(nodeId), lfdSeq(head.getSeq()), s.lfdActorUID("uid", uid), lfdSid(head.sid), lfdTimeout(int64(head.timeout)))

	// 回复确认数据包.
	if err := s.sendAckPacket(nodeId, &head); err != nil {
		s.getLogger().ErrorFields("[HandlePacketRawReq] send ack packet failed",
			lfdRemoteNodeId(nodeId), lfdSeq(head.getSeq()), s.lfdActorUID("uid", uid), lfdSid(head.sid), lfdTimeout(int64(head.timeout)), lfdError(err))
	}

	// 检查是否已经超时.
	timeout := time.Duration(int(head.timeout)-s.getCfg().MaxRTT) * time.Millisecond
	if timeout <= 0 {
		s.freeBuffer(b)
		s.getLogger().WarnFields("[HandlePacketRawReq] already timeout",
			lfdRemoteNodeId(nodeId), lfdSeq(head.getSeq()), s.lfdActorUID("uid", uid), lfdSid(head.sid), lfdTimeout(int64(head.timeout)))
		return nil
	}

	// 创建并发送请求.
	deadline := time.Now().Add(timeout)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	request := newContext(s, newRawRequest(nodeId, head.getSeq(), head.sid, *b, deadline.UnixMilli()))
	if err := s.send2LocalActor(ctx, uid, request, true); err != nil {
		request.release()
		s.getLogger().ErrorFields("[HandlePacketRawReq] send request to actor failed",
			lfdRemoteNodeId(nodeId), lfdSeq(head.getSeq()), s.lfdActorUID("uid", uid), lfdSid(head.sid), lfdTimeout(int64(head.timeout)), lfdError(err))

		// 编码发送错误响应数据包
		var respHead rawRespPacketHead
		respHead.seq = s.genSeq()
		respHead.fromId = head.toId
		respHead.sid = head.sid
		if !errors.As(err, &respHead.errCode) {
			respHead.errCode = errCodeInternalError
		}
		if err := s.sendRemotePacket(ctx, nodeId, &respHead, nil); err != nil {
			s.getLogger().ErrorFields("[HandlePacketRawReq] send errcode response to actor failed",
				lfdRemoteNodeId(nodeId), lfdSeq(head.getSeq()), s.lfdActorUID("uid", uid), lfdSid(head.sid), lfdTimeout(int64(head.timeout)), lfdError(err))
		}
	}

	return nil
}

// svcHandlePacketS2SRpc PacketTypeS2SRpc 处理器.
func svcHandlePacketS2SRpc(s *Service, nodeId string, b *Buffer) error {
	if nodeId == s.nodeId() {
		return errors.New("gactor: [HandlePacketS2SRpc] receive from local")
	}

	// 解码包头.
	var head s2sRpcPacketHead
	if err := head.decode(b); err != nil {
		return pkgerrors.WithMessage(err, "gactor: [HandlePacketS2SRpc] decode request Head")
	}

	s.getLogger().DebugFields("[HandlePacketS2SRpc]",
		lfdRemoteNodeId(nodeId), lfdSeq(head.getSeq()), lfdReqId(head.reqId), s.lfdActorUID("fromId", head.fromId), s.lfdActorUID("toId", head.toId), lfdTimeout(int64(head.timeout)))

	// 回复确认数据包.
	if err := s.sendAckPacket(nodeId, &head); err != nil {
		s.getLogger().ErrorFields("[HandlePacketS2SRpc] send ack packet failed",
			lfdRemoteNodeId(nodeId), lfdSeq(head.getSeq()), lfdReqId(head.reqId), s.lfdActorUID("fromId", head.fromId), s.lfdActorUID("toId", head.toId), lfdTimeout(int64(head.timeout)), lfdError(err))
	}

	// 检查是否已经超时.
	timeout := time.Duration(int(head.timeout)-s.getCfg().MaxRTT) * time.Millisecond
	if timeout <= 0 {
		s.freeBuffer(b)
		s.getLogger().WarnFields("[HandlePacketS2SRpc] already timeout",
			lfdRemoteNodeId(nodeId), lfdSeq(head.getSeq()), lfdReqId(head.reqId), s.lfdActorUID("fromId", head.fromId), s.lfdActorUID("toId", head.toId), lfdTimeout(int64(head.timeout)))
		return nil
	}

	// 创建并发送请求.
	// 检查是否已经超时.
	deadline := time.Now().Add(timeout)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	request := newContext(s, newRPCRequest(nodeId, head.getSeq(), head.reqId, head.fromId, *b, deadline.UnixMilli()))
	if err := s.send2LocalActor(ctx, head.toId, request, true); err != nil {
		request.release()
		s.getLogger().ErrorFields("[HandlePacketS2SRpc] send request to actor failed",
			lfdRemoteNodeId(nodeId), lfdSeq(head.getSeq()), lfdReqId(head.reqId), s.lfdActorUID("fromId", head.fromId), s.lfdActorUID("toId", head.toId), lfdTimeout(int64(head.timeout)), lfdError(err))

		// 编码发送错误响应数据包.
		var respHead s2sRpcRespPacketHead
		respHead.seq = s.genSeq()
		respHead.reqId = head.reqId
		respHead.fromId = head.toId
		respHead.toId = head.fromId
		if !errors.As(err, &respHead.errCode) {
			respHead.errCode = errCodeInternalError
		}
		if err := s.sendRemotePacket(ctx, nodeId, &respHead, nil); err != nil {
			s.getLogger().ErrorFields("[HandlePacketS2SRpc] send errcode response to actor failed",
				lfdRemoteNodeId(nodeId), lfdSeq(head.getSeq()), lfdReqId(head.reqId), s.lfdActorUID("fromId", head.fromId), s.lfdActorUID("toId", head.toId), lfdTimeout(int64(head.timeout)), lfdError(err))
		}
	}

	return nil
}

// svcHandlePacketS2SRpcResp PacketTypeS2SRpcResp 处理器.
func svcHandlePacketS2SRpcResp(s *Service, nodeId string, b *Buffer) error {
	// 解码包头.
	var head s2sRpcRespPacketHead
	if err := head.decode(b); err != nil {
		return pkgerrors.WithMessage(err, "gactor: [HandlePacketS2SRpcResp] decode request Head")
	}

	s.getLogger().DebugFields("[HandlePacketS2SRpcResp]",
		lfdRemoteNodeId(nodeId), lfdSeq(head.getSeq()), lfdReqId(head.reqId), s.lfdActorUID("fromId", head.fromId), s.lfdActorUID("toId", head.toId), lfdErrCode(head.errCode))

	// 远程调用回复, 回复确认数据包.
	if nodeId != s.nodeId() {
		if err := s.sendAckPacket(nodeId, &head); err != nil {
			s.getLogger().ErrorFields("[HandlePacketS2SRpcResp] send ack packet failed",
				lfdRemoteNodeId(nodeId), lfdSeq(head.getSeq()), lfdReqId(head.reqId), s.lfdActorUID("fromId", head.fromId), s.lfdActorUID("toId", head.toId), lfdErrCode(head.errCode), lfdError(err))
		}
	}

	s.rpcManager.handleResponse(head.reqId, head.fromId, head.toId, b, head.errCode)

	return nil
}

// svcHandlePacketS2SCast PacketTypeS2SCast 处理器.
func svcHandlePacketS2SCast(s *Service, nodeId string, b *Buffer) error {
	// 解码包头.
	var head s2sCastPacketHead
	if err := head.decode(b); err != nil {
		return pkgerrors.WithMessage(err, "gactor: [HandlePacketS2SCast] decode request Head")
	}

	s.getLogger().DebugFields("[HandlePacketS2SCast]",
		lfdRemoteNodeId(nodeId), lfdSeq(head.getSeq()), s.lfdActorUID("fromId", head.fromId), s.lfdActorUID("toId", head.toId))

	// 回复确认数据包.
	if err := s.sendAckPacket(nodeId, &head); err != nil {
		s.getLogger().ErrorFields("[HandlePacketS2SCast] send ack packet failed",
			lfdRemoteNodeId(nodeId), lfdSeq(head.getSeq()), s.lfdActorUID("fromId", head.fromId), s.lfdActorUID("toId", head.toId), lfdError(err))
	}

	// 创建并发送请求.
	ctx := context.Background()
	request := newContext(s, newCastRequest(nodeId, head.getSeq(), head.fromId, *b))
	if err := s.send2LocalActor(ctx, head.toId, request, true); err != nil {
		request.release()
		s.getLogger().ErrorFields("[HandlePacketS2SCast] send request to actor failed",
			lfdRemoteNodeId(nodeId), lfdSeq(head.getSeq()), s.lfdActorUID("fromId", head.fromId), s.lfdActorUID("toId", head.toId), lfdError(err))
	}

	return nil
}

// serviceLocalPacketHandlers Service 本地 Packet 处理器注册.
var serviceLocalPacketHandlers = map[PacketType]func(s *Service, nodeId string, b *Buffer) error{
	PacketTypeS2SRpcResp: svcHandlePacketS2SRpcResp,
}

// onLocalPacket 处理字节切片 b 指代的本地网络数据包.
func (s *Service) onLocalPacket(b []byte) error {
	// 设置缓冲区.
	var buf Buffer
	buf.SetBuf(b)

	// 读取数据包类型.
	pt, err := buf.readPacketType()
	if err != nil {
		return pkgerrors.WithMessage(err, "read packet type")
	}

	// 获取处理器.
	handler := serviceLocalPacketHandlers[pt]
	if handler == nil {
		return fmt.Errorf("local packet type %d not support", pt)
	}

	s.mtxState.RLock()
	defer s.mtxState.RUnlock()
	if s.state != serviceStateStarted && s.state != serviceStateStopping {
		return serviceStateErr(s.state)
	}

	return handler(s, s.nodeId(), &buf)
}

// servicePacketHandlers Service Packet 处理器注册.
var servicePacketHandlers = map[PacketType]func(s *Service, nodeId string, b *Buffer) error{
	PacketTypeAck:        svcHandlePacketAck,
	PacketTypeConnect:    svcHandlePacketConnect,
	PacketTypeDisconnect: svcHandlePacketDisconnect,
	PacketTypeRawReq:     svcHandlePacketRawReq,
	PacketTypeS2SRpc:     svcHandlePacketS2SRpc,
	PacketTypeS2SRpcResp: svcHandlePacketS2SRpcResp,
	PacketTypeS2SCast:    svcHandlePacketS2SCast,
}

// HandlePacket 处理字节切片 b 指代的网络数据包.
// 若返回非空 error, 则 b 需要用户自行回收. 否则内部工作流会自动回收.
func (s *Service) HandlePacket(nodeId string, b []byte) error {
	// 设置缓冲区.
	var buf Buffer
	buf.SetBuf(b)

	// 读取数据包类型.
	pt, err := buf.readPacketType()
	if err != nil {
		return pkgerrors.WithMessage(err, "read packet type")
	}

	// 获取处理器.
	handler := servicePacketHandlers[pt]
	if handler == nil {
		return fmt.Errorf("packet type %d not support", pt)
	}

	s.mtxState.RLock()
	defer s.mtxState.RUnlock()
	if s.state != serviceStateStarted && s.state != serviceStateStopping {
		return serviceStateErr(s.state)
	}

	return handler(s, nodeId, &buf)
}
