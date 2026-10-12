package gactor

import (
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

	// 服务必须处于运行状态, 否则忽略
	if s.checkStarted() != nil {
		return nil
	}

	// 发送消息.
	if err := s.send2LocalActor(uid, newMessageConnect(nodeId, head.sid), ""); err != nil {
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

	// 服务必须处于运行状态, 否则忽略
	if s.checkStarted() != nil {
		return nil
	}

	if err := s.send2LocalActor(uid, newMessageDisconnected(nodeId, head.sid), ""); err != nil {
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

	// 服务必须处于运行状态，否则返回停机错误.
	if s.checkStarted() != nil {
		// 编码发送错误响应数据包
		respHead := newRawRespHeadFromReq(s.genSeq(), ErrCodeServiceStopped, &head)
		_ = s.sendRemotePacket(nodeId, &respHead, nil)
		return nil
	}

	// 创建并发送请求.
	request := newContext(s, newRawRequest(nodeId, head.getSeq(), head.sid, *b, time.Now().Add(timeout).UnixMilli()))
	if err := s.send2LocalActor(uid, request, ""); err != nil {
		request.discard(s)
		s.getLogger().ErrorFields("[HandlePacketRawReq] send request to actor failed",
			lfdRemoteNodeId(nodeId), lfdSeq(head.getSeq()), s.lfdActorUID("uid", uid), lfdSid(head.sid), lfdTimeout(int64(head.timeout)), lfdError(err))

		// 编码发送错误响应数据包
		respHead := newRawRespHeadFromReq(s.genSeq(), Err2ErrCode(err), &head)
		if err := s.sendRemotePacket(nodeId, &respHead, nil); err != nil {
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

	// 服务必须处于运行状态，否则返回停机错误.
	if s.checkStarted() != nil {
		// 编码发送错误响应数据包
		respHead := newS2SRpcRespHeadFromReq(s.genSeq(), ErrCodeServiceStopped, &head)
		_ = s.sendRemotePacket(nodeId, &respHead, nil)
		return nil
	}

	// 创建并发送请求.
	request := newContext(s, newRPCRequest(nodeId, head.getSeq(), head.reqId, head.fromId, *b, time.Now().Add(timeout).UnixMilli()))
	if err := s.send2LocalActor(head.toId, request, ""); err != nil {
		request.discard(s)
		s.getLogger().ErrorFields("[HandlePacketS2SRpc] send request to actor failed",
			lfdRemoteNodeId(nodeId), lfdSeq(head.getSeq()), lfdReqId(head.reqId), s.lfdActorUID("fromId", head.fromId), s.lfdActorUID("toId", head.toId), lfdTimeout(int64(head.timeout)), lfdError(err))

		// 编码发送错误响应数据包.
		respHead := newS2SRpcRespHeadFromReq(s.genSeq(), Err2ErrCode(err), &head)
		if err := s.sendRemotePacket(nodeId, &respHead, nil); err != nil {
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

	// 若服务已完全停机, 忽略.
	if s.checkNotStopped() != nil {
		return nil
	}

	// 处理 RPC 响应
	var err error
	if head.errCode != ErrCodeOK {
		err = head.errCode
	}
	s.rpcManager.handleResponse(head.reqId, head.fromId, head.toId, b, err)

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

	// 服务必须处于运行状态，否则忽略.
	if s.checkStarted() != nil {
		return nil
	}

	// 创建并发送请求.
	request := newContext(s, newCastRequest(nodeId, head.getSeq(), head.fromId, *b))
	if err := s.send2LocalActor(head.toId, request, ""); err != nil {
		request.discard(s)
		s.getLogger().ErrorFields("[HandlePacketS2SCast] send request to actor failed",
			lfdRemoteNodeId(nodeId), lfdSeq(head.getSeq()), s.lfdActorUID("fromId", head.fromId), s.lfdActorUID("toId", head.toId), lfdError(err))
	}

	return nil
}

// svcHandlePacketS2SForward PacketTypeS2SForward 处理器.
func svcHandlePacketS2SForward(s *Service, nodeId string, b *Buffer) error {
	if nodeId == s.nodeId() {
		return errors.New("gactor: [HandlePacketS2SForward] receive from local")
	}

	var head s2sForwardPacketHead
	if err := head.decode(b); err != nil {
		return pkgerrors.WithMessage(err, "gactor: [HandlePacketS2SForward] decode request Head")
	}

	s.getLogger().DebugFields("[HandlePacketS2SForward]",
		lfdRemoteNodeId(nodeId), lfdSeq(head.getSeq()), s.lfdActorUID("fromId", head.fromId), s.lfdActorUID("toId", head.toId))

	if err := s.sendAckPacket(nodeId, &head); err != nil {
		s.getLogger().ErrorFields("[HandlePacketS2SForward] send ack packet failed",
			lfdRemoteNodeId(nodeId), lfdSeq(head.getSeq()), s.lfdActorUID("fromId", head.fromId), s.lfdActorUID("toId", head.toId), lfdError(err))
	}

	if s.checkStarted() != nil {
		s.freeBuffer(b)
		return nil
	}

	msg := newMessageForward(*b)
	if err := s.send2StartedLocalActor(head.toId, msg); err != nil {
		msg.discard(s)
		s.getLogger().ErrorFields("[HandlePacketS2SForward] send message to actor failed",
			lfdRemoteNodeId(nodeId), lfdSeq(head.getSeq()), s.lfdActorUID("fromId", head.fromId), s.lfdActorUID("toId", head.toId), lfdError(err))
		return nil
	}

	return nil
}

// svcHandlePacketS2SMigrateStart PacketTypeS2SMigrateStart 处理器.
func svcHandlePacketS2SMigrateStart(s *Service, nodeId string, b *Buffer) error {
	var head s2sMigrateStartPacketHead
	if err := head.decode(b); err != nil {
		return pkgerrors.WithMessage(err, "gactor: [HandlePacketS2SMigrateStart] decode request Head")
	}

	if err := s.sendAckPacket(nodeId, &head); err != nil {
		s.getLogger().ErrorFields("[HandlePacketS2SMigrateStart] send ack packet failed",
			lfdRemoteNodeId(nodeId), lfdSeq(head.getSeq()), s.lfdActorUID("uid", head.uid), lfdError(err))
	}
	s.freeBuffer(b)

	if s.checkStarted() != nil {
		return nil
	}
	// sourceNodeID 是迁移代际的一部分，必须和实际网络来源一致。
	if nodeId != head.sourceNodeID {
		s.denyMigrationControl()
		s.getLogger().WarnFields("[HandlePacketS2SMigrateStart] ignore mismatched source node",
			lfdRemoteNodeId(nodeId), lfdSeq(head.getSeq()), s.lfdActorUID("uid", head.uid))
		return nil
	}

	return s.startInboundMigration(head)
}

// svcHandlePacketS2SMigrateResult PacketTypeS2SMigrateResult 处理器.
func svcHandlePacketS2SMigrateResult(s *Service, nodeId string, b *Buffer) error {
	var head s2sMigrateResultPacketHead
	if err := head.decode(b); err != nil {
		return pkgerrors.WithMessage(err, "gactor: [HandlePacketS2SMigrateResult] decode request Head")
	}

	if err := s.sendAckPacket(nodeId, &head); err != nil {
		s.getLogger().ErrorFields("[HandlePacketS2SMigrateResult] send ack packet failed",
			lfdRemoteNodeId(nodeId), lfdSeq(head.getSeq()), s.lfdActorUID("uid", head.uid), lfdError(err))
	}
	s.freeBuffer(b)

	if s.checkStarted() != nil {
		return nil
	}
	// 迁移结果只能由本轮锁定目标节点返回，避免异常节点或迟到包错误推进源 Actor 状态。
	if !s.isMigrationTarget(nodeId) {
		s.denyMigrationControl()
		s.getLogger().WarnFields("[HandlePacketS2SMigrateResult] ignore unexpected source node",
			lfdRemoteNodeId(nodeId), lfdSeq(head.getSeq()), s.lfdActorUID("uid", head.uid))
		return nil
	}

	actor, err := s.refActor(head.uid)
	if err != nil || actor == nil {
		return err
	}
	defer actor.core().deref()
	return actor.core().receiveMessage(newMessageMigrationResult(head.sourceLeaseID, head.startSeq, head.errCode))
}

// svcHandlePacketS2SMigrateForward PacketTypeS2SMigrateForward 处理器.
func svcHandlePacketS2SMigrateForward(s *Service, nodeId string, b *Buffer) error {
	var head s2sMigrateForwardPacketHead
	if err := head.decode(b); err != nil {
		return pkgerrors.WithMessage(err, "gactor: [HandlePacketS2SMigrateForward] decode request Head")
	}

	if err := s.sendAckPacket(nodeId, &head); err != nil {
		s.getLogger().ErrorFields("[HandlePacketS2SMigrateForward] send ack packet failed",
			lfdRemoteNodeId(nodeId), lfdSeq(head.getSeq()), s.lfdActorUID("uid", head.uid), lfdError(err))
	}

	if s.checkStarted() != nil {
		s.freeBuffer(b)
		return nil
	}
	if !s.acceptsMigrationLease(head.uid, nodeId, head.sourceLeaseID) {
		s.denyMigrationControl()
		s.freeBuffer(b)
		return nil
	}

	return s.handleMigrateForwardPacket(head, b)
}

func (s *Service) handleMigrateForwardPacket(head s2sMigrateForwardPacketHead, b *Buffer) error {
	var inner Buffer
	inner.SetBuf(append([]byte(nil), b.UnreadData()...))
	s.freeBuffer(b)

	pt, err := inner.readPacketType()
	if err != nil {
		s.freeBuffer(&inner)
		return err
	}
	if pt != head.innerPacketType {
		s.freeBuffer(&inner)
		return errors.New("gactor: migrate forward inner packet type mismatch")
	}

	// 先把迁移转发包恢复为本地消息，再统一投递到目标 Actor，
	// 避免每个分支都重复 send2LocalActor / discard 逻辑。
	msg, err := s.buildMigrateForwardMessage(head, &inner)
	if err != nil {
		return err
	}
	if msg == nil {
		return nil
	}
	if err := s.send2LocalActor(head.uid, msg, ""); err != nil {
		msg.discard(s)
		return err
	}
	return nil
}

// buildMigrateForwardMessage 将迁移转发包恢复为本地可投递的消息。
// 成功返回的 message 会接管 inner 的生命周期；返回 nil 表示该消息可直接忽略。
func (s *Service) buildMigrateForwardMessage(head s2sMigrateForwardPacketHead, inner *Buffer) (message, error) {
	switch head.innerPacketType {
	case PacketTypeRawReq:
		var innerHead rawReqPacketHead
		if err := innerHead.decode(inner); err != nil {
			s.freeBuffer(inner)
			return nil, err
		}
		timeout := time.Duration(int(innerHead.timeout)-s.getCfg().MaxRTT) * time.Millisecond
		if timeout <= 0 {
			s.freeBuffer(inner)
			return nil, nil
		}
		req := newContext(s, newRawRequest(head.originNodeID, innerHead.seq, innerHead.sid, *inner, time.Now().Add(timeout).UnixMilli()))
		return req, nil
	case PacketTypeS2SRpc:
		var innerHead s2sRpcPacketHead
		if err := innerHead.decode(inner); err != nil {
			s.freeBuffer(inner)
			return nil, err
		}
		timeout := time.Duration(int(innerHead.timeout)-s.getCfg().MaxRTT) * time.Millisecond
		if timeout <= 0 {
			s.freeBuffer(inner)
			return nil, nil
		}
		req := newContext(s, newRPCRequest(head.originNodeID, innerHead.seq, innerHead.reqId, innerHead.fromId, *inner, time.Now().Add(timeout).UnixMilli()))
		return req, nil
	case PacketTypeS2SCast:
		var innerHead s2sCastPacketHead
		if err := innerHead.decode(inner); err != nil {
			s.freeBuffer(inner)
			return nil, err
		}
		req := newContext(s, newCastRequest(head.originNodeID, innerHead.seq, innerHead.fromId, *inner))
		return req, nil
	case PacketTypeConnect:
		var innerHead connectPacketHead
		if err := innerHead.decode(inner); err != nil {
			s.freeBuffer(inner)
			return nil, err
		}
		s.freeBuffer(inner)
		return newMessageConnect(head.originNodeID, innerHead.sid), nil
	case PacketTypeDisconnect:
		var innerHead disconnectPacketHead
		if err := innerHead.decode(inner); err != nil {
			s.freeBuffer(inner)
			return nil, err
		}
		s.freeBuffer(inner)
		return newMessageDisconnected(head.originNodeID, innerHead.sid), nil
	default:
		s.freeBuffer(inner)
		return nil, errors.New("gactor: unsupported migrate inner packet type")
	}
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

	return handler(s, s.nodeId(), &buf)
}

// servicePacketHandlers Service Packet 处理器注册.
var servicePacketHandlers = map[PacketType]func(s *Service, nodeId string, b *Buffer) error{
	PacketTypeAck:               svcHandlePacketAck,
	PacketTypeConnect:           svcHandlePacketConnect,
	PacketTypeDisconnect:        svcHandlePacketDisconnect,
	PacketTypeRawReq:            svcHandlePacketRawReq,
	PacketTypeS2SRpc:            svcHandlePacketS2SRpc,
	PacketTypeS2SRpcResp:        svcHandlePacketS2SRpcResp,
	PacketTypeS2SCast:           svcHandlePacketS2SCast,
	PacketTypeS2SForward:        svcHandlePacketS2SForward,
	PacketTypeS2SMigrateStart:   svcHandlePacketS2SMigrateStart,
	PacketTypeS2SMigrateResult:  svcHandlePacketS2SMigrateResult,
	PacketTypeS2SMigrateForward: svcHandlePacketS2SMigrateForward,
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

	return handler(s, nodeId, &buf)
}
