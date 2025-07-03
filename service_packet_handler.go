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
		return pkgerrors.WithMessage(err, "gactor: svcHandlePacketAck: decode request Head")
	}

	// 释放缓冲区.
	s.freeBuffer(b)

	// 移除待确认数据包.
	s.remPacket2Ack(head.ackPt, head.ackSeq)

	return nil
}

// svcHandlePacketRawReq PacketTypeRawReq 处理器.
func svcHandlePacketRawReq(s *Service, nodeId string, b *Buffer) error {
	if nodeId == s.nodeId() {
		return errors.New("gactor: receive PacketTypeRawReq from local")
	}

	// 解码包头
	var head rawReqPacketHead
	if err := head.decode(b); err != nil {
		return pkgerrors.WithMessage(err, "gactor: svcHandlePacketRawReq: decode request Head")
	}

	// 回复确认数据包.
	if err := s.sendAckPacket(nodeId, &head); err != nil {
		s.getLogger().ErrorFields("svcHandlePacketRawReq: send ack packet failed", lfdActorUID(head.toId), lfdError(err))
	}

	// 检查是否已经超时.
	timeout := time.Duration(int(head.timeout)-s.getCfg().MaxRTT) * time.Millisecond
	if timeout <= 0 {
		s.freeBuffer(b)
		s.getLogger().WarnFields("svcHandlePacketRawReq: already timeout", lfdActorUID(head.toId))
		return nil
	}

	// 创建并发送请求.
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	request := newContext(ctx, cancel, s, newRawRequest(nodeId, head, *b))
	if err := s.send2Actor(ctx, head.toId, request); err != nil {
		request.release()
		s.getLogger().ErrorFields("svcHandlePacketRawReq: send request to actor failed", lfdActorUID(head.toId), lfdError(err))

		// 编码发送错误响应数据包
		var respHead rawRespPacketHead
		respHead.seq_ = s.genSeq()
		respHead.fromId = head.toId
		respHead.sid = head.sid
		if !errors.As(err, &respHead.errCode) {
			respHead.errCode = errCodeInternalError
		}
		if err := s.sendRemotePacket(ctx, nodeId, &respHead, nil); err != nil {
			s.getLogger().ErrorFields("svcHandlePacketRawReq: send errcode response to actor failed", lfdActorUID(head.toId), lfdError(err))
		}
	}

	return nil
}

// svcHandlePacketS2SRpc PacketTypeS2SRpc 处理器.
func svcHandlePacketS2SRpc(s *Service, nodeId string, b *Buffer) error {
	if nodeId == s.nodeId() {
		return errors.New("gactor: receive PacketTypeS2SRpc from local")
	}

	// 解码包头.
	var head s2sRpcPacketHead
	if err := head.decode(b); err != nil {
		return pkgerrors.WithMessage(err, "gactor: svcHandlePacketS2SRpc: decode request Head")
	}

	// 回复确认数据包.
	if err := s.sendAckPacket(nodeId, &head); err != nil {
		s.getLogger().ErrorFields("svcHandlePacketS2SRpc: send ack packet failed", lfdActorUID(head.toId), lfdError(err))
	}

	// 检查是否已经超时.
	timeout := time.Duration(int(head.timeout)-s.getCfg().MaxRTT) * time.Millisecond
	if timeout <= 0 {
		s.freeBuffer(b)
		s.getLogger().WarnFields("svcHandlePacketS2SRpc: already timeout", lfdActorUID(head.toId))
		return nil
	}

	// 创建并发送请求.
	// 检查是否已经超时.
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	request := newContext(ctx, cancel, s, newRPCRequest(nodeId, head, *b))
	if err := s.send2Actor(ctx, head.toId, request); err != nil {
		request.release()
		s.getLogger().ErrorFields("svcHandlePacketS2SRpc: send request to actor failed", lfdActorUID(head.toId), lfdError(err))

		// 编码发送错误响应数据包.
		var respHead s2sRpcRespPacketHead
		respHead.seq_ = s.genSeq()
		respHead.reqId = head.reqId
		respHead.fromId = head.toId
		if !errors.As(err, &respHead.errCode) {
			respHead.errCode = errCodeInternalError
		}
		if err := s.sendRemotePacket(ctx, nodeId, &respHead, nil); err != nil {
			s.getLogger().ErrorFields("svcHandlePacketS2SRpc: send errcode response to actor failed", lfdActorUID(head.toId), lfdError(err))
		}
	}

	return nil
}

// svcHandlePacketS2SRpcResp PacketTypeS2SRpcResp 处理器.
func svcHandlePacketS2SRpcResp(s *Service, nodeId string, b *Buffer) error {
	// 解码包头.
	var head s2sRpcRespPacketHead
	if err := head.decode(b); err != nil {
		return pkgerrors.WithMessage(err, "gactor: svcHandlePacketS2SRpcResp: decode request Head")
	}

	// 远程调用回复, 回复确认数据包.
	if nodeId != s.nodeId() {
		if err := s.sendAckPacket(nodeId, &head); err != nil {
			s.getLogger().ErrorFields("svcHandlePacketS2SRpcResp: send ack packet failed", lfdActorUID(head.fromId), lfdError(err))
		}
	}

	if !s.rpcManager.handleResponse(head.reqId, b, head.errCode) {
		s.getLogger().ErrorFields("svcHandlePacketS2SRpcResp: call not found", lfdReqId(head.reqId))
	}

	return nil
}

// svcHandlePacketS2SCast PacketTypeS2SCast 处理器.
func svcHandlePacketS2SCast(s *Service, nodeId string, b *Buffer) error {
	// 解码包头.
	var head s2sCastPacketHead
	if err := head.decode(b); err != nil {
		return pkgerrors.WithMessage(err, "gactor: svcHandlePacketS2SCast: decode request Head")
	}

	// 回复确认数据包.
	if err := s.sendAckPacket(nodeId, &head); err != nil {
		s.getLogger().ErrorFields("svcHandlePacketS2SCast: send ack packet failed", lfdActorUID(head.toId), lfdError(err))
	}

	// 创建并发送请求.
	ctx := context.Background()
	request := newContext(ctx, nil, s, newCastRequest(head, *b))
	if err := s.send2Actor(ctx, head.toId, request); err != nil {
		request.release()
		s.getLogger().ErrorFields("svcHandlePacketS2SCast: send request to actor failed", lfdActorUID(head.toId), lfdError(err))
	}

	return nil
}

// svcHandlePacketS2SDisconnected PacketTypeS2SDisconnected 处理器.
func svcHandlePacketS2SDisconnected(s *Service, nodeId string, b *Buffer) error {
	// 解码包头.
	var head s2sDisconnectedPacketHead
	if err := head.decode(b); err != nil {
		return pkgerrors.WithMessage(err, "gactor: svcHandlePacketS2SDisconnected: decode request Head")
	}

	// 回复确认数据包.
	if err := s.sendAckPacket(nodeId, &head); err != nil {
		s.getLogger().ErrorFields("svcHandlePacketS2SDisconnected: send ack packet failed", lfdActorUID(head.uid), lfdError(err))

	}

	s.freeBuffer(b)

	if err := s.send2Actor(context.Background(), head.uid, newMessageDisconnected(nodeId, head.sid)); err != nil {
		s.getLogger().ErrorFields("svcHandlePacketS2SDisconnected: send message to actor failed", lfdActorUID(head.uid), lfdError(err))
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
	PacketTypeAck:             svcHandlePacketAck,
	PacketTypeRawReq:          svcHandlePacketRawReq,
	PacketTypeS2SRpc:          svcHandlePacketS2SRpc,
	PacketTypeS2SRpcResp:      svcHandlePacketS2SRpcResp,
	PacketTypeS2SCast:         svcHandlePacketS2SCast,
	PacketTypeS2SDisconnected: svcHandlePacketS2SDisconnected,
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
