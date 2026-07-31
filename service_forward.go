package gactor

import (
	"errors"
	"time"
)

// Forward 向目标 Actor 透传未编码业务消息.
func (s *Service) Forward(to ActorUID, payload any) error {
	return s.forward(ActorUID{}, to, payload)
}

// forward 代理 from, 向 to 指向的目标 Actor 透传消息.
func (s *Service) forward(from, to ActorUID, payload any) error {
	// 检查服务状态
	if err := s.checkNotStopped(); err != nil {
		return err
	}

	// 先解析目标节点，并校验目标 Actor 是否允许透传.
	toNodeId, err := s.resolveForwardNodeOfActor(to)
	if err != nil {
		return err
	}

	// 本地透传只要求目标 Actor 当前已存在且已唤醒.
	if toNodeId == s.nodeId() {
		return s.forwardLocal(from, to, payload)
	}

	// 远端透传采用 cast 语义，只负责单向投递到目标节点.
	return s.forwardRemote(toNodeId, from, to, payload)
}

// resolveForwardNodeOfActor 解析可透传目标所在节点.
// 与普通 cast/rpc 不同，这里不会通过路由创建、唤醒或重新注册 Actor.
func (s *Service) resolveForwardNodeOfActor(uid ActorUID) (string, error) {
	if err := s.checkNotStopped(); err != nil {
		return "", err
	}

	define := s.getDefine(uid.Category)
	if define == nil {
		return "", ErrCodeActorUndefined
	}
	if !define.isForwardSupported() {
		return "", ErrCodeActorForwardUnsupported
	}

	location, err := s.getCfg().Handler.GetActorRegistry().GetActorLocation(uid)
	if err != nil {
		if errors.Is(err, ErrActorNotExists) {
			return "", ErrCodeActorNotExists
		}
		return "", err
	}

	if location.NodeId == "" {
		return "", ErrCodeActorNotExists
	}

	// 本地透传仍要求目标 Actor 已处于当前节点的在线状态，避免通过透传唤醒本地离线 Actor。
	if location.NodeId == s.nodeId() && location.ExpireAt > 0 && location.ExpireAt <= time.Now().Unix() {
		return "", ErrCodeActorNotExists
	}

	// 远端透传不再由源节点做在线约束，直接交给目标节点继续完成 Actor 级投递。
	return location.NodeId, nil
}

// forwardLocal 将透传消息投递到本地 Actor 信箱.
// 本地投递前先完成 payload 编码，再封装为 messageForward.
func (s *Service) forwardLocal(from, to ActorUID, payload any) error {
	encodedPayload, err := s.encodePayload(PacketTypeRawPush, payload)
	if err != nil {
		s.logger.ErrorFields("[forwardLocal] encode payload failed",
			s.lfdActorUID("fromId", from),
			s.lfdActorUID("toId", to),
			lfdError(err))
		return ErrCodeEncodePacketFailed
	}

	var buf Buffer
	buf.SetBuf(encodedPayload)
	msg := newMessageForward(buf)
	if err := s.send2StartedLocalActor(to, msg); err != nil {
		msg.discard(s)
		return err
	}
	return nil
}

// forwardRemote 将透传消息发送到远端节点，不等待业务级回包.
func (s *Service) forwardRemote(nodeId string, from, to ActorUID, payload any) error {
	ph := newS2SForwardHead(s.genSeq(), from, to)
	return s.sendRemotePacket(nodeId, &ph, payload)
}

// send2StartedLocalActor 仅向当前已存在、已唤醒的本地 Actor 投递消息.
func (s *Service) send2StartedLocalActor(uid ActorUID, msg message) error {
	actor, err := s.refActor(uid)
	if err != nil {
		return err
	}
	if actor == nil {
		return ErrCodeActorNotExists
	}
	defer actor.core().deref()
	if err = actor.core().receiveMessage(msg); err != nil {
		return err
	}
	return nil
}
