package gactor

import (
	"context"
)

// message 封装 Actor 消息.
type message interface {
	// handle 处理消息.
	handle(a actorImpl)

	// handleError 处理错误.
	handleError(a actorImpl, err error)

	// release 回收.
	release()
}

// messageConnect 封装连接消息.
type messageConnect struct {
	nodeId string
	sid    uint32
}

func newMessageConnect(nodeId string, sid uint32) *messageConnect {
	return &messageConnect{
		nodeId: nodeId,
		sid:    sid,
	}
}

// handle 处理消息.
func (m *messageConnect) handle(a actorImpl) {
	ca, ok := a.(*cActor)
	if !ok {
		a.core().getLogger().Error("[HandleMessageConnect] not cActor")
		return
	}

	session := ActorSession{
		NodeId: m.nodeId,
		SID:    m.sid,
	}
	a.core().getLogger().DebugFields("[HandleMessageConnect]", lfdSession(session))
	ca.updateSession(context.Background(), session)
}

// handleError 处理错误.
func (m *messageConnect) handleError(a actorImpl, err error) {}

// release 回收.
func (m *messageConnect) release() {}

// messageDisconnect 封装断开连接消息.
type messageDisconnect struct {
	nodeId string
	sid    uint32
}

func newMessageDisconnected(nodeId string, sid uint32) *messageDisconnect {
	return &messageDisconnect{
		nodeId: nodeId,
		sid:    sid,
	}
}

func (m *messageDisconnect) handle(a actorImpl) {
	ca, ok := a.(*cActor)
	if !ok {
		a.core().getLogger().Error("[HandleMessageDisconnect] not cActor")
		return
	}
	session := ActorSession{
		NodeId: m.nodeId,
		SID:    m.sid,
	}
	a.core().getLogger().DebugFields("[HandleMessageDisconnect]", lfdSession(session))
	ca.onDisconnect(session)
}

func (m *messageDisconnect) handleError(_ actorImpl, _ error) {}

func (m *messageDisconnect) release() {}

// messageCheckAlive 检查 Actor 是否存活的消息.
type messageCheckAlive struct {
	done chan error
}

func (m *messageCheckAlive) handle(a actorImpl) {
	a.core().getLogger().Debug("[HandleMessageCheckAlive]")
	close(m.done)
}

func (m *messageCheckAlive) handleError(a actorImpl, err error) {
	m.done <- err
	close(m.done)
}

func (m *messageCheckAlive) release() {}
