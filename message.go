package gactor

import (
	"context"
	"time"
)

// message 封装 Actor 消息.
type message interface {
	// handle 处理消息.
	handle(a actorImpl) error

	// handleError 处理错误.
	handleError(a actorImpl, err error) error

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

const disconnectTimeout = 3 * time.Second

// handle 处理消息.
func (m *messageConnect) handle(a actorImpl) error {
	ca, ok := a.(*cActor)
	if !ok {
		return ErrNotCActor
	}

	ctx, cancel := context.WithTimeout(context.Background(), disconnectTimeout)
	defer cancel()
	ca.updateSession(ctx, ActorSession{
		NodeId: m.nodeId,
		SID:    m.sid,
	})

	return nil
}

// handleError 处理错误.
func (m *messageConnect) handleError(a actorImpl, err error) error { return nil }

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

func (m *messageDisconnect) handle(a actorImpl) error {
	ca, ok := a.(*cActor)
	if !ok {
		return ErrNotCActor
	}

	ca.onDisconnect(ActorSession{
		NodeId: m.nodeId,
		SID:    m.sid,
	})

	return nil
}

func (m *messageDisconnect) handleError(_ actorImpl, _ error) error { return nil }

func (m *messageDisconnect) release() {}

// messageCheckAlive 检查 Actor 是否存活的消息.
type messageCheckAlive struct {
	done chan error
}

func (m *messageCheckAlive) handle(_ actorImpl) error {
	close(m.done)
	return nil
}

func (m *messageCheckAlive) handleError(_ actorImpl, err error) error {
	m.done <- err
	close(m.done)
	return nil
}

func (m *messageCheckAlive) release() {}
