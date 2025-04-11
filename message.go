package gactor

// message 封装 Actor 消息.
type message interface {
	// handle 处理消息.
	handle(a actorImpl) error

	// handleError 处理错误.
	handleError(a actorImpl, err error) error

	// release 回收.
	release()
}

// messageDisconnected 封装断开连接消息.
type messageDisconnected struct {
	nodeId string
	sid    uint32
}

func newMessageDisconnected(nodeId string, sid uint32) *messageDisconnected {
	return &messageDisconnected{
		nodeId: nodeId,
		sid:    sid,
	}
}

func (m *messageDisconnected) handle(a actorImpl) error {
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

func (m *messageDisconnected) handleError(_ actorImpl, _ error) error { return nil }

func (m *messageDisconnected) release() {}

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
