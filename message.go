package gactor

// message 封装 Actor 消息.
type message interface {
	// handle 处理消息.
	handle(a actorImpl)

	// handleError 处理错误.
	handleError(a actorImpl, err error)

	// release 在消息经由 Actor 处理完成后回收资源.
	release(a actorImpl)

	// discard 在消息未进入 Actor 信箱时回收资源.
	discard(s *Service)
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
	ca, ok := a.(*cactor)
	if !ok {
		a.core().getLogger().Error("[HandleMessageConnect] not cActor")
		return
	}

	session := ActorSession{
		NodeId: m.nodeId,
		SID:    m.sid,
	}
	a.core().getLogger().DebugFields("[HandleMessageConnect]", lfdSession(session))
	ca.updateSession(session)
}

// handleError 处理错误.
func (m *messageConnect) handleError(a actorImpl, err error) {}

// release 回收.
func (m *messageConnect) release(_ actorImpl) {}

// discard 回收.
func (m *messageConnect) discard(_ *Service) {}

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
	ca, ok := a.(*cactor)
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

func (m *messageDisconnect) release(_ actorImpl) {}

func (m *messageDisconnect) discard(_ *Service) {}

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

func (m *messageCheckAlive) release(_ actorImpl) {}

func (m *messageCheckAlive) discard(_ *Service) {}

// messageForward 封装 Service 透传消息.
type messageForward struct {
	payload Buffer
}

func newMessageForward(payload Buffer) *messageForward {
	msg := &messageForward{
		payload: payload,
	}
	return msg
}

func (m *messageForward) handle(a actorImpl) {
	ca, ok := a.(*cactor)
	if !ok {
		a.core().getLogger().Error("[HandleMessageForward] not cActor")
		return
	}

	// 透传消息必须串行进入 Actor 信箱；实际处理时再判断连接状态.
	if !ca.session.IsConnected() {
		a.core().getLogger().Debug("[HandleMessageForward] actor not connected, skip")
		return
	}

	if err := ca.pushRawPayload(m.payload.UnreadData()); err != nil {
		a.core().getLogger().ErrorFields("[HandleMessageForward] push raw payload failed", lfdError(err))
	}
}

func (m *messageForward) handleError(a actorImpl, err error) {}

func (m *messageForward) release(a actorImpl) {
	a.core().service().freeBuffer(&m.payload)
}

func (m *messageForward) discard(s *Service) {
	s.freeBuffer(&m.payload)
}
