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

// messageCompletedAsyncRPC 封装异步 RPC 完成消息.
type messageCompletedAsyncRPC struct {
	payload Buffer       // 响应负载数据.
	err     error        // 错误信息.
	cb      ActorRPCFunc // 回调函数.
}

func newMessageCompletedAsyncRPC(payload Buffer, err error, cb ActorRPCFunc) *messageCompletedAsyncRPC {
	return &messageCompletedAsyncRPC{
		payload: payload,
		err:     err,
		cb:      cb,
	}
}

// handle 处理消息.
func (m *messageCompletedAsyncRPC) handle(a actorImpl) {
	resp := RPCResp{
		svc:     a.core().service(),
		payload: m.payload,
		err:     m.err,
	}
	m.cb(a, resp)
	resp.release()
}

// handleError 处理错误.
func (m *messageCompletedAsyncRPC) handleError(a actorImpl, err error) {}

// release 在消息经由 Actor 处理完成后回收资源.
func (m *messageCompletedAsyncRPC) release(a actorImpl) {
	a.core().service().freeBuffer(&m.payload)
}

// discard 在消息未进入 Actor 信箱时回收资源.
func (m *messageCompletedAsyncRPC) discard(s *Service) {
	s.freeBuffer(&m.payload)
}

// messageTriggeredTimer 封装已触发定时器消息.
type messageTriggeredTimer struct {
	tid  TimerId        // 定时器ID.
	f    ActorTimerFunc // 定时器方法.
	args any            // 参数.
}

func newMessageTriggeredTimer(tid TimerId, f ActorTimerFunc, args any) *messageTriggeredTimer {
	return &messageTriggeredTimer{
		tid:  tid,
		f:    f,
		args: args,
	}
}

// handle 处理消息.
func (m *messageTriggeredTimer) handle(a actorImpl) {
	core := a.core()
	if !core.isRunning() || !core.service().isRunning() {
		return
	}
	m.f(ActorTimerArgs{
		Actor: a,
		TID:   m.tid,
		Args:  m.args,
	})
}

// handleError 处理错误.
func (m *messageTriggeredTimer) handleError(a actorImpl, err error) {}

// release 在消息经由 Actor 处理完成后回收资源.
func (m *messageTriggeredTimer) release(a actorImpl) {}

// discard 在消息未进入 Actor 信箱时回收资源.
func (m *messageTriggeredTimer) discard(s *Service) {}

// messageFunc 封装函数消息.
type messageFunc struct {
	f    ActorFunc
	args any
}

func newMessageFunc(f ActorFunc, args any) *messageFunc {
	return &messageFunc{
		f:    f,
		args: args,
	}
}

// handle 处理消息.
func (m *messageFunc) handle(a actorImpl) {
	m.f(a, m.args)
}

// handleError 处理错误.
func (m *messageFunc) handleError(a actorImpl, err error) {}

// release 在消息经由 Actor 处理完成后回收资源.
func (m *messageFunc) release(a actorImpl) {}

// discard 在消息未进入 Actor 信箱时回收资源.
func (m *messageFunc) discard(s *Service) {}
