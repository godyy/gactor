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
	if a.core().pendingAsyncRPC > 0 {
		a.core().pendingAsyncRPC--
	}
	a.core().onAsyncCallbackDone(a)
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

// messageAsynnCall 异步调用消息.
type messageAsynnCall struct {
	id   uint32
	args any
	err  error
}

func newMessageAsyncCall(id uint32, args any, err error) *messageAsynnCall {
	return &messageAsynnCall{
		id:   id,
		args: args,
		err:  err,
	}
}

// handle 处理消息.
func (m *messageAsynnCall) handle(a actorImpl) {
	a.core().invokeAsyncCall(a, m.id, m.args, m.err)
	a.core().onAsyncCallbackDone(a)
}

// handleError 处理错误.
func (m *messageAsynnCall) handleError(a actorImpl, err error) {}

// release 在消息经由 Actor 处理完成后回收资源.
func (m *messageAsynnCall) release(a actorImpl) {}

// discard 在消息未进入 Actor 信箱时回收资源.
func (m *messageAsynnCall) discard(s *Service) {}

// messageStartMigration 通知源 Actor 开始迁移。
// 当前迁移目标由 Service 统一锁定，因此该消息只负责触发 draining 流程。
type messageStartMigration struct{}

func newMessageStartMigration() *messageStartMigration {
	return &messageStartMigration{}
}

func (m *messageStartMigration) handle(a actorImpl) {
	core := a.core()
	if core.isMigrated() || core.isMigratingOut() || core.isMigrationDraining() {
		return
	}

	// 这里只切换到 draining 状态，后续会等待异步闭环完成后再真正发起迁移。
	core.updateFlag(actorFlagMigrationDraining, true)
	core.service().markMigrationActorDraining(a.ActorUID())
	core.tryStartMigration(a)
}

func (m *messageStartMigration) handleError(a actorImpl, err error) {}
func (m *messageStartMigration) release(a actorImpl)                {}
func (m *messageStartMigration) discard(s *Service)                 {}

// messageMigrationResult 表示目标节点返回的迁移结果。
// 只有源租约和启动序号都匹配时，才允许推进源 Actor 的迁移状态。
type messageMigrationResult struct {
	sourceLeaseID string
	startSeq      uint32
	errCode       ErrCode
}

func newMessageMigrationResult(sourceLeaseID string, startSeq uint32, errCode ErrCode) *messageMigrationResult {
	return &messageMigrationResult{
		sourceLeaseID: sourceLeaseID,
		startSeq:      startSeq,
		errCode:       errCode,
	}
}

func (m *messageMigrationResult) handle(a actorImpl) {
	core := a.core()
	migration := core.migrationRuntime()
	if !core.isMigratingOut() || migration == nil || core.leaseID != m.sourceLeaseID || migration.startSeq != m.startSeq {
		return
	}
	if migration.resultTimerID != TimerIdNone {
		core.StopTimer(migration.resultTimerID)
		migration.resultTimerID = TimerIdNone
	}

	if m.errCode == ErrCodeOK {
		core.updateFlag(actorFlagMigratingOut, false)
		core.updateFlag(actorFlagMigrated, true)
		migration.startSeq = 0
		// 目标节点已确认接管，释放源节点的迁移并发槽位供下一批 Actor 使用。
		core.service().releaseMigrationSlot(a.ActorUID())
		// 目标节点确认接管成功后，Service 侧计划表也同步切到 migrated，
		// 这样整轮迁移就可以继续向“收敛完成”推进。
		core.service().markMigrationActorMigrated(a.ActorUID())
		if migration.retryTimerID != TimerIdNone {
			core.StopTimer(migration.retryTimerID)
			migration.retryTimerID = TimerIdNone
		}

		// 源 Actor 进入 migrated 后不再续租，避免旧租约继续对外暴露。
		if core.keepAliveTimerId != TimerIdNone {
			core.StopTimer(core.keepAliveTimerId)
			core.keepAliveTimerId = TimerIdNone
		}
		return
	}

	core.restoreMigration(a, m.errCode)
}

func (m *messageMigrationResult) handleError(a actorImpl, err error) {}
func (m *messageMigrationResult) release(a actorImpl)                {}
func (m *messageMigrationResult) discard(s *Service)                 {}

// messageApplyInboundMigration 在目标 Actor 启动后应用迁移上下文。
// 该消息负责恢复目标侧接管所需的源租约和会话信息，并触发业务恢复回调。
type messageApplyInboundMigration struct {
	sourceNodeID  string
	sourceLeaseID string
	startSeq      uint32
	session       ActorSession
	done          chan error
}

func newMessageApplyInboundMigration(sourceNodeID, sourceLeaseID string, startSeq uint32, session ActorSession) *messageApplyInboundMigration {
	return &messageApplyInboundMigration{
		sourceNodeID:  sourceNodeID,
		sourceLeaseID: sourceLeaseID,
		startSeq:      startSeq,
		session:       session,
		done:          make(chan error, 1),
	}
}

func (m *messageApplyInboundMigration) handle(a actorImpl) {
	core := a.core()
	migration := core.ensureMigrationRuntime()
	migration.acceptedSourceNodeID = m.sourceNodeID
	migration.acceptedSourceLeaseID = m.sourceLeaseID

	// 面向客户端的 Actor 需要恢复源节点转移过来的会话，保证连接无感切换。
	if ca, ok := a.(*cactor); ok && m.session.NodeId != "" {
		ca.session = m.session
	}

	if behavior, ok := a.Behavior().(ActorMigrationBehavior); ok {
		m.done <- behavior.OnMigrateIn(ActorMigrationContext{
			UID:           a.ActorUID(),
			SourceNodeID:  m.sourceNodeID,
			SourceLeaseID: m.sourceLeaseID,
			TargetNodeID:  core.service().nodeId(),
			TargetLeaseID: core.leaseID,
			StartSeq:      m.startSeq,
			Session:       m.session,
		})
	} else {
		// 未实现迁移恢复接口时，默认视为无需额外恢复逻辑。
		m.done <- nil
	}
}

func (m *messageApplyInboundMigration) handleError(a actorImpl, err error) {
	m.done <- err
}
func (m *messageApplyInboundMigration) release(a actorImpl) {}
func (m *messageApplyInboundMigration) discard(s *Service)  {}
