package gactor

import (
	"time"
)

// migrationResultTimerArgs 用于标记当前等待中的迁移结果定时器。
// 通过 startSeq 识别同一轮迁移，避免旧定时器或旧结果误伤新流程。
type migrationResultTimerArgs struct {
	StartSeq uint32
}

// migrationRetryTimerArgs 用于标记迁移失败后的自动重试定时器。
type migrationRetryTimerArgs struct{}

// tryStartMigration 在异步闭环完成后尝试继续迁移。
// 只有 draining 阶段且不存在未完成异步调用时，才允许正式发起迁移。
func (a *actorCore) tryStartMigration(ai actorImpl) {
	if !a.isMigrationDraining() {
		return
	}
	if len(a.asyncCalls) > 0 || a.pendingAsyncRPC > 0 {
		return
	}
	a.startMigration(ai)
}

// startMigration 执行源 Actor 的迁移出站逻辑。
// 这里会先触发业务快照，再通知目标节点启动对应 Actor。
// 成功发包后，Actor 会从 draining 切到 migrating_out，并开始等待目标端结果。
func (a *actorCore) startMigration(ai actorImpl) {
	if !a.isMigrationDraining() {
		return
	}
	// 并发槽位不足时单独标记为等待槽位，避免被误判为目标端失败重试。
	if !a.service().tryAcquireMigrationSlot(ai.ActorUID()) {
		a.scheduleMigrationSlotWait(ai)
		return
	}
	a.ensureMigrationRuntime().waitingForSlot = false

	session := ActorSession{}
	if ca, ok := ai.(*cactor); ok {
		session = ca.session
	}

	// 目标节点由本 Actor 的冻结迁移计划决定，重试时不会重新规划。
	targetNodeID := a.service().migrationTargetOf(ai.ActorUID())
	if targetNodeID == "" {
		a.service().releaseMigrationSlot(ai.ActorUID())
		a.scheduleMigrationRetry(ai)
		return
	}
	head := newS2SMigrateStartHead(a.service().genSeq(), ai.ActorUID(), a.service().nodeId(), a.leaseID, isCActorImpl(ai), session)
	// 先记录本轮序号，确保 OnMigrateOut 失败时 restoreMigration 也能释放槽位并安排重试。
	migration := a.ensureMigrationRuntime()
	migration.startSeq = head.seq
	if behavior, ok := ai.Behavior().(ActorMigrationBehavior); ok {
		ctx := ActorMigrationContext{
			UID:           ai.ActorUID(),
			SourceNodeID:  head.sourceNodeID,
			SourceLeaseID: head.sourceLeaseID,
			TargetNodeID:  targetNodeID,
			StartSeq:      head.seq,
			Session:       session,
		}
		if err := behavior.OnMigrateOut(ctx); err != nil {
			a.getLogger().ErrorFields("OnMigrateOut failed", lfdError(err))
			a.restoreMigration(ai, err)
			return
		}
	}
	if err := a.service().sendRemotePacket(targetNodeID, &head, nil); err != nil {
		a.getLogger().ErrorFields("send migrate start failed", lfdError(err))
		a.restoreMigration(ai, err)
		return
	}

	// 迁移开始包发送成功后，源 Actor 进入等待目标节点结果的阶段。
	a.updateFlag(actorFlagMigrationDraining, false)
	a.updateFlag(actorFlagMigratingOut, true)
	a.service().updateMigrationStart(ai.ActorUID(), a.leaseID, head.seq)
	migration.resultTimerID = ai.StartTimer(migrationResultTimeout(), false, migrationResultTimerArgs{StartSeq: head.seq}, actorOnMigrationResultTimeout)
}

// restoreMigration 在目标端失败或超时时回滚源 Actor 的迁移态，并自动安排下一次重试。
func (a *actorCore) restoreMigration(ai actorImpl, cause error) {
	a.updateFlag(actorFlagMigrationDraining, false)
	a.updateFlag(actorFlagMigratingOut, false)
	migration := a.migrationRuntime()
	if migration == nil {
		return
	}
	migration.startSeq = 0
	if migration.resultTimerID != TimerIdNone {
		a.StopTimer(migration.resultTimerID)
		migration.resultTimerID = TimerIdNone
	}
	// 当前迁移尝试已结束，无论是超时还是目标端失败都必须归还并发槽位。
	a.service().releaseMigrationSlot(ai.ActorUID())

	// 回滚后立刻进入 retrying，由定时器驱动下一次重新迁移。
	a.service().markMigrationActorRetrying(ai.ActorUID(), cause)
	a.scheduleMigrationRetry(ai)
}

// scheduleMigrationRetry 在迁移失败后安排一次自动重试。
func (a *actorCore) scheduleMigrationRetry(ai actorImpl) {
	if !a.service().isMigrating() || a.isMigrated() {
		return
	}

	migration := a.ensureMigrationRuntime()
	if migration.retryTimerID != TimerIdNone {
		a.StopTimer(migration.retryTimerID)
		migration.retryTimerID = TimerIdNone
	}
	// Actor 侧保留独立计数，用于计算连续失败时的退避时间；
	// Service 侧则同步记录可观测的重试状态。
	migration.retryAttempts++
	delay := migrationRetryInterval(migration.retryAttempts)
	migration.retryTimerID = ai.StartTimer(delay, false, migrationRetryTimerArgs{}, actorOnMigrationRetry)
	if migration.retryTimerID != TimerIdNone {
		// 只有定时器创建成功时才暴露下一次重试时间，避免快照展示无效计划。
		a.service().setMigrationNextRetry(ai.ActorUID(), delay)
	}
}

// scheduleMigrationSlotWait 以短退避重新检查并发槽位。
// 这不是迁移失败，因此不会增加 retryAttempts 或 LastRetryErr。
func (a *actorCore) scheduleMigrationSlotWait(ai actorImpl) {
	migration := a.ensureMigrationRuntime()
	if migration.retryTimerID != TimerIdNone {
		return
	}
	migration.waitingForSlot = true
	a.service().markMigrationActorWaitingSlot(ai.ActorUID())
	migration.retryTimerID = ai.StartTimer(25*time.Millisecond, false, migrationRetryTimerArgs{}, actorOnMigrationRetry)
	if migration.retryTimerID != TimerIdNone {
		a.service().setMigrationNextRetry(ai.ActorUID(), 25*time.Millisecond)
	}
}

// actorOnMigrationResultTimeout 在目标节点长时间未返回结果时恢复本地处理。
func actorOnMigrationResultTimeout(args ActorTimerArgs) {
	ai := args.Actor.(actorImpl)
	core := ai.core()
	marker, ok := args.Args.(migrationResultTimerArgs)
	migration := core.migrationRuntime()
	if !ok || !core.isMigratingOut() || migration == nil || migration.startSeq != marker.StartSeq {
		return
	}
	core.restoreMigration(ai, ErrTimeout)
}

// actorOnMigrationRetry 在迁移失败后重新进入 draining 流程。
func actorOnMigrationRetry(args ActorTimerArgs) {
	ai := args.Actor.(actorImpl)
	core := ai.core()
	if _, ok := args.Args.(migrationRetryTimerArgs); !ok {
		return
	}

	migration := core.migrationRuntime()
	if migration == nil {
		return
	}
	migration.retryTimerID = TimerIdNone
	// 定时器已触发，快照不应再显示一个过期的下一次重试时间。
	core.service().clearMigrationNextRetry(ai.ActorUID())

	if core.isMigrated() || core.isMigratingOut() || !core.service().isMigrating() {
		return
	}
	if migration.waitingForSlot {
		migration.waitingForSlot = false
		core.service().markMigrationActorDraining(ai.ActorUID())
		core.tryStartMigration(ai)
		return
	}
	if core.isMigrationDraining() {
		core.tryStartMigration(ai)
		return
	}

	// 迁移目标已按 Actor 冻结，重试时只重新进入 draining 流程。
	if core.service().migrationTargetOf(ai.ActorUID()) == "" {
		return
	}
	core.updateFlag(actorFlagMigrationDraining, true)
	core.service().markMigrationActorDraining(ai.ActorUID())
	core.tryStartMigration(ai)
}

// handleMigrationMsg 统一接管迁移中的消息处理分流。
// 已迁移 Actor 只保留转发职责；draining / migratingOut 则继续让状态推进消息通行。
// 这里优先保障迁移结果、超时和异步闭环消息能够继续推进状态机。
func (a *actorCore) handleMigrationMsg(ai actorImpl, msg message) bool {
	if a.isMigrated() {
		switch msg.(type) {
		case *messageStartMigration, *messageMigrationResult:
			return true
		case *messageTriggeredTimer, *messageAsynnCall, *messageCompletedAsyncRPC:
			return true
		}

		// 已迁移后，业务消息统一转发到目标节点继续处理。
		if err := forwardMigrationMessage(a.service(), ai.ActorUID(), msg); err != nil {
			msg.handleError(ai, err)
		}
		return canForwardMigrationMessage(msg)
	}

	if a.isMigratingOut() {
		switch msg.(type) {
		case *messageMigrationResult:
			return false
		case *messageStartMigration:
			return true
		default:
			return false
		}
	}

	if a.isMigrationDraining() {
		switch msg.(type) {
		case *messageStartMigration:
			return true
		}
	}

	return false
}

func isCActorImpl(a actorImpl) bool {
	_, ok := a.(*cactor)
	return ok
}

func canForwardMigrationMessage(msg message) bool {
	switch msg.(type) {
	case *Context, *messageConnect, *messageDisconnect:
		return true
	default:
		return false
	}
}

// forwardMigrationMessage 将已迁移源 Actor 收到的消息转发到目标节点。
// 转发时会保留原始来源节点，确保目标节点处理后的回复仍然返回给真正的请求方。
// 这里复用 Actor 级冻结目标和计划内记录的 sourceLeaseID。
func forwardMigrationMessage(s *Service, uid ActorUID, msg message) error {
	if !s.isMigrationForwarding() {
		return ErrCodeMigrationForwardingClosed
	}
	st := s.getMigrationState(uid)
	targetNodeID := s.migrationTargetOf(uid)
	if st == nil || targetNodeID == "" || st.sourceLeaseID == "" {
		return ErrCodeMigrationInProgress
	}
	if !s.tryAcquireMigrationForwardSlot() {
		s.recordMigrationForwardBusy()
		return ErrCodeMigrationForwardBusy
	}
	defer s.releaseMigrationForwardSlot()

	switch m := msg.(type) {
	case *Context:
		// Context 中的具体请求自行负责构造迁移转发包，
		// 这里不再重复按 raw/rpc/cast 三种类型分支。
		head, payload, err := m.req.buildMigrationForwardRawPacket(s, uid, st.sourceLeaseID)
		if err != nil {
			return err
		}
		err = s.sendRemoteRawPacket(targetNodeID, head, payload)
		s.recordMigrationForwardResult(err)
		return err
	case *messageConnect:
		innerHead := newConnectHead(s.genSeq(), uid.ID, m.sid)
		innerPacket, err := s.encodeRawPacket(&innerHead, nil)
		if err != nil {
			return err
		}
		outerHead := newS2SMigrateForwardHead(s.genSeq(), uid, st.sourceLeaseID, m.nodeId, PacketTypeConnect)
		err = s.sendRemoteRawPacket(targetNodeID, &outerHead, innerPacket)
		s.recordMigrationForwardResult(err)
		return err
	case *messageDisconnect:
		innerHead := newDisconnectHead(s.genSeq(), uid.ID, m.sid)
		innerPacket, err := s.encodeRawPacket(&innerHead, nil)
		if err != nil {
			return err
		}
		outerHead := newS2SMigrateForwardHead(s.genSeq(), uid, st.sourceLeaseID, m.nodeId, PacketTypeDisconnect)
		err = s.sendRemoteRawPacket(targetNodeID, &outerHead, innerPacket)
		s.recordMigrationForwardResult(err)
		return err
	default:
		return ErrCodeActorForwardUnsupported
	}
}

// onAsyncCallbackDone 会在异步回调闭环完成后尝试继续迁移。
func (a *actorCore) onAsyncCallbackDone(ai actorImpl) {
	if a.pendingAsyncRPC > 0 || len(a.asyncCalls) > 0 {
		return
	}
	if a.isMigrationDraining() {
		a.tryStartMigration(ai)
	}
}
