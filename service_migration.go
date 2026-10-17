package gactor

import (
	"context"
	"errors"
	"math/rand"
	"sort"
	"time"

	pkgerrors "github.com/pkg/errors"
)

// MigrationConfig Service 动态迁移配置.
type MigrationConfig struct {
	// MigrationMaxConcurrentActors 限制同时等待目标节点结果的 Actor 数量。
	// 默认值 256；小于等于 0 时使用默认值。
	MigrationMaxConcurrentActors int

	// MigrationMaxConcurrentInboundActors 限制目标节点同时接管的 Actor 数量。
	// 默认值 256；小于等于 0 时使用默认值。
	MigrationMaxConcurrentInboundActors int

	// MigrationMaxConcurrentForwards 限制源节点同时发送的迁移转发请求数量。
	// 默认值 256；小于等于 0 时使用默认值。
	MigrationMaxConcurrentForwards int
}

// inboundMigrationAttempt 唯一标识目标节点正在处理的一次迁移接管。
// 同一 UID 的重复迁移包复用该尝试，避免重复占用接管容量。
type inboundMigrationAttempt struct {
	sourceNodeID  string
	sourceLeaseID string
}

type inboundMigrationSlotResult uint8

const (
	inboundMigrationSlotAcquired inboundMigrationSlotResult = iota
	inboundMigrationSlotDuplicate
	inboundMigrationSlotConflict
	inboundMigrationSlotBusy
)

// serviceMigrationRuntime 收拢单轮 Service 迁移的可变运行时状态。
// 所有字段必须在持有 Service.mtxMigration 时访问。
type serviceMigrationRuntime struct {
	migrationStarted         bool                              // 是否迁移中.
	migrationConverged       bool                              // 迁移计划是否已经收敛完成.
	migrationConvergedAt     time.Time                         // 迁移收敛时间.
	migrationTargetNodeID    string                            // 单目标计划的 Service 级目标；混合目标时为空.
	migrationActors          map[ActorUID]*actorMigrationState // 迁移中的 Actor 局部迁移状态.
	migrationCounters        migrationPhaseCounters            // 迁移阶段统计.
	migrationConvergedCh     chan struct{}                     // 本轮迁移收敛完成通知.
	migrationStoppedCh       chan struct{}                     // 本轮迁移被 Service 停止中断通知.
	migrationWaitStopped     bool                              // 是否已中断迁移等待.
	migrationConvergedCb     ServiceMigrationConvergedFunc     // 迁移收敛回调.
	migrationCallbackFired   bool                              // 迁移收敛回调是否已触发.
	migrationForwarding      bool                              // 是否继续由源节点转发已迁移 Actor 的业务消息.
	migrationInFlight        map[ActorUID]struct{}             // 已占用迁移并发槽位的 Actor.
	migrationForwardInFlight int                               // 正在发送的迁移转发请求数量.
	migrationForwarded       uint64                            // 已成功发送的迁移转发包数量.
	migrationForwardFailed   uint64                            // 迁移转发失败数量.
	migrationForwardBusy     uint64                            // 因转发背压被拒绝的业务消息数量.
	migrationStartedAt       time.Time                         // 本轮迁移开始时间，用于计算收敛耗时.
	migrationRetryCount      uint64                            // 本轮迁移累计安排的重试次数.
	migrationMaxRetry        uint32                            // 单个 Actor 的最大累计重试次数.
}

// actorMigrationState 记录源节点侧针对某个 Actor 的迁移上下文。
// 目标节点在计划冻结时确定，后续重试和转发不会修改。
type actorMigrationState struct {
	targetNodeID  string              // 本 Actor 在冻结计划中的目标节点。
	phase         ActorMigrationPhase // 当前迁移阶段。
	sourceLeaseID string              // 已发起迁移时使用的源租约。
	startSeq      uint32              // 当前迁移开始包序号，用于过滤迟到结果。
	retrying      bool                // 是否正等待下一次自动重试。
	retryAttempts uint32              // 本轮迁移累计重试次数。
	lastRetryAt   time.Time           // 最近一次安排重试的时间。
	lastRetryErr  string              // 最近一次重试的触发原因。
	nextRetryAt   time.Time           // 已安排重试的预计触发时间。

	startRetryScheduled bool // 启动消息重试是否已入定时器，防止重复调度。
}

// migrationPhaseCounters 聚合冻结计划中的阶段数量。
// 仅在持有 Service.mtxMigration 时读写。
type migrationPhaseCounters struct {
	nonTerminal  int // 未进入 migrated 或 stopped 终态的 Actor 数。
	pending      int // 尚未投递或处理迁移启动消息的 Actor 数。
	draining     int // 等待异步闭环完成的 Actor 数。
	waitingSlot  int // 已完成 drain、等待迁移并发槽位的 Actor 数。
	migratingOut int // 已向目标节点发起迁移、等待结果的 Actor 数。
	retrying     int // 已回滚并等待下一次迁移尝试的 Actor 数。
	migrated     int // 已确认由目标节点接管的 Actor 数。
	stopped      int // 在源节点停止、不再参与迁移的 Actor 数。
}

func newMigrationPhaseCounters(actorCount int) migrationPhaseCounters {
	return migrationPhaseCounters{
		nonTerminal: actorCount,
		pending:     actorCount,
	}
}

// add 增量调整指定阶段的数量。
func (c *migrationPhaseCounters) add(phase ActorMigrationPhase, delta int) {
	switch phase {
	case ActorMigrationPhasePending:
		c.pending += delta
	case ActorMigrationPhaseDraining:
		c.draining += delta
	case ActorMigrationPhaseWaitingSlot:
		c.waitingSlot += delta
	case ActorMigrationPhaseMigratingOut:
		c.migratingOut += delta
	case ActorMigrationPhaseRetrying:
		c.retrying += delta
	case ActorMigrationPhaseMigrated:
		c.migrated += delta
	case ActorMigrationPhaseStopped:
		c.stopped += delta
	}
}

// transition 同步阶段计数与非终态计数。
func (c *migrationPhaseCounters) transition(from, to ActorMigrationPhase) {
	if from == to {
		return
	}
	c.add(from, -1)
	c.add(to, 1)

	fromTerminal := isTerminalMigrationPhase(from)
	toTerminal := isTerminalMigrationPhase(to)
	switch {
	case !fromTerminal && toTerminal:
		c.nonTerminal--
	case fromTerminal && !toTerminal:
		c.nonTerminal++
	}
}

// ActorMigrationPhase 表示 Actor 当前的迁移阶段。
type ActorMigrationPhase string

const (
	ActorMigrationPhasePending      = ActorMigrationPhase("pending")
	ActorMigrationPhaseDraining     = ActorMigrationPhase("draining")
	ActorMigrationPhaseWaitingSlot  = ActorMigrationPhase("waiting_for_slot")
	ActorMigrationPhaseMigratingOut = ActorMigrationPhase("migrating_out")
	ActorMigrationPhaseRetrying     = ActorMigrationPhase("retrying")
	ActorMigrationPhaseMigrated     = ActorMigrationPhase("migrated")
	ActorMigrationPhaseStopped      = ActorMigrationPhase("stopped")
)

// ActorMigrationSnapshot 描述单个 Actor 当前的迁移快照。
type ActorMigrationSnapshot struct {
	UID           ActorUID            // Actor 唯一标识。
	TargetNodeID  string              // 本 Actor 在冻结计划中的目标节点。
	Phase         ActorMigrationPhase // 当前迁移阶段。
	SourceLeaseID string              // 当前迁移代际对应的源租约。
	StartSeq      uint32              // 当前迁移开始包序号。
	Retrying      bool                // 是否等待自动重试。
	RetryAttempts uint32              // 本轮迁移累计重试次数。
	LastRetryAt   time.Time           // 最近一次重试调度时间，零值表示尚未重试。
	LastRetryErr  string              // 最近一次重试原因。
	NextRetryAt   time.Time           // 下一次重试计划时间，零值表示未安排重试。
}

// ServiceMigrationSnapshot 描述当前 Service 的迁移收敛进度。
type ServiceMigrationSnapshot struct {
	Started               bool                     // 是否已开始本轮迁移。
	Converged             bool                     // 冻结计划内所有 Actor 是否均已进入终态。
	ConvergedAt           time.Time                // 零值表示当前尚未收敛完成。
	TargetNodeID          string                   // 单目标计划的锁定目标；混合目标计划时为空。
	TrackedActorCount     int                      // 本轮冻结计划中的 Actor 数量。
	PendingActorCount     int                      // 尚未开始处理迁移的 Actor 数量。
	DrainingActorCount    int                      // 等待异步闭环完成的 Actor 数量。
	WaitingSlotActorCount int                      // 因迁移并发上限等待槽位的 Actor 数量。
	MigratingOutCount     int                      // 等待目标节点接管结果的 Actor 数量。
	MigratedActorCount    int                      // 已确认迁移成功的 Actor 数量。
	RetryingActorCount    int                      // 等待下一次迁移尝试的 Actor 数量。
	StoppedActorCount     int                      // 已停止、不再参与迁移的 Actor 数量。
	InFlightActorCount    int                      // 当前占用目标端接管并发槽位的 Actor 数量。
	MaxConcurrentActors   int                      // 本轮允许同时迁移的最大 Actor 数量。
	ForwardInFlightCount  int                      // 当前正在发送的迁移转发请求数量。
	MaxConcurrentForwards int                      // 源节点允许同时发送的最大转发请求数量。
	ForwardingEnabled     bool                     // 源节点是否仍转发已迁移 Actor 的业务消息。
	ForwardedCount        uint64                   // 已成功发送的迁移转发包数量。
	ForwardFailedCount    uint64                   // 迁移转发失败数量。
	ForwardBusyCount      uint64                   // 因转发背压被拒绝的业务消息数量。
	ControlDeniedCount    uint64                   // 因来源校验被拒绝的迁移控制包数量。
	RetryCount            uint64                   // 本轮累计安排的迁移重试次数。
	MaxRetryAttempts      uint32                   // 单个 Actor 的最大累计重试次数。
	StartedAt             time.Time                // 本轮迁移开始时间。
	Actors                []ActorMigrationSnapshot // 按 UID 排序的 Actor 迁移明细。
}

// ServiceMigrationConvergedFunc 在迁移计划收敛完成后回调。
// 回调入参为触发时刻的迁移快照。
type ServiceMigrationConvergedFunc func(snapshot ServiceMigrationSnapshot)

// ServiceMigrationPlanner 为冻结计划中的每个 Actor 返回本轮固定的目标节点。
// 它在迁移开始前同步调用一次；实现必须快速、无阻塞，且不得反向创建或迁移本 Service 的 Actor。
// 返回错误会使整轮迁移在开始前失败。
type ServiceMigrationPlanner func(uid ActorUID) (targetNodeID string, err error)

// migrationStartRetryTimerArgs 标记 Service 侧的迁移启动消息重试。
// Actor 尚未收到启动消息时，重试必须由 Service 负责。
type migrationStartRetryTimerArgs struct {
	UID     ActorUID // 需要重投迁移启动消息的 Actor。
	Attempt uint32   // 对应的重试次数，用于忽略旧定时器。
}

func (s *Service) isMigrating() bool {
	s.mtxMigration.RLock()
	defer s.mtxMigration.RUnlock()
	return s.migration != nil && s.migration.migrationStarted
}

func (s *Service) migrationTargetOf(uid ActorUID) string {
	s.mtxMigration.RLock()
	defer s.mtxMigration.RUnlock()
	if s.migration == nil || !s.migration.migrationStarted {
		return ""
	}
	if st := s.migration.migrationActors[uid]; st != nil {
		return st.targetNodeID
	}
	// 兼容包内直接构造的单目标迁移运行时。
	return s.migration.migrationTargetNodeID
}

// isMigrationTargetOf 校验迁移控制结果是否来自该 Actor 锁定的目标节点。
func (s *Service) isMigrationTargetOf(uid ActorUID, nodeID string) bool {
	s.mtxMigration.RLock()
	defer s.mtxMigration.RUnlock()
	if s.migration == nil || !s.migration.migrationStarted {
		return false
	}
	if st := s.migration.migrationActors[uid]; st != nil {
		return st.targetNodeID == nodeID
	}
	return s.migration.migrationTargetNodeID == nodeID
}

// denyMigrationControl 记录因来源校验而拒绝的迁移控制包。
func (s *Service) denyMigrationControl() {
	s.mtxMigration.Lock()
	s.migrationControlDenied++
	s.mtxMigration.Unlock()
	s.monitorMigrationAction(MonitorMigrationControlDenied)
}

// cloneMigrationSnapshotLocked 在持有迁移锁时复制只读快照。
// 聚合统计直接读取阶段计数；Actor 明细的排序由锁外调用方完成。
func (s *Service) cloneMigrationSnapshotLocked() ServiceMigrationSnapshot {
	if s.migration == nil {
		return ServiceMigrationSnapshot{
			ControlDeniedCount: s.migrationControlDenied,
		}
	}
	snapshot := ServiceMigrationSnapshot{
		Started:               s.migration.migrationStarted,
		Converged:             s.migration.migrationConverged,
		ConvergedAt:           s.migration.migrationConvergedAt,
		TargetNodeID:          s.migration.migrationTargetNodeID,
		TrackedActorCount:     len(s.migration.migrationActors),
		PendingActorCount:     s.migration.migrationCounters.pending,
		DrainingActorCount:    s.migration.migrationCounters.draining,
		WaitingSlotActorCount: s.migration.migrationCounters.waitingSlot,
		MigratingOutCount:     s.migration.migrationCounters.migratingOut,
		MigratedActorCount:    s.migration.migrationCounters.migrated,
		RetryingActorCount:    s.migration.migrationCounters.retrying,
		StoppedActorCount:     s.migration.migrationCounters.stopped,
		InFlightActorCount:    len(s.migration.migrationInFlight),
		MaxConcurrentActors:   s.cfg.Migration.MigrationMaxConcurrentActors,
		ForwardInFlightCount:  s.migration.migrationForwardInFlight,
		MaxConcurrentForwards: s.cfg.Migration.MigrationMaxConcurrentForwards,
		ForwardingEnabled:     s.migration.migrationForwarding,
		ForwardedCount:        s.migration.migrationForwarded,
		ForwardFailedCount:    s.migration.migrationForwardFailed,
		ForwardBusyCount:      s.migration.migrationForwardBusy,
		ControlDeniedCount:    s.migrationControlDenied,
		RetryCount:            s.migration.migrationRetryCount,
		MaxRetryAttempts:      s.migration.migrationMaxRetry,
		StartedAt:             s.migration.migrationStartedAt,
		Actors:                make([]ActorMigrationSnapshot, 0, len(s.migration.migrationActors)),
	}
	for uid, st := range s.migration.migrationActors {
		if st == nil {
			continue
		}
		actorSnapshot := ActorMigrationSnapshot{
			UID:           uid,
			TargetNodeID:  st.targetNodeID,
			Phase:         st.phase,
			SourceLeaseID: st.sourceLeaseID,
			StartSeq:      st.startSeq,
			Retrying:      st.retrying,
			RetryAttempts: st.retryAttempts,
			LastRetryAt:   st.lastRetryAt,
			LastRetryErr:  st.lastRetryErr,
			NextRetryAt:   st.nextRetryAt,
		}
		snapshot.Actors = append(snapshot.Actors, actorSnapshot)
	}
	return snapshot
}

// sortMigrationSnapshot 在迁移锁外稳定排序 Actor 明细，缩短状态推进的锁持有时间。
func sortMigrationSnapshot(snapshot *ServiceMigrationSnapshot) {
	sort.Slice(snapshot.Actors, func(i, j int) bool {
		left, right := snapshot.Actors[i].UID, snapshot.Actors[j].UID
		if left.Category != right.Category {
			return left.Category < right.Category
		}
		return left.ID < right.ID
	})
}

func isTerminalMigrationPhase(phase ActorMigrationPhase) bool {
	switch phase {
	case ActorMigrationPhaseMigrated, ActorMigrationPhaseStopped:
		return true
	default:
		return false
	}
}

// canTransitMigrationPhase 限制迁移阶段只能沿状态机单向推进。
// stopped 允许从任意阶段进入；migrated 仅允许因 Actor 停止转为 stopped。
func canTransitMigrationPhase(from, to ActorMigrationPhase) bool {
	if from == to {
		return true
	}
	if to == ActorMigrationPhaseStopped {
		return true
	}
	switch from {
	case ActorMigrationPhasePending:
		return to == ActorMigrationPhaseDraining
	case ActorMigrationPhaseDraining:
		return to == ActorMigrationPhaseWaitingSlot || to == ActorMigrationPhaseMigratingOut || to == ActorMigrationPhaseRetrying
	case ActorMigrationPhaseWaitingSlot:
		return to == ActorMigrationPhaseDraining || to == ActorMigrationPhaseRetrying
	case ActorMigrationPhaseMigratingOut:
		return to == ActorMigrationPhaseRetrying || to == ActorMigrationPhaseMigrated
	case ActorMigrationPhaseRetrying:
		return to == ActorMigrationPhaseDraining
	default:
		return false
	}
}

// tryMarkMigrationConvergedLocked 在迁移锁保护下检查当前冻结计划是否已经完全收敛。
// 只有当计划内所有 Actor 都进入终态时，才会标记收敛并准备触发一次性回调。
// 这里不会主动退出 migrationStarted，因为源节点仍可能需要继续承担转发职责。
func (s *Service) tryMarkMigrationConvergedLocked() (ServiceMigrationSnapshot, ServiceMigrationConvergedFunc, bool) {
	if !s.migration.migrationStarted || s.migration.migrationConverged || s.migration.migrationWaitStopped {
		return ServiceMigrationSnapshot{}, nil, false
	}
	// 收敛判定改为基于增量维护的非终态计数，避免每次状态推进都在锁内全量扫描。
	if s.migration.migrationCounters.nonTerminal > 0 {
		return ServiceMigrationSnapshot{}, nil, false
	}
	s.migration.migrationConverged = true
	s.migration.migrationConvergedAt = time.Now()
	// 监控实现可能回查迁移快照，不能在 mtxMigration 锁内同步调用。
	go s.monitorMigrationConverged(s.migration.migrationConvergedAt.Sub(s.migration.migrationStartedAt))
	// 关闭完成 channel 唤醒所有等待者。阶段转换不允许从终态回退，
	// 因此收敛状态一经成立便保持不可逆。
	close(s.migration.migrationConvergedCh)
	if s.migration.migrationConvergedCb == nil || s.migration.migrationCallbackFired {
		return ServiceMigrationSnapshot{}, nil, false
	}
	s.migration.migrationCallbackFired = true
	return s.cloneMigrationSnapshotLocked(), s.migration.migrationConvergedCb, true
}

// notifyMigrationConverged 在锁外异步触发收敛回调。
// 这样即使回调本身较慢，也不会阻塞迁移状态推进。
func (s *Service) notifyMigrationConverged(snapshot ServiceMigrationSnapshot, cb ServiceMigrationConvergedFunc, ok bool) {
	if !ok || cb == nil {
		return
	}
	// 回调快照不再占用迁移锁进行排序。
	sortMigrationSnapshot(&snapshot)
	go func() {
		// 用户回调不应影响 Service 的迁移运行时。
		defer recoverAndLog("migration converged callback panic", s.getLogger(), nil)
		cb(snapshot)
	}()
}

// initMigrationPlan 冻结本轮迁移计划。
// 迁移收敛判定仅基于这份冻结快照，不受迁移过程中动态新增 Actor 影响。
func (s *Service) initMigrationPlan(targets map[ActorUID]string) {
	s.mtxMigration.Lock()
	if s.migration == nil {
		s.migration = &serviceMigrationRuntime{}
	}
	s.migration.migrationActors = make(map[ActorUID]*actorMigrationState, len(targets))
	s.migration.migrationCounters = newMigrationPhaseCounters(len(targets))
	if s.migration.migrationConvergedCh == nil {
		// 仅用于内部测试或直接初始化计划的场景；正常路径会在 beginMigration 创建。
		s.migration.migrationConvergedCh = make(chan struct{})
	}
	if s.migration.migrationStoppedCh == nil {
		// 与收敛完成 channel 分离，避免 Service 停机被错误视作迁移成功。
		s.migration.migrationStoppedCh = make(chan struct{})
	}
	for uid, targetNodeID := range targets {
		s.migration.migrationActors[uid] = &actorMigrationState{
			targetNodeID: targetNodeID,
			phase:        ActorMigrationPhasePending,
		}
	}
	s.migration.migrationConverged = false
	s.migration.migrationConvergedAt = time.Time{}
	s.migration.migrationWaitStopped = false
	s.migration.migrationCallbackFired = false
	snapshot, cb, ok := s.tryMarkMigrationConvergedLocked()
	s.mtxMigration.Unlock()

	s.notifyMigrationConverged(snapshot, cb, ok)
}

// updateMigrationActor 更新计划内 Actor 的迁移状态。
// 若 uid 不在冻结计划中，则直接忽略，避免污染本轮收敛判定。
func (s *Service) updateMigrationActor(uid ActorUID, update func(st *actorMigrationState)) {
	s.mtxMigration.Lock()
	if s.migration == nil {
		s.mtxMigration.Unlock()
		return
	}
	st := s.migration.migrationActors[uid]
	if st == nil {
		s.mtxMigration.Unlock()
		return
	}
	next := *st
	update(&next)
	if !canTransitMigrationPhase(st.phase, next.phase) {
		s.mtxMigration.Unlock()
		s.getLogger().WarnFields(
			"ignore invalid actor migration phase transition",
			s.lfdActorUID("uid", uid),
		)
		return
	}
	s.migration.migrationCounters.transition(st.phase, next.phase)
	if next.retryAttempts > st.retryAttempts {
		s.migration.migrationRetryCount += uint64(next.retryAttempts - st.retryAttempts)
		if next.retryAttempts > s.migration.migrationMaxRetry {
			s.migration.migrationMaxRetry = next.retryAttempts
		}
	}
	if st.phase == ActorMigrationPhasePending && next.phase != ActorMigrationPhasePending {
		next.startRetryScheduled = false
	}
	*st = next
	snapshot, cb, ok := s.tryMarkMigrationConvergedLocked()
	s.mtxMigration.Unlock()

	s.notifyMigrationConverged(snapshot, cb, ok)
}

// markMigrationActorDraining 标记计划内 Actor 已进入 draining 阶段。
// 该阶段表示 Actor 已停止继续消费普通业务消息，开始等待异步闭环完成。
func (s *Service) markMigrationActorDraining(uid ActorUID) {
	s.updateMigrationActor(uid, func(st *actorMigrationState) {
		st.phase = ActorMigrationPhaseDraining
		st.retrying = false
		st.nextRetryAt = time.Time{}
	})
}

// updateMigrationStart 在源 Actor 发出迁移开始包后记录本轮迁移信息。
func (s *Service) updateMigrationStart(uid ActorUID, sourceLeaseID string, startSeq uint32) {
	s.updateMigrationActor(uid, func(st *actorMigrationState) {
		st.phase = ActorMigrationPhaseMigratingOut
		st.sourceLeaseID = sourceLeaseID
		st.startSeq = startSeq
		st.retrying = false
		st.nextRetryAt = time.Time{}
	})
}

// markMigrationActorRetrying 标记计划内 Actor 正在等待自动重试。
// 该阶段说明本轮迁移已失败或超时，但 Service 仍会继续推进收敛。
func (s *Service) markMigrationActorRetrying(uid ActorUID, cause error) {
	s.updateMigrationActor(uid, func(st *actorMigrationState) {
		st.phase = ActorMigrationPhaseRetrying
		st.startSeq = 0
		st.retrying = true
		st.retryAttempts++
		st.lastRetryAt = time.Now()
		if cause != nil {
			st.lastRetryErr = cause.Error()
		}
	})
	s.monitorMigrationAction(MonitorMigrationRetry)
}

func (s *Service) markMigrationActorMigrated(uid ActorUID) {
	s.updateMigrationActor(uid, func(st *actorMigrationState) {
		st.phase = ActorMigrationPhaseMigrated
		st.startSeq = 0
		st.retrying = false
		st.nextRetryAt = time.Time{}
	})
}

// markMigrationActorStopped 标记计划内 Actor 已在源节点停止。
// 对收敛判定来说，停止也属于终态，不应继续阻塞整轮迁移收敛。
func (s *Service) markMigrationActorStopped(uid ActorUID) {
	s.releaseMigrationSlot(uid)
	s.updateMigrationActor(uid, func(st *actorMigrationState) {
		st.phase = ActorMigrationPhaseStopped
		st.startSeq = 0
		st.retrying = false
		st.nextRetryAt = time.Time{}
	})
}

// tryAcquireMigrationSlot 限制同时等待目标节点接管结果的 Actor 数量。
// 由 Actor 在真正发送 MigrateStart 前调用；同一 UID 的重复调用保持幂等。
func (s *Service) tryAcquireMigrationSlot(uid ActorUID) bool {
	s.mtxMigration.Lock()
	defer s.mtxMigration.Unlock()
	if s.migration == nil || !s.migration.migrationStarted || s.migration.migrationWaitStopped {
		return false
	}
	if _, ok := s.migration.migrationInFlight[uid]; ok {
		return true
	}
	if len(s.migration.migrationInFlight) >= s.cfg.Migration.MigrationMaxConcurrentActors {
		return false
	}
	s.migration.migrationInFlight[uid] = struct{}{}
	return true
}

// markMigrationActorWaitingSlot 表示 Actor 已完成 drain，但正等待迁移并发槽位。
// 该状态不是目标端失败，不会计入重试次数或最后错误。
func (s *Service) markMigrationActorWaitingSlot(uid ActorUID) {
	s.updateMigrationActor(uid, func(st *actorMigrationState) {
		st.phase = ActorMigrationPhaseWaitingSlot
		st.retrying = false
		st.nextRetryAt = time.Time{}
	})
	s.monitorMigrationAction(MonitorMigrationWaitForSlot)
}

// releaseMigrationSlot 释放 Actor 占用的接管并发槽位，可安全重复调用。
func (s *Service) releaseMigrationSlot(uid ActorUID) {
	s.mtxMigration.Lock()
	if s.migration != nil {
		delete(s.migration.migrationInFlight, uid)
	}
	s.mtxMigration.Unlock()
}

// recordMigrationForwardResult 汇总源节点转发链路的成功与失败次数。
func (s *Service) recordMigrationForwardResult(err error) {
	s.mtxMigration.Lock()
	if s.migration == nil {
		s.mtxMigration.Unlock()
		return
	}
	if err == nil {
		s.migration.migrationForwarded++
	} else {
		s.migration.migrationForwardFailed++
	}
	s.mtxMigration.Unlock()
	if err == nil {
		s.monitorMigrationAction(MonitorMigrationForwardSuccess)
	} else {
		s.monitorMigrationAction(MonitorMigrationForwardFailure)
	}
}

// tryAcquireMigrationForwardSlot 限制源节点转发期的同步发送并发度。
func (s *Service) tryAcquireMigrationForwardSlot() bool {
	s.mtxMigration.Lock()
	defer s.mtxMigration.Unlock()
	if s.migration == nil || !s.migration.migrationStarted || !s.migration.migrationForwarding ||
		s.migration.migrationForwardInFlight >= s.cfg.Migration.MigrationMaxConcurrentForwards {
		if s.migration != nil {
			s.migration.migrationForwardBusy++
		}
		return false
	}
	s.migration.migrationForwardInFlight++
	return true
}

// releaseMigrationForwardSlot 释放转发发送槽位。
func (s *Service) releaseMigrationForwardSlot() {
	s.mtxMigration.Lock()
	if s.migration != nil && s.migration.migrationForwardInFlight > 0 {
		s.migration.migrationForwardInFlight--
	}
	s.mtxMigration.Unlock()
}

// recordMigrationForwardBusy 记录转发背压拒绝，不将其混入底层网络失败统计。
func (s *Service) recordMigrationForwardBusy() {
	s.monitorMigrationAction(MonitorMigrationForwardBusy)
}

// MigrationSnapshot 返回当前 Service 的迁移快照。
// 该接口用于观测迁移收敛进度，不会修改任何运行时状态。
func (s *Service) MigrationSnapshot() ServiceMigrationSnapshot {
	s.mtxMigration.RLock()
	snapshot := s.cloneMigrationSnapshotLocked()
	s.mtxMigration.RUnlock()
	sortMigrationSnapshot(&snapshot)
	return snapshot
}

// WaitMigrationConverged 阻塞等待当前迁移计划收敛完成。
// 若尚未开始迁移，返回 ErrCodeMigrationNotStarted；调用方可通过 ctx 取消等待。
func (s *Service) WaitMigrationConverged(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mtxMigration.RLock()
	if s.migration == nil || !s.migration.migrationStarted {
		s.mtxMigration.RUnlock()
		return ErrCodeMigrationNotStarted
	}
	if s.migration.migrationConverged {
		s.mtxMigration.RUnlock()
		return nil
	}
	done := s.migration.migrationConvergedCh
	stopped := s.migration.migrationStoppedCh
	s.mtxMigration.RUnlock()

	// 收敛完成、Service 停机和调用方取消三种结果必须区分返回，
	// 以免部署编排将“已停机”错误地当成“已完成迁移”。
	select {
	case <-done:
		return nil
	case <-stopped:
		return ErrCodeServiceStopped
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Service) getMigrationState(uid ActorUID) *actorMigrationState {
	s.mtxMigration.RLock()
	defer s.mtxMigration.RUnlock()
	if s.migration == nil {
		return nil
	}

	st := s.migration.migrationActors[uid]
	if st == nil {
		return nil
	}

	cp := *st
	return &cp
}

// beginMigration 将 Service 切换为迁移中状态，并记录本轮冻结计划的收敛回调。
func (s *Service) beginMigration(targetNodeID string, cb ServiceMigrationConvergedFunc) error {
	if err := s.lockState(serviceStateStarted, false); err != nil {
		return err
	}
	defer s.unlockState(false)

	s.mtxMigration.Lock()
	defer s.mtxMigration.Unlock()
	if s.migration == nil {
		s.migration = &serviceMigrationRuntime{}
	}
	if s.migration.migrationStarted {
		return ErrCodeMigrationInProgress
	}
	s.migration.migrationStarted = true
	s.migration.migrationTargetNodeID = targetNodeID
	s.migration.migrationConverged = false
	s.migration.migrationConvergedAt = time.Time{}
	// 每轮迁移独立创建通知 channel，避免复用上一轮已关闭的 channel。
	s.migration.migrationConvergedCh = make(chan struct{})
	s.migration.migrationStoppedCh = make(chan struct{})
	s.migration.migrationWaitStopped = false
	s.migration.migrationForwarding = true
	s.migration.migrationInFlight = make(map[ActorUID]struct{})
	s.migration.migrationForwarded = 0
	s.migration.migrationForwardFailed = 0
	s.migration.migrationForwardBusy = 0
	s.migration.migrationForwardInFlight = 0
	s.migrationControlDenied = 0
	s.migration.migrationRetryCount = 0
	s.migration.migrationMaxRetry = 0
	s.migration.migrationStartedAt = time.Now()
	// 回调与迁移目标一样都属于“本轮迁移计划”的一部分，
	// 因此在开始迁移时一并冻结下来，避免后续动态改写语义。
	s.migration.migrationConvergedCb = cb
	s.migration.migrationCallbackFired = false
	// 监控实现是外部代码，异步触发避免其回查快照时重入迁移锁。
	go s.monitorMigrationAction(MonitorMigrationStart)
	return nil
}

// FinishMigrationForwarding 结束源节点的业务转发阶段。
// 仅当冻结计划已收敛后才能调用；调用后源节点不再接收新的转发职责，
// 调用方可随后执行 Stop 释放仍驻留的 migrated Actor。
func (s *Service) FinishMigrationForwarding() error {
	s.mtxMigration.Lock()
	defer s.mtxMigration.Unlock()
	if s.migration == nil || !s.migration.migrationStarted {
		return ErrCodeMigrationNotStarted
	}
	if !s.migration.migrationConverged {
		return ErrCodeMigrationInProgress
	}
	s.migration.migrationForwarding = false
	return nil
}

// FinalizeMigration 结束本轮已收敛迁移并清理源节点迁移上下文。
// 调用前必须先关闭转发阶段。方法会停止仍驻留源节点的 migrated Actor，
// 等待其全部退出后清空迁移计划，使 Service 可以重新开始下一轮迁移。
func (s *Service) FinalizeMigration(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	s.mtxMigration.RLock()
	if s.migration == nil || !s.migration.migrationStarted {
		s.mtxMigration.RUnlock()
		return ErrCodeMigrationNotStarted
	}
	if !s.migration.migrationConverged {
		s.mtxMigration.RUnlock()
		return ErrCodeMigrationInProgress
	}
	if s.migration.migrationForwarding {
		s.mtxMigration.RUnlock()
		return ErrCodeMigrationForwardingActive
	}
	uids := make([]ActorUID, 0, s.migration.migrationCounters.migrated)
	for uid, st := range s.migration.migrationActors {
		if st != nil && st.phase == ActorMigrationPhaseMigrated {
			uids = append(uids, uid)
		}
	}
	s.mtxMigration.RUnlock()

	// 收敛后只会留下已迁移的源 Actor。主动停掉它们可释放邮箱、定时器和本地资源。
	// OnStop 会根据 actorFlagMigrated 推导迁移回收原因，避免重复持久化业务状态。
	for _, uid := range uids {
		actor, err := s.getActor(uid)
		if err != nil || actor == nil {
			s.markMigrationActorStopped(uid)
			continue
		}
		if err := actor.core().stop(false); err != nil && !ErrIsActorStop(err) {
			return pkgerrors.WithMessagef(err, "stop migrated actor %v", uid)
		}
	}

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if s.isMigrationFinalized() {
			s.clearFinalizedMigration()
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// isMigrationFinalized 判断迁移计划中的 Actor 是否全部完成源节点退出。
func (s *Service) isMigrationFinalized() bool {
	s.mtxMigration.RLock()
	defer s.mtxMigration.RUnlock()
	if s.migration == nil || !s.migration.migrationStarted || s.migration.migrationForwarding {
		return false
	}
	return s.migration.migrationCounters.migrated == 0
}

// clearFinalizedMigration 仅在所有 migrated Actor 已停止后清空本轮迁移状态。
func (s *Service) clearFinalizedMigration() {
	s.mtxMigration.Lock()
	defer s.mtxMigration.Unlock()
	if s.migration == nil || !s.migration.migrationStarted || s.migration.migrationForwarding || s.migration.migrationCounters.migrated != 0 {
		return
	}
	s.migration = nil
	go s.monitorMigrationAction(MonitorMigrationFinalized)
}

// StartMigration 开始将当前 Service 上的 Actor 迁移到同一目标节点。
// 它是 StartMigrationWithPlanner 的单目标兼容入口。
func (s *Service) StartMigration(targetNodeID string, cb ServiceMigrationConvergedFunc) error {
	// 单目标 API 的参数校验不依赖冻结计划，空 Service 也必须拒绝无效目标。
	if targetNodeID == "" || targetNodeID == s.nodeId() {
		return ErrCodeMigrationTargetInvalid
	}
	return s.StartMigrationWithPlanner(func(ActorUID) (string, error) {
		return targetNodeID, nil
	}, cb)
}

// StartMigrationWithPlanner 将当前 Service 上的全部 Actor 按规划函数返回的目标节点迁移。
// 规划结果会在开始前冻结，后续重试和转发均复用该结果。
// cb 可选；若不关心收敛通知，直接传 nil 即可。
func (s *Service) StartMigrationWithPlanner(planner ServiceMigrationPlanner, cb ServiceMigrationConvergedFunc) error {
	if planner == nil {
		return ErrCodeMigrationTargetInvalid
	}

	// 阻止普通 Actor 在“切换迁移中标记”和“冻结计划”之间完成创建。
	s.mtxMigrationAdmission.Lock()
	// 一次抓取完成“冻结计划”和“实际投递”所需的 UID 批次。
	planBatches := make([][]ActorUID, 0, len(s.priorityList))
	for priorityIndex := len(s.priorityList) - 1; priorityIndex >= 0; priorityIndex-- {
		priority := s.getPriority(priorityIndex)
		uids := s.snapshotPriorityActorUIDs(priority)
		planBatches = append(planBatches, uids)
	}

	targets := make(map[ActorUID]string)
	commonTarget := ""
	multipleTargets := false
	for _, uids := range planBatches {
		for _, uid := range uids {
			targetNodeID, err := planner(uid)
			if err != nil {
				s.mtxMigrationAdmission.Unlock()
				return pkgerrors.WithMessagef(err, "plan migration target for actor %v", uid)
			}
			if targetNodeID == "" || targetNodeID == s.nodeId() {
				s.mtxMigrationAdmission.Unlock()
				return ErrCodeMigrationTargetInvalid
			}
			targets[uid] = targetNodeID
			if commonTarget == "" {
				commonTarget = targetNodeID
			} else if commonTarget != targetNodeID {
				multipleTargets = true
			}
		}
	}
	if multipleTargets || len(targets) == 0 {
		commonTarget = ""
	}
	if err := s.beginMigration(commonTarget, cb); err != nil {
		s.mtxMigrationAdmission.Unlock()
		return err
	}
	s.initMigrationPlan(targets)
	s.mtxMigrationAdmission.Unlock()

	// 按优先级由低到高逐层触发迁移，保证依赖更弱的 Actor 先迁走。
	for _, uids := range planBatches {
		for _, uid := range uids {
			s.deliverMigrationStart(uid)
		}
	}

	return nil
}

// deliverMigrationStart 尝试向计划内 Actor 投递迁移启动消息。
// 信箱暂时繁忙时不终止整轮迁移，而是转由 Service 定时重试。
func (s *Service) deliverMigrationStart(uid ActorUID) {
	if st := s.getMigrationState(uid); st == nil || st.phase != ActorMigrationPhasePending {
		return
	}

	msg := newMessageStartMigration()
	err := s.send2StartedLocalActor(uid, msg)
	if err == nil {
		return
	}
	if errors.Is(err, ErrCodeActorNotExists) || ErrIsActorStop(err) {
		// Actor 已不在源节点运行，将其视为本轮迁移的终态，避免阻塞收敛。
		s.markMigrationActorStopped(uid)
		return
	}
	if ErrIsServiceStop(err) {
		// 停机期间不再安排新的重试，WaitMigrationConverged 会由停机路径唤醒。
		return
	}
	s.getLogger().WarnFields(
		"send migration start message failed, will retry",
		s.lfdActorUID("uid", uid),
		lfdError(err),
	)

	s.scheduleMigrationStartRetry(uid, err)
}

// onMigrationStartRetry 重试尚未成功投递的迁移启动消息。
func (s *Service) onMigrationStartRetry(args TimerArgs) {
	retry, ok := args.Args.(migrationStartRetryTimerArgs)
	if !ok {
		return
	}
	if !s.consumeMigrationStartRetry(retry) {
		return
	}
	s.deliverMigrationStart(retry.UID)
}

// scheduleMigrationStartRetry 为尚未投递成功的启动消息安排唯一的一次重试。
func (s *Service) scheduleMigrationStartRetry(uid ActorUID, cause error) {
	s.mtxMigration.Lock()
	if s.migration == nil {
		s.mtxMigration.Unlock()
		return
	}
	st := s.migration.migrationActors[uid]
	if st == nil || st.phase != ActorMigrationPhasePending || st.startRetryScheduled || s.migration.migrationWaitStopped {
		s.mtxMigration.Unlock()
		return
	}
	st.startRetryScheduled = true
	st.retryAttempts++
	s.migration.migrationRetryCount++
	if st.retryAttempts > s.migration.migrationMaxRetry {
		s.migration.migrationMaxRetry = st.retryAttempts
	}
	st.lastRetryAt = time.Now()
	if cause != nil {
		st.lastRetryErr = cause.Error()
	}
	attempt := st.retryAttempts
	// 同一 Actor 同时只允许一个待触发的启动消息重试；
	// 退避和抖动避免大量繁忙信箱在同一时刻被再次写入。
	delay := migrationRetryInterval(attempt)
	st.nextRetryAt = time.Now().Add(delay)
	s.mtxMigration.Unlock()
	s.monitorMigrationAction(MonitorMigrationRetry)

	if tid := s.StartTimer(delay, false, migrationStartRetryTimerArgs{
		UID:     uid,
		Attempt: attempt,
	}, s.onMigrationStartRetry); tid == TimerIdNone {
		s.clearMigrationStartRetry(uid, attempt)
	}
}

// consumeMigrationStartRetry 仅消费仍属于当前重试轮次的定时器，避免旧定时器重复投递。
func (s *Service) consumeMigrationStartRetry(retry migrationStartRetryTimerArgs) bool {
	s.mtxMigration.Lock()
	defer s.mtxMigration.Unlock()

	if s.migration == nil {
		return false
	}
	st := s.migration.migrationActors[retry.UID]
	if st == nil || st.phase != ActorMigrationPhasePending || !st.startRetryScheduled ||
		st.retryAttempts != retry.Attempt || s.migration.migrationWaitStopped {
		return false
	}
	st.startRetryScheduled = false
	st.nextRetryAt = time.Time{}
	return true
}

func (s *Service) clearMigrationStartRetry(uid ActorUID, attempt uint32) {
	s.mtxMigration.Lock()
	defer s.mtxMigration.Unlock()

	if s.migration != nil {
		if st := s.migration.migrationActors[uid]; st != nil && st.retryAttempts == attempt {
			st.startRetryScheduled = false
			st.nextRetryAt = time.Time{}
		}
	}
}

// setMigrationNextRetry 记录已进入 retrying 的 Actor 下次自动重试时间。
func (s *Service) setMigrationNextRetry(uid ActorUID, delay time.Duration) {
	s.updateMigrationActor(uid, func(st *actorMigrationState) {
		st.nextRetryAt = time.Now().Add(delay)
	})
}

// clearMigrationNextRetry 清除已触发或已取消的重试计划。
func (s *Service) clearMigrationNextRetry(uid ActorUID) {
	s.updateMigrationActor(uid, func(st *actorMigrationState) {
		st.nextRetryAt = time.Time{}
	})
}

// migrationRetryInterval 返回带上限的指数退避时间，并加入小幅随机抖动削峰。
func migrationRetryInterval(attempt uint32) time.Duration {
	const (
		baseDelay = time.Second
		maxDelay  = 30 * time.Second
		maxShift  = 5
	)
	if attempt == 0 {
		attempt = 1
	}
	shift := attempt - 1
	if shift > maxShift {
		shift = maxShift
	}
	delay := baseDelay << shift
	if delay > maxDelay {
		delay = maxDelay
	}
	// 仅增加不超过 25% 的随机延迟，既能削峰，又不会让重试时间不可预期地过长。
	return delay + time.Duration(rand.Int63n(int64(delay/4)+1))
}

// abortMigrationWaiters 在 Service 停机时中断仍在等待的迁移收敛调用。
func (s *Service) abortMigrationWaiters() {
	s.mtxMigration.Lock()
	defer s.mtxMigration.Unlock()

	if s.migration == nil || !s.migration.migrationStarted || s.migration.migrationConverged || s.migration.migrationWaitStopped {
		return
	}
	s.migration.migrationWaitStopped = true
	// 停机后禁止再将 Actor 状态推进为“迁移收敛”，并唤醒所有阻塞等待者。
	close(s.migration.migrationStoppedCh)
}

// snapshotPriorityActorUIDs 抓取当前优先级下的 Actor 快照列表。
// 这里只采 UID 快照，不在 Service 层直接介入业务快照。
func (s *Service) snapshotPriorityActorUIDs(priority int) []ActorUID {
	priorityActors := s.getPriorityActors(priority)
	uids := make([]ActorUID, 0, 16)
	for category, categoryActors := range priorityActors.categoryActors {
		categoryActors.actors.Traverse(func(id ActorID, actor actorImpl) bool {
			uids = append(uids, ActorUID{Category: category, ID: id})
			return true
		})
	}
	return uids
}

// tryAcquireInboundMigrationSlot 为目标端接管预留并发容量。
// 重复包不会重复占用容量，不同源租约则拒绝覆盖正在处理的接管。
func (s *Service) tryAcquireInboundMigrationSlot(head s2sMigrateStartPacketHead) inboundMigrationSlotResult {
	attempt := inboundMigrationAttempt{
		sourceNodeID:  head.sourceNodeID,
		sourceLeaseID: head.sourceLeaseID,
	}
	s.mtxInboundMigration.Lock()
	defer s.mtxInboundMigration.Unlock()

	if current, ok := s.inboundMigrationInFlight[head.uid]; ok {
		if current == attempt {
			return inboundMigrationSlotDuplicate
		}
		return inboundMigrationSlotConflict
	}
	if len(s.inboundMigrationInFlight) >= s.cfg.Migration.MigrationMaxConcurrentInboundActors {
		return inboundMigrationSlotBusy
	}
	if s.inboundMigrationInFlight == nil {
		s.inboundMigrationInFlight = make(map[ActorUID]inboundMigrationAttempt)
	}
	s.inboundMigrationInFlight[head.uid] = attempt
	return inboundMigrationSlotAcquired
}

// releaseInboundMigrationSlot 在目标端接管完成或启动失败后归还并发容量。
func (s *Service) releaseInboundMigrationSlot(uid ActorUID, sourceNodeID, sourceLeaseID string) {
	s.mtxInboundMigration.Lock()
	defer s.mtxInboundMigration.Unlock()

	if current, ok := s.inboundMigrationInFlight[uid]; ok &&
		current.sourceNodeID == sourceNodeID && current.sourceLeaseID == sourceLeaseID {
		delete(s.inboundMigrationInFlight, uid)
	}
}

func (s *Service) sendInboundMigrationResult(head s2sMigrateStartPacketHead, errCode ErrCode) error {
	resultHead := newS2SMigrateResultHead(s.genSeq(), head.uid, head.sourceLeaseID, head.seq, errCode)
	return s.sendRemotePacket(head.sourceNodeID, &resultHead, nil)
}

// startInboundMigration 在目标节点接收迁移开始包后拉起对应 Actor。
// 若同一轮迁移已经接管完成，则直接回复成功，保持幂等。
func (s *Service) startInboundMigration(head s2sMigrateStartPacketHead) error {
	if actor, err := s.getActor(head.uid); err == nil && actor != nil {
		if migration := actor.core().migrationRuntime(); migration != nil &&
			migration.acceptedSourceNodeID == head.sourceNodeID &&
			migration.acceptedSourceLeaseID == head.sourceLeaseID {
			resultHead := newS2SMigrateResultHead(s.genSeq(), head.uid, head.sourceLeaseID, head.seq, ErrCodeOK)
			return s.sendRemotePacket(head.sourceNodeID, &resultHead, nil)
		}
		// 目标节点已经承载同一 UID 的另一轮迁移，旧租约不能覆盖当前实例。
		// 立即通知源端失败，避免其被动等待迁移结果超时。
		resultHead := newS2SMigrateResultHead(s.genSeq(), head.uid, head.sourceLeaseID, head.seq, ErrCodeMigrationInProgress)
		return s.sendRemotePacket(head.sourceNodeID, &resultHead, nil)
	}

	switch s.tryAcquireInboundMigrationSlot(head) {
	case inboundMigrationSlotDuplicate:
		// 同一接管已在执行，完成后会统一回复迁移结果。
		return nil
	case inboundMigrationSlotConflict:
		return s.sendInboundMigrationResult(head, ErrCodeMigrationInProgress)
	case inboundMigrationSlotBusy:
		// 源端收到 ErrCodeActorBusy 后会按既有退避逻辑重试。
		s.monitorMigrationAction(MonitorMigrationTakeOverFailure)
		return s.sendInboundMigrationResult(head, ErrCodeActorBusy)
	}

	define := s.getDefine(head.uid.Category)
	if define == nil {
		s.releaseInboundMigrationSlot(head.uid, head.sourceNodeID, head.sourceLeaseID)
		return ErrCodeActorUndefined
	}
	defineBase := define.base()
	categoryActors := s.getCategoryActors(defineBase.priority, defineBase.category)
	// inbound 参数必须在 starter 启动前写入，确保注册和恢复流程按接管语义执行。
	starter := s.ensureActorStarter(categoryActors, defineBase, head.uid, "", func(starter *actorStarter) {
		starter.isInboundMigration = true
		starter.migrationSourceNodeID = head.sourceNodeID
		starter.migrationSourceLeaseID = head.sourceLeaseID
		starter.migrationSession = ActorSession{
			NodeId: head.sessionNodeID,
			SID:    head.sessionSID,
		}
		starter.migrationStartSeq = head.seq
		starter.inboundMigrationSlotHeld = true
	})
	// inbound migration 不会经过普通 startActor 调用链。
	// 这里补齐 starter 的引用释放，避免目标 Actor 保留启动期引用而无法停机。
	if !starter.ref() {
		s.releaseInboundMigrationSlot(head.uid, head.sourceNodeID, head.sourceLeaseID)
		return ErrCodeActorStartFailed
	}
	defer starter.deref()

	if !starter.isCompleted() && !categoryActors.isActorStopping(head.uid.ID) {
		starter.start(s)
		return nil
	}
	// starter 已由本地启动或停机流程占用，当前迁移不会进入 starter.start。
	s.releaseInboundMigrationSlot(head.uid, head.sourceNodeID, head.sourceLeaseID)
	return nil
}

// shouldForwardMissingActor 判断当前消息是否允许在缺失 Actor 时直接走迁移转发。
func (s *Service) shouldForwardMissingActor(msg message) bool {
	if !s.isMigrationForwarding() {
		return false
	}
	switch msg.(type) {
	case *Context, *messageConnect, *messageDisconnect:
		return true
	default:
		return false
	}
}

// isMigrationForwarding 判断源节点是否仍承担已迁移 Actor 的转发职责。
func (s *Service) isMigrationForwarding() bool {
	s.mtxMigration.RLock()
	defer s.mtxMigration.RUnlock()
	return s.migration != nil && s.migration.migrationStarted && s.migration.migrationForwarding
}

func (s *Service) tryForwardMissingActor(uid ActorUID, msg message) error {
	if !s.shouldForwardMissingActor(msg) {
		return nil
	}

	// 迁移过程中若本地 Actor 缺失，则直接按冻结的迁移上下文转发到目标节点继续处理。
	return forwardMigrationMessage(s, uid, msg)
}

func migrationResultTimeout() time.Duration {
	return 5 * time.Second
}

// acceptsMigrationLease 判断目标节点当前是否接受该源租约对应的迁移转发。
// 这里同时兼容已启动 Actor 和尚在启动中的 actorStarter。
func (s *Service) acceptsMigrationLease(uid ActorUID, sourceNodeID, sourceLeaseID string) bool {
	actor, err := s.getActor(uid)
	if err == nil && actor != nil {
		migration := actor.core().migrationRuntime()
		return migration != nil && migration.acceptedSourceNodeID == sourceNodeID && migration.acceptedSourceLeaseID == sourceLeaseID
	}

	define := s.getDefine(uid.Category)
	if define == nil {
		return false
	}
	starter := s.getCategoryActors(define.base().priority, define.base().category).getStarter(uid.ID)
	if starter == nil {
		return false
	}
	starter.mtx.Lock()
	defer starter.mtx.Unlock()
	return starter.isInboundMigration && starter.migrationSourceNodeID == sourceNodeID && starter.migrationSourceLeaseID == sourceLeaseID
}
