package gactor

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/godyy/gactor/internal/utils"
)

// ActorConfig Actor 配置.
type ActorConfig struct {
	// ActorDefines Actor 定义.
	// required.
	ActorDefines []IActorDefine

	// ClientActorCategory 当值非0时, 表示客户端需要通信的actor的分类.
	// 一般情况下, 客户端都与同一分类的actor通信.
	// PS: 该分类必须已在 ActorDefines 中定义.
	ClientActorCategory uint16
}

func (c *ActorConfig) init() {
	if len(c.ActorDefines) == 0 {
		panic("gactor: ActorConfig: ActorDefines not specified")
	}

	// 验证ClientActorCategory是否已定义.
	if c.ClientActorCategory > 0 {
		clientCategoryValid := false
		for _, ad := range c.ActorDefines {
			if ad.common().Category == c.ClientActorCategory {
				clientCategoryValid = true
				break
			}
		}
		if !clientCategoryValid {
			panic("gactor: ActorConfig: ClientActorCategory not defined in ActorDefines")
		}
	}
}

// stopActors 停机所有 Actor.
func (s *Service) stopActors() {
	// 按照优先级层级依次停机.
	// 每个层级需要一次性停机成功达到阈值, 才能继续往上层停机.
	var (
		priorityIndex int = -1
		counter       int
	)
	for {
		s.mtxActor.RLock()
		if priorityIndex != s.maxActorPriorityIndex {
			counter = 0
			priorityIndex = s.maxActorPriorityIndex
		}
		s.mtxActor.RUnlock()

		// 停机完成.
		if priorityIndex < 0 {
			break
		}

		// 停机当前层级.
		if s.stopPriorityActors(priorityIndex) {
			counter++
			if counter >= 3 {
				// 达到阈值.
				s.mtxActor.Lock()
				s.nextMaxActorPriorityIndex(priorityIndex)
				s.mtxActor.Unlock()
				counter = 0
			}
		}

		time.Sleep(1 * time.Millisecond)
	}
}

// stopPriorityActors 根据优先级索引停机执行优先级层级.
func (s *Service) stopPriorityActors(priorityIndex int) bool {
	priority := s.getPriority(priorityIndex)
	priorityActors := s.getPriorityActors(priority)
	stopped := true
	for _, categoryActors := range priorityActors.categoryActors {
		if !s.stopCategoryActors(categoryActors) {
			stopped = false
		}
	}
	return stopped
}

// stopCategoryActors 停机分类.
func (s *Service) stopCategoryActors(categoryActors *categoryActors) bool {
	stopped := true
	categoryActors.actors.Traverse(func(id int64, actor actorImpl) bool {
		_ = actor.stop()
		stopped = false
		return true
	})
	categoryActors.starters.Traverse(func(id int64, actor *actorStarter) bool {
		stopped = false
		return false
	})
	return stopped
}

// ErrActorDeployedOnOtherNode Actor 部署在其它节点上.
var ErrActorDeployedOnOtherNode = errors.New("gactor: actor deployed on other node")

// StartActor 尝试启动 uid 指定的 Actor, 通过向 Actor 投递消息并检查消息的处理
// 结果来判断是否启动成功.
// PS: 即使 Actor 已经被启动, 仍会向其投递消息, 并检查处理结果.
// PS: 开始停机后, 不能再通过 StartActor 启动 Actor.
func (s *Service) StartActor(ctx context.Context, uid ActorUID) error {
	// 优先检查ctx是否已取消
	if err := ctx.Err(); err != nil {
		return ctx.Err()
	}

	// 若 ctx 未设置deadline, 附加默认超时.
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.cfg.DefCtxTimeout)
		defer cancel()
	}

	if err := s.lockState(serviceStateStarted, true); err != nil {
		return err
	}
	defer s.unlockState(true)

	// 获取 Actor 所在节点信息.
	nodeId, err := s.getNodeIdOfActor(uid)
	if err != nil {
		return err
	}

	// 判断 Actor 是否部署在其它节点上.
	if nodeId != s.nodeId() {
		return ErrActorDeployedOnOtherNode
	}

	// 投递消息.
	msg := &messageCheckAlive{
		done: make(chan error, 1),
	}
	if err := s.send2Actor(ctx, uid, msg); err != nil {
		return err
	}

	select {
	case err := <-msg.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// updateMaxActorPriorityIndex 更新 Actor最大优先级索引.
func (s *Service) updateMaxActorPriorityIndex(index int) {
	if index > s.maxActorPriorityIndex {
		s.maxActorPriorityIndex = index
	}
}

// nextMaxActorPriorityIndex 更新 Actor 最大优先级索引至下一级.
func (s *Service) nextMaxActorPriorityIndex(index int) {
	if index == s.maxActorPriorityIndex {
		s.maxActorPriorityIndex--
	}
}

// getPriorityIndex 获取指定优先级下的所有 Actor.
func (s *Service) getPriorityActors(priority int) *priorityActors {
	return s.priorityActors[priority]
}

// getPriorityIndex 获取指定优先级类别下的所有 Actor.
func (s *Service) getCategoryActors(priority int, category uint16) *categoryActors {
	return s.priorityActors[priority].categoryActors[category]
}

// getCategoryActorsByCategory 获取指定类别下的所有 Actor.
func (s *Service) getCategoryActorsByCategory(category uint16) *categoryActors {
	define := s.getDefine(category).common()
	return s.getCategoryActors(define.Priority, define.Category)
}

// startActor 启动 uid 指定的 Actor.
func (s *Service) startActor(ctx context.Context, uid ActorUID) (actorImpl, error) {
	// 获取 Actor 定义.
	define := s.getDefine(uid.Category)
	if define == nil {
		return nil, ErrActorDefineNotExists
	}

	defineCommon := define.common()

	categoryActors := s.getCategoryActors(defineCommon.Priority, defineCommon.Category)
	var starter *actorStarter

	for starter == nil {
		categoryActors.lock(true)

		// 优先尝试引用当前有效的 Actor.
		if actor, err := categoryActors.refActor(uid.ID); err != nil {
			categoryActors.unlock(true)
			return nil, err
		} else if actor != nil {
			categoryActors.unlock(true)
			return actor, nil
		}

		// 优先获取当前正有效的启动器.
		starter = categoryActors.getStarter(uid.ID)

		categoryActors.unlock(true)

		// 当前无有效启动器.
		if starter == nil {
			// 尝试创建并添加启动.
			// 若添加成功, 执行启动逻辑.

			starter = &actorStarter{
				uid:  uid,
				done: make(chan struct{}, 1),
			}
			actual, loaded := categoryActors.addStarter(uid.ID, starter)
			if loaded {
				// 添加失败.
				close(starter.done)
				starter = nil
				if !actual.ref() {
					starter = nil
					continue
				}
				starter = actual
			} else {
				// 添加成功.
				starter.ref()

				// 更新最大优先级索引.
				priorityIndex := s.getPriorityIndex(defineCommon.Priority)
				s.mtxActor.Lock()
				s.updateMaxActorPriorityIndex(priorityIndex)
				s.mtxActor.Unlock()

				// 若当前不存在相同 id 的正在停机的 Actor, 执行启动逻辑.
				// 否则, 等待 Actor 停机完成再出发启动逻辑.
				if !categoryActors.isActorStopping(uid.ID) {
					starter.start(s)
				}
			}
		} else {
			if !starter.ref() {
				starter = nil
			}
		}
	}

	defer starter.deref()

	select {
	case <-starter.done:
		// 等待启动完成.
		actor, err := starter.actor, starter.err
		if err == nil {
			// 引用.
			err = actor.core().ref()
		}
		if err != nil {
			return nil, err
		}
		return actor, err
	case <-ctx.Done():
		// context 逻辑.
		return nil, ctx.Err()
	}
}

// refActor 引用 Actor.
func (s *Service) refActor(uid ActorUID) (actorImpl, error) {
	if err := s.lockNotStopped(true); err != nil {
		return nil, err
	}
	defer s.unlockState(true)

	// 获取 Actor 定义.
	define := s.getDefine(uid.Category)
	if define == nil {
		return nil, ErrActorDefineNotExists
	}
	defineCommon := define.common()

	// 获取 Actor 分类集合.
	categoryActors := s.getCategoryActors(defineCommon.Priority, defineCommon.Category)
	categoryActors.lock(true)
	defer categoryActors.unlock(true)

	// 引用 Actor.
	actor, err := categoryActors.refActor(uid.ID)
	if err != nil {
		return nil, err
	}

	return actor, nil
}

// getActor 获取 Actor.
func (s *Service) getActor(uid ActorUID) (actorImpl, error) {
	if err := s.lockNotStopped(true); err != nil {
		return nil, err
	}
	defer s.unlockState(true)

	// 获取 Actor 定义.
	define := s.getDefine(uid.Category)
	if define == nil {
		return nil, ErrActorDefineNotExists
	}
	defineCommon := define.common()

	// 获取 Actor 分类集合.
	categoryActors := s.getCategoryActors(defineCommon.Priority, defineCommon.Category)
	categoryActors.lock(true)
	defer categoryActors.unlock(true)

	// 获取 Actor.
	actor := categoryActors.getActor(uid.ID)
	if actor == nil {
		return nil, nil
	}

	return actor, nil
}

// newActorCore 创建 Actor.
func (s *Service) createActor(uid ActorUID) actorImpl {
	actorDefine := s.defineMap[uid.Category]
	return actorDefine.createActor(s, uid.ID)
}

// getNodeIdOfActor 获取 Actor 所在节点ID.
func (s *Service) getNodeIdOfActor(uid ActorUID) (string, error) {
	return getNodeIdOfActor(s.cfg.Handler.GetMetaDriver(), uid)
}

// send2Actor 发送消息 msg 到 uid 指定的 Actor.
func (s *Service) send2Actor(ctx context.Context, uid ActorUID, msg message) error {
	// 若 ctx 已超时, 中断后续逻辑.
	if err := ctx.Err(); err != nil {
		return err
	}

	// 若ctx未设置deadline, 设置默认超时.
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.cfg.DefCtxTimeout)
		defer cancel()
	}

	// 启动 Actor.
	actor, err := s.startActor(ctx, uid)
	if err != nil {
		return err
	}

	// 解除引用.
	defer actor.core().deref()

	return actor.core().receiveMessage(ctx, msg)
}

// onActorStopped 处理 Actor 停机完成事件.
func (s *Service) onActorStopped(actor actorImpl) {
	s.logger.DebugFields("on actor stopped", s.lfdActorUID("uid", actor.ActorUID()))

	// 删除 Actor.
	ac := actor.core()
	categoryActors := s.getCategoryActorsByCategory(ac.Category)
	categoryActors.delActor(ac.id)

	// 更新监控数据.
	s.monitorActorStop(ac.Category)

	// 启动下一个 Actor.
	if starter := categoryActors.getStarter(ac.id); starter != nil {
		starter.start(s)
	}
}

// cast 代理 from, 向 to 指向的目标 Actor 投递消息.
// ctx 若未设置deadline, 底层会设置默认的超时时间.
// ctx 只会影响消息的投送, 不会涉及请求的处理.
func (s *Service) cast(ctx context.Context, from, to ActorUID, payload any) error {
	var (
		toNodeId string
		err      error
	)

	// 优先检查 ctx 是否已取消.
	if err := ctx.Err(); err != nil {
		s.monitorCastActionContextErr(err)
		return err
	}

	// 若 ctx 未设置deadline, 附加默认超时.
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.cfg.DefCtxTimeout)
		defer cancel()
	}

	// 获取目标 Actor 所在节点信息.
	toNodeId, err = s.getNodeIdOfActor(to)
	if err != nil {
		s.monitorCastAction(MonitorCANodeInfoErr)
		return err
	}

	// 生成包序号.
	seq := s.genSeq()

	// 如果 Actor 位于其它节点.
	// 编码数据并发送到远端.
	if toNodeId != s.nodeId() {
		ph := s2sCastPacketHead{
			seq:    seq,
			fromId: from,
			toId:   to,
		}
		if err := s.sendRemotePacket(ctx, toNodeId, &ph, payload); err != nil {
			s.monitorCastActionSend2RemoteErr(err)
			return err
		}
	}

	// 编码 payload
	encodedPayload, err := s.encodePayload(PacketTypeS2SCast, payload)
	if err != nil {
		s.monitorCastAction(MonitorCASend2LocalErr)
		return err
	}

	// 创建 castRequest 并发送给 Actor.
	// actor 成功接收 cast 请求，就一定会处理.
	buf := Buffer{}
	buf.SetBuf(encodedPayload)
	request := newContext(s, newCastRequest(s.nodeId(), seq, from, buf))
	if err := s.send2Actor(ctx, to, request); err != nil {
		request.release()
		s.monitorCastActionSend2LocalErr(err)
		return err
	} else {
		s.monitorCastAction(MonitorCACast)
		return nil
	}
}

// Cast 向 to 指向的 Actor 投递消息. 若 Service 未启动或停机, 返回错误.
func (s *Service) Cast(ctx context.Context, to ActorUID, payload any) error {
	if err := s.lockState(serviceStateStarted, true); err != nil {
		return err
	}
	defer s.unlockState(true)
	return s.cast(ctx, ActorUID{}, to, payload)
}

// makeClientActorUID 构造客户端通信的目标ActorUID
func (s *Service) makeClientActorUID(id int64) ActorUID {
	return ActorUID{
		Category: s.getCfg().ClientActorCategory,
		ID:       id,
	}
}

// actorStarter Actor 启动器.
type actorStarter struct {
	mtx      sync.Mutex    // mtx.
	state    int32         // 状态, 0:初始化 1:启动
	refCount int           // 引用计数.
	uid      ActorUID      // Actor 唯一ID
	done     chan struct{} // 结束信号.
	actor    actorImpl     // Actor.
	err      error         // Error.
}

func (s *actorStarter) ref() bool {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if s.state == 2 {
		return false
	}

	s.refCount++
	return true
}

func (s *actorStarter) deref() {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if s.refCount > 0 {
		s.refCount--
		if s.refCount == 0 {
			s.doStop()
		}
	}
}

// complete 完成逻辑.
func (s *actorStarter) complete(actor actorImpl, err error) {
	s.actor, s.err = actor, err
	close(s.done)
}

// start 启动逻辑.
func (s *actorStarter) start(svc *Service) {
	// 只会启动一次.
	s.mtx.Lock()
	if s.state != 0 {
		s.mtx.Unlock()
		return
	}
	s.state = 1
	s.mtx.Unlock()

	svc.getLogger().DebugFields("actorStarter start", svc.lfdActorUID("uid", s.uid))

	categoryActors := svc.getCategoryActorsByCategory(s.uid.Category)
	defer categoryActors.delStarter(s.uid.ID)

	// 创建并启动 Actor.
	actor := svc.createActor(s.uid)
	if err := actor.start(); err != nil {
		svc.getLogger().ErrorFields("actor start failed", svc.lfdActorUID("uid", s.uid), lfdError(err))
		s.complete(nil, err)
		return
	}

	// 引用并公告 Actor 启动完成.
	err := actor.core().ref()
	if err == nil {
		// 更新监控数据.
		svc.monitorActorStart(s.uid.Category)

		// 公告 Actor.
		categoryActors.lock(false)
		categoryActors.addActor(actor)
		categoryActors.unlock(false)
	} else {
		svc.getLogger().ErrorFields("ref actor failed started", svc.lfdActorUID("uid", s.uid), lfdError(err))
		actor = nil
	}

	s.complete(actor, err)
}

// doStop
func (s *actorStarter) doStop() {
	if s.actor != nil {
		_ = s.actor.core().deref()
	}
	s.state = 2
}

// categoryActors 聚合同一分类下的所有 Actor.
type categoryActors struct {
	mtx           sync.RWMutex                               // 读写锁.
	actors        *utils.ConcurrentMap[int64, actorImpl]     // 未终止的 Actor.
	stoppedActors *utils.ConcurrentMap[int64, actorImpl]     // 已终止的 Actor.
	starters      *utils.ConcurrentMap[int64, *actorStarter] // 启动器。
}

func newCategoryActors() *categoryActors {
	return &categoryActors{
		actors:        &utils.ConcurrentMap[int64, actorImpl]{},
		stoppedActors: &utils.ConcurrentMap[int64, actorImpl]{},
		starters:      &utils.ConcurrentMap[int64, *actorStarter]{},
	}
}

func (ca *categoryActors) lock(read bool) {
	if read {
		ca.mtx.RLock()
	} else {
		ca.mtx.Lock()
	}
}

func (ca *categoryActors) unlock(read bool) {
	if read {
		ca.mtx.RUnlock()
	} else {
		ca.mtx.Unlock()
	}
}

func (ca *categoryActors) addActor(actor actorImpl) {
	ca.actors.Store(actor.core().id, actor)
}

func (ca *categoryActors) delActor(id int64) {
	ca.actors.Delete(id)
}

func (ca *categoryActors) getActor(id int64) actorImpl {
	if actor, exists := ca.actors.Load(id); exists {
		return actor
	} else {
		return nil
	}
}

func (ca *categoryActors) refActor(id int64) (actorImpl, error) {
	actor, exists := ca.actors.Load(id)
	if !exists {
		return nil, nil
	}

	err := actor.core().ref()
	if err == nil {
		return actor, nil
	}

	if errors.Is(err, ErrActorStopping) || errors.Is(err, ErrActorStopped) {
		return nil, nil
	}

	return nil, err
}

func (ca *categoryActors) isActorStopping(id int64) bool {
	if actor, exists := ca.actors.Load(id); exists {
		return !actor.core().isRunning()
	} else {
		return false
	}
}

func (ca *categoryActors) addStarter(id int64, sa *actorStarter) (actual *actorStarter, loaded bool) {
	return ca.starters.LoadOrStore(id, sa)
}

func (ca *categoryActors) getStarter(id int64) *actorStarter {
	if sa, exists := ca.starters.Load(id); exists {
		return sa
	} else {
		return nil
	}
}

func (ca *categoryActors) delStarter(id int64) {
	ca.starters.Delete(id)
}

// priorityActors 同一优先级下的所有 Actor.
type priorityActors struct {
	categoryActors map[uint16]*categoryActors
}

func newPriorityActors() *priorityActors {
	return &priorityActors{
		categoryActors: make(map[uint16]*categoryActors),
	}
}

func (pa *priorityActors) addCategoryActors(category uint16, ca *categoryActors) {
	pa.categoryActors[category] = ca
}
