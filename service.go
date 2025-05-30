package gactor

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/godyy/gactor/internal/utils"
	"github.com/godyy/glog"
	"github.com/godyy/gtimewheel"
)

// ErrPacketEscape 表示数据包逃逸.
var ErrPacketEscape = errors.New("gactor: packet escape")

// ServiceHandler 封装 Service 处理器需要实现的功能.
type ServiceHandler interface {
	// GetMetaDriver 获取 Meta 数据驱动.
	GetMetaDriver() MetaDriver

	// GetNetAgent 获取网络代理.
	GetNetAgent() NetAgent

	// GetPacketCodec 获取数据包编解码器.
	GetPacketCodec() PacketCodec

	// GetTimeSystem 获取时间系统.
	GetTimeSystem() TimeSystem

	// GetMonitor 获取监控器.
	GetMonitor() ServiceMonitor
}

// ServiceConfig Service 配置.
type ServiceConfig struct {
	// ActorDefines Actor 定义.
	// required.
	ActorDefines []IActorDefine

	// TimeWheelLevels 时间轮配置.
	TimeWheelLevels []gtimewheel.LevelConfig

	// MaxTimerDelay 最大定时器延迟. 默认值为 TimeWheelLevels 配置支持的最大值.
	MaxTimerDelay time.Duration

	// MaxTriggerdTimerAmount 表示能够容纳的已触发的待执行定时器的最大数量. 当容量达
	// 到上限时, 新触发的定时器将等待先前的定时器执行后才能继续排队. 默认值 1000.
	MaxTriggerdTimerAmount int

	// DefRPCTimeout 默认 RPC 超时时间. 默认值 5s.
	// 网络传输保留毫秒级精度, 向下取整.
	DefRPCTimeout time.Duration

	// MaxCompletedRPCAmount 表示能够容纳的已完成调用且排队等待执行回调的最大 RPC
	// 调用数量. 当容量达到上限时, 新完成的 RPC 调用将等待先前的 RPC 调用执行回调后才
	// 能继续排队. 默认值 1000.
	MaxCompletedRPCAmount int

	// ActorReceiveCompletedAsyncRPCTimeout Actor 在接收已完成异步 RPC 调用时的
	// 超时时间. 默认值 1s.
	ActorReceiveCompletedAsyncRPCTimeout time.Duration

	// MaxRTT 最大网络延迟 ms, 用于控制因为网络RTT导致的超时衰减.
	// 默认值 50ms.
	MaxRTT int

	// Handler Service 处理器.
	// required.
	Handler ServiceHandler
}

func (c *ServiceConfig) init() {
	if len(c.ActorDefines) == 0 {
		panic("gactor: ServiceConfig: ActorDefines not specified")
	}

	if len(c.TimeWheelLevels) == 0 {
		panic("gactor: ServiceConfig: TimeWheelLevels not specified")
	}

	timeWheelMaxDelay := time.Duration(0)
	timeWheelHLevel := c.TimeWheelLevels[len(c.TimeWheelLevels)-1]
	timeWheelMaxDelay = timeWheelHLevel.Span * time.Duration(timeWheelHLevel.Slots)
	if c.MaxTimerDelay <= 0 {
		c.MaxTimerDelay = timeWheelMaxDelay
	} else if c.MaxTimerDelay > timeWheelMaxDelay {
		panic("gactor: ServiceConfig: MaxTimerDelay exceeds TimeWheelLevels")
	}

	if c.MaxTriggerdTimerAmount <= 0 {
		c.MaxTriggerdTimerAmount = 1000
	}

	if c.DefRPCTimeout <= 0 {
		c.DefRPCTimeout = 5 * time.Second
	}

	if c.MaxCompletedRPCAmount <= 0 {
		c.MaxCompletedRPCAmount = 1000
	}

	if c.ActorReceiveCompletedAsyncRPCTimeout <= 0 {
		c.ActorReceiveCompletedAsyncRPCTimeout = 1 * time.Second
	}

	if c.MaxRTT <= 0 {
		c.MaxRTT = 50
	}

	if c.Handler == nil {
		panic("gactor: ServiceConfig: Handler not specified")
	}

	if c.Handler.GetMetaDriver() == nil {
		panic("gactor: ServiceConfig: Handler has no MetaDriver")
	}

	if c.Handler.GetNetAgent() == nil {
		panic("gactor: ServiceConfig: Handler has no NetAgent")
	}

	if c.Handler.GetPacketCodec() == nil {
		panic("gactor: ServiceConfig: Handler has no PacketCodec")
	}

	if c.Handler.GetTimeSystem() == nil {
		panic("gactor: ServiceConfig: Handler has no TimeSystem")
	}
}

var (
	// ErrServiceNotStarted 服务未启动.
	ErrServiceNotStarted = errors.New("gactor: service not started")

	// ErrServiceStarted 服务已启动.
	ErrServiceStarted = errors.New("gactor: service started")

	// ErrServiceStopping 服务正在停机.
	ErrServiceStopping = errors.New("gactor: service stopping")

	// ErrServiceStopped 服务已停机.
	ErrServiceStopped = errors.New("gactor: service stopped")
)

const (
	serviceStateInit     = 0 // 初始化.
	serviceStateStarted  = 1 // 服务已启动.
	serviceStateStopping = 2 // 正在停机.
	serviceStateStopped  = 3 // 已停机.
)

// serviceStateErrs Service State error 映射.
var serviceStateErrs = map[int8]error{
	serviceStateInit:     ErrServiceNotStarted,
	serviceStateStarted:  ErrServiceStarted,
	serviceStateStopping: ErrServiceStopping,
	serviceStateStopped:  ErrServiceStopped,
}

func serviceStateErr(state int8) error {
	err, ok := serviceStateErrs[state]
	if !ok {
		panic(fmt.Sprintf("gactor: invalid service state %d", state))
	}
	return err
}

// triggeredTimer 已触发的定时器.
type triggeredTimer struct {
	cb   TimerFunc
	args TimerArgs
}

// Service Actor 服务.
type Service struct {
	*actorDefineSet                // 集成 Actor 定义.
	*rpcManager                    // RPC 调用管理器.
	cfg             *ServiceConfig // 配置.
	oriLogger       glog.Logger    // 原始日志工具.
	logger          glog.Logger    // 日志.

	mtxState sync.RWMutex  // 状态读写锁.
	state    int8          // 状态.
	cStopped chan struct{} // 已停止信号.

	mtxActor              sync.RWMutex            // Actor 读写锁.
	priorityActors        map[int]*priorityActors // 按优先级管理的 Actor 集合.
	maxActorPriorityIndex int                     // 当前 Actor 最大优先级索引.

	mtxTimer          sync.RWMutex          // 定时器读写锁.
	timeWheel         *gtimewheel.TimeWheel // 时间轮.
	timeWheelTicker   *time.Ticker          // 时间轮 ticker.
	lastTickTimeWheel time.Time             // 上次时间轮的 tick 时间.
	triggeredTimers   chan triggeredTimer   // 已触发的定时器, 等待执行.
}

func NewService(cfg *ServiceConfig, option ...ServiceOption) *Service {
	cfg.init()

	actorDefineSet := newActorDefineSet(cfg.ActorDefines)

	priorityActors := make(map[int]*priorityActors, len(actorDefineSet.priorityList))
	for _, priority := range actorDefineSet.priorityList {
		priorityActors[priority] = newPriorityActors()
	}
	for _, actorDefine := range actorDefineSet.defineMap {
		priorityActors[actorDefine.common().Priority].addCategoryActors(actorDefine.common().Category, newCategoryActors())
	}

	s := &Service{
		actorDefineSet:        actorDefineSet,
		cfg:                   cfg,
		state:                 serviceStateInit,
		cStopped:              make(chan struct{}),
		priorityActors:        priorityActors,
		maxActorPriorityIndex: -1,
		triggeredTimers:       make(chan triggeredTimer, cfg.MaxTriggerdTimerAmount),
	}

	s.rpcManager = newRPCManager(s, cfg.MaxCompletedRPCAmount)

	if timeWheel, err := gtimewheel.NewTimeWheel(cfg.TimeWheelLevels, s.timerExecutor); err != nil {
		panic(err)
	} else {
		s.timeWheel = timeWheel
	}

	for _, o := range option {
		o(s)
	}

	s.initLogger()

	return s
}

// nodeId 返回本地节点ID.
func (s *Service) nodeId() string {
	return s.cfg.Handler.GetNetAgent().NodeId()
}

// initLogger 初始化日志工具.
func (s *Service) initLogger() {
	if s.oriLogger == nil {
		s.oriLogger = createStdLogger(glog.DebugLevel)
	}
	if s.logger == nil {
		s.logger = s.oriLogger.Named("Service").WithFields(lfdNodeId(s.nodeId()))
	}
}

// setLogger 设置日志工具.
func (s *Service) setLogger(logger glog.Logger) {
	s.oriLogger = logger.Named("gactor")
	s.logger = s.oriLogger.Named("Service").WithFields(lfdNodeId(s.nodeId()))
}

// getLogger 获取 logger.
func (s *Service) getLogger() glog.Logger {
	return s.logger
}

// getCfg 获取配置.
func (s *Service) getCfg() *ServiceConfig {
	return s.cfg
}

// lockState 如果可以, 锁定 needState 指定的状态.
// read 表示是否读锁.
func (s *Service) lockState(needState int8, read bool) error {
	if read {
		s.mtxState.RLock()
	} else {
		s.mtxState.Lock()
	}

	state := s.state
	if state == needState {
		return nil
	}

	if read {
		s.mtxState.RUnlock()
	} else {
		s.mtxState.Unlock()
	}

	return serviceStateErr(state)
}

// unlockState 解锁状态. read 表示是否读锁.
func (s *Service) unlockState(read bool) {
	if read {
		s.mtxState.RUnlock()
	} else {
		s.mtxState.Unlock()
	}
}

// lockNotStopped 如果已启动且未完全停机, 锁定状态.
func (s *Service) lockNotStopped(read bool) error {
	if read {
		s.mtxState.RLock()
	} else {
		s.mtxState.Lock()
	}

	state := s.state
	if state != serviceStateInit && state != serviceStateStopped {
		return nil
	}

	if read {
		s.mtxState.RUnlock()
	} else {
		s.mtxState.Unlock()
	}

	return serviceStateErr(state)
}

// isRunning 返回是否运行中.
func (s *Service) isRunning() bool {
	s.mtxState.RLock()
	defer s.mtxState.RUnlock()
	return s.state == serviceStateStarted
}

// Start 启动.
func (s *Service) Start() error {
	if err := s.lockState(serviceStateInit, false); err != nil {
		return err
	}
	defer s.unlockState(false)

	// rpc.
	s.rpcManager.start()

	// timer.
	s.lastTickTimeWheel = s.getTimeSystem().Now()
	s.timeWheelTicker = time.NewTicker(s.cfg.TimeWheelLevels[0].Span)
	for i := 0; i < runtime.NumCPU(); i++ {
		go s.execTriggeredTimers()
	}

	s.state = serviceStateStarted

	go s.loop()

	s.logger.Info("started")

	return nil
}

// Stop 停机.
func (s *Service) Stop() error {
	if err := s.lockState(serviceStateStarted, false); err != nil {
		return err
	}
	s.state = serviceStateStopping
	s.unlockState(false)

	s.logger.Info("stopping")

	s.timeWheelTicker.Stop()
	s.stopActors()

	s.mtxState.Lock()
	s.state = serviceStateStopped
	close(s.cStopped)
	s.mtxState.Unlock()

	s.logger.Info("stopped")

	return nil
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

	if err := ctx.Err(); err != nil {
		return err
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

// StartTimer 启动定时器.
func (s *Service) StartTimer(d time.Duration, periodic bool, args any, cb TimerFunc) TimerId {
	if cb == nil {
		panic("gactor: cb is nil")
	}

	if err := s.lockState(serviceStateStarted, true); err != nil {
		return TimerIdNone
	}
	defer s.unlockState(true)

	s.mtxTimer.RLock()
	defer s.mtxTimer.RUnlock()

	now := s.getTimeSystem().Now()
	offset := now.Sub(s.lastTickTimeWheel)
	tid, err := s.timeWheel.AddTimer(gtimewheel.TimerOptions{
		Delay:    d,
		Offset:   offset,
		Periodic: periodic,
		Func:     cb,
		Args:     args,
	})
	if err != nil {
		// 更新监控数据.
		s.monitorStartTimerAmount(s.timeWheel.AddAmount())
	}

	return tid
}

// actorTimerArgs Actor 定时器.
type actorTimerArgs struct {
	uid  ActorUID       // Actor UID.
	cb   ActorTimerFunc // 回调函数.
	args any            // 参数.
}

// startActorTimer 启动 Actor 定时器.
func (s *Service) startActorTimer(uid ActorUID, d time.Duration, periodic bool, args any, cb ActorTimerFunc) TimerId {
	if cb == nil {
		panic("gactor: cb is nil")
	}

	return s.StartTimer(d, periodic, &actorTimerArgs{
		uid:  uid,
		cb:   cb,
		args: args,
	}, s.execActorTimer)
}

// execActorTimer 执行 Actor 定时器.
func (s *Service) execActorTimer(args TimerArgs) {
	aargs := args.Args.(*actorTimerArgs)

	actor, err := s.refActor(aargs.uid)
	if err != nil || actor == nil {
		return
	}

	defer actor.core().deref()

	actor.core().receiveTriggerdTimer(args.TID, aargs.args, aargs.cb)
}

// StopTimer 停止定时器.
func (s *Service) StopTimer(tid TimerId) {
	if err := s.lockState(serviceStateStarted, true); err != nil {
		return
	}
	defer s.unlockState(true)

	if s.timeWheel.RemoveTimer(tid) {
		// 更新监控数据.
		s.monitorStopTimerAmount(s.timeWheel.RemoveAmount())
	}
}

// tickTimer 推进时间轮.
func (s *Service) tickTimeWheel() {
	s.mtxTimer.Lock()

	// 计算已流逝的 tick 数.
	tickSpan := s.cfg.TimeWheelLevels[0].Span
	now := s.getTimeSystem().Now()
	elapsedTicks := int64(now.Sub(s.lastTickTimeWheel) / tickSpan)

	s.mtxTimer.Unlock()

	// 流逝时间不足一个tick.
	if elapsedTicks <= 0 {
		return
	}

	// 循环推进时间轮.
	for i := int64(0); i < elapsedTicks; i++ {
		s.mtxTimer.Lock()
		// 更新时间轮的 tick 时间.
		s.lastTickTimeWheel = s.lastTickTimeWheel.Add(tickSpan)
		// 推进时间轮.
		s.timeWheel.Tick()
		s.mtxTimer.Unlock()

		// 等待时间轮推进结束.
		s.timeWheel.TickEnd()

		// 更新定时器监控数据.
		s.monitorStartTimerAmount(s.timeWheel.AddAmount())
		s.monitorStopTimerAmount(s.timeWheel.RemoveAmount())
		s.monitorTriggerTimerAmount(s.timeWheel.TriggerAmount())
	}
}

// timerExecutor 定时器执行回调.
func (s *Service) timerExecutor(tf TimerFunc, args gtimewheel.TimerArgs) {
	select {
	case s.triggeredTimers <- triggeredTimer{
		cb:   tf,
		args: args,
	}:
	case <-s.cStopped:
	}
}

// execTriggerTimers 执行已触发的定时器.
func (s *Service) execTriggeredTimers() {
	for {
		select {
		case t := <-s.triggeredTimers:
			s.execTriggeredTimer(t)
		case <-s.cStopped:
			return
		}
	}
}

// execTimer 执行定时器.
func (s *Service) execTriggeredTimer(t triggeredTimer) {
	defer func() {
		if x := recover(); x != nil {
			s.getLogger().ErrorFields("exec timer panic", lfdPanic(x))
		}
	}()

	t.cb(t.args)
}

// doRPC 代理 from Actor 向 to 指向的 Actor 发起 RPC 调用.
// ctx 用于控制超时. params 表示请求参数. cb 为回调函数.
func (s *Service) doRPC(ctx context.Context, cancel context.CancelFunc, to ActorUID, params any, cb RPCFunc) error {
	var (
		toNodeId string
		call     *rpcCall
		p        Packet
		err      error
	)

	// 获取目标 Actor 所在节点信息.
	toNodeId, err = s.getNodeIdOfActor(to)
	if err != nil {
		s.monitorRPCAction(MonitorCANodeInfoErr)
		return err
	}

	if err := ctx.Err(); err != nil {
		s.monitorRPCActionContextErr(err)
		return err
	}

	deadline, ok := ctx.Deadline()
	if !ok {
		s.monitorRPCAction(MonitorCAContextErr)
		return errors.New("context deadline not set")
	}

	// 创建 RPC 调用实例.
	call = s.rpcManager.createCall(to, deadline, cb)

	// 包头.
	ph := s2sRpcPacketHead{
		reqId:   call.reqId,
		toId:    to,
		timeout: uint32(time.Until(deadline).Milliseconds()),
	}

	if toNodeId != s.nodeId() {
		// 目标节点非本地.
		// 编码数据包并发送到远端节点.

		if err = s.sendPacket(ctx, toNodeId, &ph, params); err != nil {
			s.monitorRPCActionSend2RemoteErr(err)
		} else if cancel != nil {
			cancel()
		}

	} else {
		// 目标节点为本地.

		// 编码 payload, 创建 rpcRequest 并发送给 Actor.
		if p, err = s.encodePayload(PacketTypeS2SRpc, params); err != nil {
			s.monitorRPCAction(MonitorCASend2LocalErr)
		} else {
			request := newContext(ctx, cancel, s, newRPCRequest(toNodeId, ph, p))
			if err = s.send2Actor(ctx, to, request); err != nil {
				request.release()
				s.monitorRPCActionSend2LocalErr(err)
			}
		}
	}

	if err != nil {
		if s.rpcManager.removeCallByReqId(call.reqId) {
			return err
		}
	}

	return nil
}

// rpcDoneFunc 同步 RPC 回调封装.
type rpcDoneFunc struct {
	done  chan struct{} // 完成信号.
	reply any           // 响应参数.
	err   error         // 错误信息.
}

func (cb *rpcDoneFunc) invoke(resp *RPCResp) {
	defer close(cb.done)

	if err := resp.Err(); err != nil {
		cb.err = err
		return
	}

	if err := resp.DecodeReply(cb.reply); err != nil {
		cb.err = err
		return
	}
}

// rpc 向 to 指向的 Actor 发起 RPC 调用.
// ctx 用于控制超时. params 表示请求参数, reply 用于接收响应参数.
func (s *Service) rpc(ctx context.Context, to ActorUID, params any, reply any) error {
	var cancel context.CancelFunc

	// 优先检查 ctx 是否done.
	// 如果 ctx 未设置 deadline, 设置默认超时.
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, ok := ctx.Deadline(); !ok {
		ctx, cancel = context.WithTimeout(ctx, s.cfg.DefRPCTimeout)
	}

	// 执行 RPC 调用.
	doneFunc := rpcDoneFunc{
		done:  make(chan struct{}),
		reply: reply,
	}
	if err := s.doRPC(ctx, cancel, to, params, doneFunc.invoke); err != nil {
		if cancel != nil {
			cancel()
		}
		return err
	}

	// 等待调用完成(超时).
	<-doneFunc.done

	return doneFunc.err
}

// RPC 向 to 指向的 Actor 发起 RPC 调用. 若 Service 未启动或停机, 返回错误.
func (s *Service) RPC(ctx context.Context, to ActorUID, params any, reply any) error {
	if err := s.lockState(serviceStateStarted, true); err != nil {
		return err
	}
	defer s.unlockState(true)
	return s.rpc(ctx, to, params, reply)
}

// asyncRPC 向 to 指向的 Actor 发起异步 RPC 调用. ctx 用于控制超时时间.
// params 表示请求参数. cb 表示异步回调函数.
func (s *Service) asyncRPC(ctx context.Context, to ActorUID, params any, cb RPCFunc) error {
	var cancel context.CancelFunc

	// 优先检查 ctx 是否done.
	// 如果 ctx 未设置 deadline, 设置默认超时.
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, ok := ctx.Deadline(); !ok {
		ctx, cancel = context.WithTimeout(ctx, s.cfg.DefRPCTimeout)
	}

	// 执行 RPC 调用.
	if err := s.doRPC(ctx, cancel, to, params, cb); err != nil {
		if cancel != nil {
			cancel()
		}
		return err
	}

	return nil
}

// AsyncRPC 向 to 指向的 Actor 发起异步 RPC 调用. 若 Service 未启动或停机, 返回错误.
func (s *Service) AsyncRPC(ctx context.Context, to ActorUID, params any, cb RPCFunc) error {
	if err := s.lockState(serviceStateStarted, true); err != nil {
		return err
	}
	defer s.unlockState(true)
	return s.asyncRPC(ctx, to, params, cb)
}

// cast 代理 from Actor 向 to 指向的 Actor 投递消息.
// ctx 用于控制超时时间, payload 表示负载消息.
// ctx 控制的超时时间只会协同到消息被发送出去, 对方是否成功收到并处理不在控
// 制范围内.
func (s *Service) cast(ctx context.Context, to ActorUID, payload any) error {
	var (
		toNodeId   string
		ctxTimeout context.Context
		cancel     context.CancelFunc
		err        error
	)

	// 获取目标 Actor 所在节点信息.
	toNodeId, err = s.getNodeIdOfActor(to)
	if err != nil {
		s.monitorCastAction(MonitorCANodeInfoErr)
		return err
	}

	// 优先检查 ctx 是否done.
	// 如果 ctx 未设置 deadline, 设置默认超时.
	if err := ctx.Err(); err != nil {
		s.monitorCastActionContextErr(err)
		return err
	}
	if _, ok := ctx.Deadline(); ok {
		ctxTimeout = ctx
	} else {
		ctxTimeout, cancel = context.WithTimeout(ctx, s.cfg.DefRPCTimeout)
		defer cancel()
	}

	// 包头.
	ph := s2sCastPacketHead{toId: to}

	// 如果 Actor 位于其它节点.
	// 编码数据并发送到远端.
	if toNodeId != s.nodeId() {
		if err := s.sendPacket(ctx, toNodeId, &ph, payload); err != nil {
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
	request := newContext(ctx, cancel, s, newCastRequest(ph, encodedPayload))
	if err := s.send2Actor(ctxTimeout, to, request); err != nil {
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
	return s.cast(ctx, to, payload)
}

// send2Actor 发送消息 msg 到 uid 指定的 Actor.
func (s *Service) send2Actor(ctx context.Context, uid ActorUID, msg message) error {
	// 若 ctx 已超时, 中断后续逻辑.
	if err := ctx.Err(); err != nil {
		return err
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
	s.logger.DebugFields("on actor stopped", lfdActorWithImpl(actor))

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

// encodePacket 编码数据包.
func (s *Service) encodePacket(ph packetHead, payload any) (Packet, error) {
	return encodePacket(ph, payload, s.cfg.Handler.GetPacketCodec())
}

// encodePayload 编码负载数据.
func (s *Service) encodePayload(pt PacketType, payload any) (Packet, error) {
	return s.cfg.Handler.GetPacketCodec().EncodePayload(pt, payload)
}

// decodePayload 解码负载数据.
func (s *Service) decodePayload(pt PacketType, p Packet, v any) error {
	return s.cfg.Handler.GetPacketCodec().DecodePayload(pt, p, v)
}

// getPacket
func (s *Service) getPacket(size int) Packet {
	return s.cfg.Handler.GetPacketCodec().GetPacket(size)
}

// putPacket 回收数据包.
func (s *Service) putPacket(p Packet) {
	s.cfg.Handler.GetPacketCodec().PutPacket(p)
}

// sendPacket 编码数据包, 并发送数据包到 nodeId 指定的节点.
func (s *Service) sendPacket(ctx context.Context, nodeId string, ph packetHead, payload any) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	// 编码数据包.
	p, err := s.encodePacket(ph, payload)
	if err != nil {
		s.logger.ErrorFields("encode packet-type failed", lfdPacketType(ph.pt()), lfdError(err))
		return errCodeEncodePacketFailed
	}

	if nodeId == s.nodeId() {
		// 本地.
		err = s.onLocalPacket(p)
	} else {
		// 远端.
		err = s.cfg.Handler.GetNetAgent().SendPacket(ctx, nodeId, p)
	}

	if err != nil {
		s.putPacket(p)
	}

	return err
}

// getTimeSystem 获取时间系统.
func (s *Service) getTimeSystem() TimeSystem {
	return s.cfg.Handler.GetTimeSystem()
}

// loop 主循环.
func (s *Service) loop() {
	for {
		select {
		case <-s.timeWheelTicker.C:
			s.tickTimeWheel()
		case <-s.rpcManager.chanTick():
			s.rpcManager.tick()
		case <-s.cStopped:
			s.rpcManager.stop()
			s.rpcManager = nil
			s.timeWheel.Stop()
			return
		}
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

	svc.getLogger().DebugFields("actorStarter start", svc.lfdActor(s.uid))

	categoryActors := svc.getCategoryActorsByCategory(s.uid.Category)
	defer categoryActors.delStarter(s.uid.ID)

	// 创建并启动 Actor.
	actor := svc.createActor(s.uid)
	if err := actor.start(); err != nil {
		svc.getLogger().ErrorFields("actor start failed", lfdActorWithImpl(actor), lfdError(err))
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
		svc.getLogger().ErrorFields("ref actor failed started", lfdActorWithImpl(actor), lfdError(err))
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
