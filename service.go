package gactor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/godyy/gactor/internal/utils"
	"github.com/godyy/glog"
	"github.com/godyy/gtimewheel"
	pkgerrors "github.com/pkg/errors"
)

// ErrBytesEscape 表示字节切片逃逸.
var ErrBytesEscape = errors.New("gactor: bytes escape")

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
	// ActorConfig Actor 配置.
	ActorConfig

	// TimerConfig 定时器配置.
	TimerConfig

	// RPCConfig RPC 配置.
	RPCConfig

	// MaxRTT 最大网络延迟 ms, 用于控制因为网络RTT导致的超时衰减.
	// 默认值 50ms.
	MaxRTT int

	// DefCtxTimeout 默认上下文超时时间.
	// 默认值 5s.
	DefCtxTimeout time.Duration

	// Handler Service 处理器.
	// required.
	Handler ServiceHandler
}

func (c *ServiceConfig) init() {
	c.ActorConfig.init()
	c.TimerConfig.init()
	c.RPCConfig.init()

	if c.MaxRTT <= 0 {
		c.MaxRTT = 50
	}

	if c.DefCtxTimeout <= 0 {
		c.DefCtxTimeout = 5 * time.Second
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

// Service Actor 服务.
type Service struct {
	cfg             *ServiceConfig // 配置.
	seqIncr         uint32         // 序号自增键值.
	oriLogger       glog.Logger    // 原始日志工具.
	logger          glog.Logger    // 日志.
	*actorDefineSet                // 集成 Actor 定义.
	*rpcManager                    // RPC 调用管理器.
	*ackManager                    // Ack 管理器.

	mtxState sync.RWMutex    // 状态读写锁.
	state    int8            // 状态.
	stopWait *utils.StopWait // 停机等待.

	mtxActor              sync.RWMutex            // Actor 读写锁.
	priorityActors        map[int]*priorityActors // 按优先级管理的 Actor 集合.
	maxActorPriorityIndex int                     // 当前 Actor 最大优先级索引.

	mtxTimer          sync.RWMutex          // 定时器读写锁.
	timeWheel         *gtimewheel.TimeWheel // 时间轮.
	lastTickTimeWheel time.Time             // 上次时间轮的 tick 时间.
	triggeredTimers   chan triggeredTimer   // 已触发的定时器, 等待执行.
	timeWheelStop     chan struct{}         // 时间轮停止信号.
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
		stopWait:              utils.NewStopWait(),
		priorityActors:        priorityActors,
		maxActorPriorityIndex: -1,
		triggeredTimers:       make(chan triggeredTimer, cfg.MaxTriggerdTimerAmount),
	}

	s.rpcManager = newRPCManager(s)

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

// getStopWait 获取停机工具.
func (s *Service) getStopWait() *utils.StopWait {
	return s.stopWait
}

// getCfg 获取配置.
func (s *Service) getCfg() *ServiceConfig {
	return s.cfg
}

// genSeq 生成序号.
func (s *Service) genSeq() uint32 {
	return atomic.AddUint32(&s.seqIncr, 1)
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

	s.state = serviceStateStarted

	// timer.
	s.startTimeWheel()

	// rpc.
	s.startRpcManager()

	// ack
	s.startAckManager()

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

	s.stopActors()
	s.stopTimeWheel()

	s.mtxState.Lock()
	s.state = serviceStateStopped
	s.mtxState.Unlock()

	s.stopWait.Stop(true)

	s.logger.Info("stopped")

	return nil
}

// encodePacket 编码数据包.
func (s *Service) encodePacket(ph packetHead, payload any) ([]byte, error) {
	return encodePacket(ph, payload, s.cfg.Handler.GetPacketCodec())
}

// encodePayload 编码负载数据.
func (s *Service) encodePayload(pt PacketType, payload any) ([]byte, error) {
	return s.cfg.Handler.GetPacketCodec().EncodePayload(pt, payload)
}

// decodePayload 解码负载数据.
func (s *Service) decodePayload(pt PacketType, b *Buffer, v any) error {
	return s.cfg.Handler.GetPacketCodec().DecodePayload(pt, b, v)
}

// getBytes 获取容量为 cap 的字节切片.
func (s *Service) getBytes(cap int) []byte {
	return s.cfg.Handler.GetPacketCodec().GetBytes(cap)
}

// putBytes 回收字节切片
func (s *Service) putBytes(b []byte) {
	s.cfg.Handler.GetPacketCodec().PutBytes(b)
}

// allocBuffer 分配缓冲区.
func (s *Service) allocBuffer(buf *Buffer, size int) {
	b := s.cfg.Handler.GetPacketCodec().GetBytes(size)
	buf.SetBuf(b)
}

// freeBuffer 回收缓冲区.
func (s *Service) freeBuffer(buf *Buffer) {
	if b := buf.Data(); b != nil {
		s.cfg.Handler.GetPacketCodec().PutBytes(b)
		buf.SetBuf(nil)
	}
}

// send 发送字节数据.
func (s *Service) send(ctx context.Context, nodeId string, b []byte) error {
	// 若ctx未设置deadline, 设置默认超时.
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.cfg.DefCtxTimeout)
		defer cancel()
	}
	return s.cfg.Handler.GetNetAgent().Send(ctx, nodeId, b)
}

// sendLocalPacket 发送本地数据包.
func (s *Service) sendLocalPacket(ctx context.Context, ph packetHead, payload any) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	// 编码数据包.
	b, err := s.encodePacket(ph, payload)
	if err != nil {
		s.logger.ErrorFields("[sendLocalPacket] encode packet failed", lfdPacketType(ph.pt()), lfdError(err))
		return errCodeEncodePacketFailed
	}

	return s.onLocalPacket(b)
}

// sendRemotePacket 发送远程数据包.
func (s *Service) sendRemotePacket(ctx context.Context, nodeId string, ph packetHead, payload any) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	// 编码数据包.
	b, err := s.encodePacket(ph, payload)
	if err != nil {
		s.logger.ErrorFields("[sendRemotePacket] encode packet failed", lfdPacketType(ph.pt()), lfdError(err))
		return errCodeEncodePacketFailed
	}

	// 添加待确认数据包.
	s.addPacket2Ack(nodeId, ph.pt(), ph.seq(), b)

	// 发送数据.
	if err = s.send(ctx, nodeId, b); err != nil {
		// 若发送失败, 直接移除待确认数据包.
		s.remPacket2Ack(ph.pt(), ph.seq())
		return pkgerrors.WithMessage(err, "send packet")
	}

	return nil
}

// sendPacket 编码数据包, 并发送数据包到 nodeId 指定的节点.
func (s *Service) sendPacket(ctx context.Context, nodeId string, ph packetHead, payload any) error {
	if nodeId == s.nodeId() {
		return s.sendLocalPacket(ctx, ph, payload)
	} else {
		return s.sendRemotePacket(ctx, nodeId, ph, payload)
	}
}

// getTimeSystem 获取时间系统.
func (s *Service) getTimeSystem() TimeSystem {
	return s.cfg.Handler.GetTimeSystem()
}
