package main

import (
	"context"
	"errors"
	"math/rand"
	stdnet "net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/godyy/gactor"
	"github.com/godyy/gactor/internal/examples/s2s/actors"
	"github.com/godyy/gactor/internal/examples/s2s/handlers/server"
	"github.com/godyy/gactor/internal/examples/s2s/handlers/user"
	"github.com/godyy/gactor/internal/examples/s2s/logger"
	"github.com/godyy/gactor/internal/examples/s2s/message"
	"github.com/godyy/gcluster"
	"github.com/godyy/gcluster/center"
	"github.com/godyy/gcluster/net"
	"github.com/godyy/gtimewheel"
	pkgerrors "github.com/pkg/errors"
	"github.com/rs/xid"
)

const (
	s1NodeId = "s1"
	s2NodeId = "s2"
	s1Addr   = "localhost:10001"
	s2Addr   = "localhost:10002"

	// actor
	userMessageBoxSize      = 10
	serverMessageBoxSize    = 10000
	userAmount              = 10000
	maxAckPacketAmount      = 10000
	maxTriggeredTimerAmount = 10000
	maxRPCCallAmount        = 10000

	// session
	pendingPacketQueueSize = 20000
)

var (
	actorDefines = []gactor.ActorDefine{
		gactor.NewCActorDefine(gactor.CActorDefineConfig{
			Name:           "user",
			Category:       actors.CategoryUser,
			Priority:       10,
			MessageBoxSize: userMessageBoxSize,
			RecycleTime:    1 * time.Hour,
			BehaviorCreator: func(actor gactor.CActor) gactor.CActorBehavior {
				return actors.NewUser(actor)
			},
		},
			gactor.WithMaxAsyncRPCAmount(1),
		),
		gactor.NewActorDefine(gactor.ActorDefineConfig{
			Name:           "server",
			Category:       actors.CategoryServer,
			Priority:       1,
			MessageBoxSize: serverMessageBoxSize,
			BehaviorCreator: func(actor gactor.Actor) gactor.ActorBehavior {
				return actors.NewServer(actor)
			},
		}),
	}

	s1, s2 *service

	userIds []gactor.ActorUID
	nodeMap map[gactor.ActorUID]string
)

type s2sMeta struct {
	uid    gactor.ActorUID
	nodeId string
}

func (m *s2sMeta) GetActorUID() gactor.ActorUID {
	return m.uid
}

func (m *s2sMeta) GetNodeId() string {
	return m.nodeId
}

func main() {
	rand.Seed(time.Now().UnixNano())

	if err := logger.Init(); err != nil {
		panic(pkgerrors.WithMessage(err, "init logger"))
	}

	nodeMap = make(map[gactor.ActorUID]string)
	nodeMap[gactor.ActorUID{
		Category: actors.CategoryServer,
		ID:       1,
	}] = s2NodeId
	userIds = make([]gactor.ActorUID, userAmount)
	for i := 0; i < userAmount; i++ {
		uid := gactor.ActorUID{
			Category: actors.CategoryUser,
			ID:       int64(i),
		}
		nodeMap[uid] = s1NodeId
		userIds[i] = uid
	}

	center := &nodeCenter{
		nodes: map[string]*nodeInfo{
			s1NodeId: {
				nodeId: s1NodeId,
				addr:   s1Addr,
			},
			s2NodeId: {
				nodeId: s2NodeId,
				addr:   s2Addr,
			},
		},
	}

	actorRegistry := &actorRegistry{actorMap: make(map[gactor.ActorUID]*actorLocation)}
	actorRouter := &actorRouter{nodes: nodeMap}

	handshakeConfig := net.HandshakeConfig{
		Token:   "123",
		Timeout: 5 * time.Second,
	}
	sessionConfig := net.SessionConfig{
		PendingPacketQueueSize: pendingPacketQueueSize,
		MaxPacketLength:        64 * 1024,
		ReadBufSize:            128 * 1024,
		WriteBufSize:           128 * 1024,
		ReadWriteTimeout:       5 * time.Second,
		HeartbeatTimeout:       2 * time.Second,
		InactiveTimeout:        60 * time.Second,
	}
	dialer := func(addr string) (stdnet.Conn, error) {
		return stdnet.Dial("tcp", addr)
	}
	createListener := func(addr string) (stdnet.Listener, error) {
		return stdnet.Listen("tcp", addr)
	}

	var ackConfig *gactor.AckConfig
	ackConfig = &gactor.AckConfig{
		MaxPacketAmount: maxAckPacketAmount,
		Timeout:         500 * time.Millisecond,
		MaxRetry:        1,
		TickInterval:    50 * time.Millisecond,
	}

	handler := func(ctx *gactor.Context) {
		switch ctx.Actor().Category() {
		case actors.CategoryUser:
			user.Handler(ctx)
		case actors.CategoryServer:
			server.Handler()(ctx)
		}
	}

	s1 = &service{
		actorRegistry: actorRegistry,
		actorRouter:   actorRouter,
		packetCodec:   &packetCodec{},
		TimeSystem:    gactor.DefTimeSystem,
	}
	if agent, err := gcluster.CreateAgent(&gcluster.AgentConfig{
		Center: center,
		Net: &net.ServiceConfig{
			NodeId:          s1NodeId,
			Addr:            s1Addr,
			Handshake:       handshakeConfig,
			Session:         sessionConfig,
			Dialer:          dialer,
			ListenerCreator: createListener,
			TimerSystem:     net.NewTimerHeap(),
		},
		Handler: s1,
	}, gcluster.WithLogger(logger.Logger())); err != nil {
		panic(pkgerrors.WithMessage(err, "create service 1 cluster agent"))
	} else {
		s1.netAgent = &netAgent{agent}
	}
	s1.Service = gactor.NewService(&gactor.ServiceConfig{
		NodeId: s1NodeId,
		ActorConfig: gactor.ActorConfig{
			ActorDefines: actorDefines,
			Handler:      handler,
		},
		TimerConfig: gactor.TimerConfig{
			TimeWheelLevels: []gtimewheel.LevelConfig{
				{Name: "100ms", Span: 100 * time.Millisecond, Slots: 10},
				{Name: "s", Span: 1 * time.Second, Slots: 60},
				{Name: "m", Span: 1 * time.Minute, Slots: 60},
				{Name: "hour", Span: 1 * time.Hour, Slots: 24},
			},
			MaxTimerAmount: maxTriggeredTimerAmount,
		},
		RPCConfig: gactor.RPCConfig{
			DefRPCTimeout:    0,
			MaxRPCCallAmount: maxRPCCallAmount,
		},

		MaxRTT:  0,
		Handler: s1,
	}, gactor.WithServiceLogger(logger.Logger()), gactor.WithServiceAckManager(ackConfig))
	if err := s1.Service.Start(); err != nil {
		panic(pkgerrors.WithMessage(err, "start service 1 actor"))
	}
	if err := s1.Agent.Start(); err != nil {
		panic(pkgerrors.WithMessage(err, "start service 1 agent"))
	}

	s2 = &service{
		actorRegistry: actorRegistry,
		actorRouter:   actorRouter,
		packetCodec:   &packetCodec{},
		TimeSystem:    gactor.DefTimeSystem,
	}
	if agent, err := gcluster.CreateAgent(&gcluster.AgentConfig{
		Center: center,
		Net: &net.ServiceConfig{
			NodeId:          s2NodeId,
			Addr:            s2Addr,
			Handshake:       handshakeConfig,
			Session:         sessionConfig,
			Dialer:          dialer,
			ListenerCreator: createListener,
			TimerSystem:     net.NewTimerHeap(),
		},
		Handler: s2,
	}, gcluster.WithLogger(logger.Logger())); err != nil {
		panic(pkgerrors.WithMessage(err, "create service 2 cluster agent"))
	} else {
		s2.netAgent = &netAgent{agent}
	}
	s2.Service = gactor.NewService(&gactor.ServiceConfig{
		NodeId: s2NodeId,
		ActorConfig: gactor.ActorConfig{
			ActorDefines: actorDefines,
			Handler:      handler,
		},
		TimerConfig: gactor.TimerConfig{
			TimeWheelLevels: []gtimewheel.LevelConfig{
				{Name: "100ms", Span: 100 * time.Millisecond, Slots: 10},
				{Name: "s", Span: 1 * time.Second, Slots: 60},
				{Name: "m", Span: 1 * time.Minute, Slots: 60},
				{Name: "hour", Span: 1 * time.Hour, Slots: 24},
			},
		},
		RPCConfig: gactor.RPCConfig{
			DefRPCTimeout:    0,
			MaxRPCCallAmount: 0,
		},
		Handler: s2,
	}, gactor.WithServiceLogger(logger.Logger()), gactor.WithServiceAckManager(ackConfig))
	if err := s2.Service.Start(); err != nil {
		panic(pkgerrors.WithMessage(err, "start service 2 actor"))
	}
	if err := s2.Agent.Start(); err != nil {
		panic(pkgerrors.WithMessage(err, "start service 2 agent"))
	}

	if err := s2.StartActor(context.Background(), gactor.ActorUID{
		Category: actors.CategoryServer,
		ID:       1,
	}); err != nil {
		panic(pkgerrors.WithMessage(err, "start server"))
	}

	for _, uid := range userIds {
		if err := s1.StartActor(context.Background(), uid); err != nil {
			panic(pkgerrors.WithMessagef(err, "start user %s", uid))
		}
	}

	chSignal := make(chan os.Signal, 1)
	signal.Notify(chSignal, syscall.SIGINT, syscall.SIGTERM)
	<-chSignal

	// time.Sleep(10 * time.Second)

	_ = s1.Stop()
	_ = s2.Stop()
}

type nodeInfo struct {
	nodeId string
	addr   string
}

func (n *nodeInfo) GetNodeId() string {
	return n.nodeId
}

func (n *nodeInfo) GetNodeAddr() string {
	return n.addr
}

type nodeCenter struct {
	nodes map[string]*nodeInfo
}

func (c *nodeCenter) GetNode(nodeId string) (center.Node, error) {
	node, ok := c.nodes[nodeId]
	if !ok {
		return nil, pkgerrors.Errorf("node %s not exists", nodeId)
	}
	return node, nil
}

type actorLocation struct {
	nodeId   string
	leaseId  string
	expireAt int64
}

type actorRegistry struct {
	mtx      sync.RWMutex
	actorMap map[gactor.ActorUID]*actorLocation
}

// MakeLeaseID 生成全剧唯一租约ID.
func (r *actorRegistry) MakeLeaseID() string {
	return xid.New().String()
}

// Register 注册 Actor.
// 若 Actor 已注册, 不论是否当前节点注册, 均通过 ActorRegisterResult 返回所在节点和过期
// 时间.
// 若 Actor 已注册, 且所在节点ID与当前节点ID不同, 返回 ErrActorAlreadyRegistered 错误,
// 否则, 使用当前租约覆盖旧租约, 并更新存续时间.
func (r *actorRegistry) RegisterActor(params gactor.ActorRegisterParams) (gactor.ActorRegisterResult, error) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	location := r.actorMap[params.UID]
	if location == nil {
		location = &actorLocation{
			nodeId:  params.NodeId,
			leaseId: params.LeaseId,
		}
		if params.TTL > 0 {
			location.expireAt = time.Now().Unix() + params.TTL
		}
		r.actorMap[params.UID] = location
		return gactor.ActorRegisterResult{
			NodeId:   params.NodeId,
			ExpireAt: location.expireAt,
		}, nil
	}
	if params.NodeId != location.nodeId {
		return gactor.ActorRegisterResult{
			NodeId:   location.nodeId,
			ExpireAt: location.expireAt,
		}, gactor.ErrActorAlreadyRegistered
	}
	location.leaseId = params.LeaseId
	if params.TTL > 0 {
		location.expireAt = time.Now().Unix() + params.TTL
	}
	return gactor.ActorRegisterResult{
		NodeId:   location.nodeId,
		ExpireAt: location.expireAt,
	}, nil
}

// UnregisterActor 注销 Actor.
// 若 Actor 未注册, 返回 ErrActorNotExists 错误.
// 若节点ID和租约ID匹配, 则注销 Actor, 否则返回 ErrLeaseMismatch 错误.
func (r *actorRegistry) UnregisterActor(params gactor.ActorUnregisterParams) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	location := r.actorMap[params.UID]
	if location == nil {
		return gactor.ErrActorNotExists
	}
	if params.NodeId != location.nodeId {
		return gactor.ErrLeaseMismatch
	}
	if params.LeaseId != location.leaseId {
		return gactor.ErrLeaseMismatch
	}
	delete(r.actorMap, params.UID)
	return nil
}

// KeepActorAlive 保持 Actor 存续.
// 若 Actor 未注册, 返回 ErrActorNotExists 错误,
// 否则, 若节点ID和租约ID匹配, 则更新 Actor 存续时间, 否则返回 ErrLeaseMismatch 错误.
func (r *actorRegistry) KeepActorAlive(params gactor.ActorKeepAliveParams) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	location := r.actorMap[params.UID]
	if location == nil {
		return gactor.ErrActorNotExists
	}
	if params.NodeId != location.nodeId {
		return gactor.ErrLeaseMismatch
	}
	if params.LeaseId != location.leaseId {
		return gactor.ErrLeaseMismatch
	}
	location.expireAt = time.Now().Unix() + params.TTL
	return nil
}

// GetActorLocation 获取 Actor 位置信息.
// 若 Actor 未注册, 返回 ErrActorNotExists 错误.
func (r *actorRegistry) GetActorLocation(uid gactor.ActorUID) (gactor.ActorLocation, error) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()
	location := r.actorMap[uid]
	if location == nil {
		return gactor.ActorLocation{}, gactor.ErrActorNotExists
	}
	return gactor.ActorLocation{
		NodeId:   location.nodeId,
		ExpireAt: location.expireAt,
	}, nil
}

type actorRouter struct {
	nodes map[gactor.ActorUID]string
}

// PickActorNode 选择节点.
func (r *actorRouter) PickActorNode(uid gactor.ActorUID) (string, error) {
	nodeId, ok := r.nodes[uid]
	if !ok {
		return "", errors.New("no node")
	}
	return nodeId, nil
}

type netAgent struct {
	*gcluster.Agent
}

func (a *netAgent) Send(nodeId string, b []byte) error {
	return a.Send2Node(nodeId, b)
}

func (a *netAgent) Send2Node(nodeId string, b []byte) error {
	err := a.Agent.Send2Node(nodeId, b)
	if errors.Is(err, net.ErrPendingPacketsFull) {
		err = gactor.ErrNetworkBusy
	}
	return err
}

type packetCodec struct {
}

func (c *packetCodec) GetBytes(size int) []byte {
	return make([]byte, 0, size)
}

func (c *packetCodec) PutBytes(b []byte) {
}

func (c *packetCodec) Encode(allocator gactor.PacketAllocator, payload any) ([]byte, error) {
	return message.EncodePacket(allocator, payload.(*message.Msg))
}

func (c *packetCodec) EncodePayload(pt gactor.PacketType, payload any) ([]byte, error) {
	return message.EncodeMsg(c, payload.(*message.Msg))
}

func (c *packetCodec) DecodePayload(pt gactor.PacketType, b *gactor.Buffer, v any) error {
	return message.DecodeMsgWithoutDecodePayload(b, v.(*message.Msg))
}

type service struct {
	*actorRegistry
	*actorRouter
	*netAgent
	*packetCodec
	*gactor.Service
	gactor.TimeSystem
}

func (s *service) GetActorRegistry() gactor.ActorRegistry {
	return s.actorRegistry
}

func (s *service) GetActorRouter() gactor.ActorRouter {
	return s.actorRouter
}

func (s *service) GetNetAgent() gactor.NetAgent {
	return s.netAgent
}

func (s *service) GetPacketCodec() gactor.PacketCodec {
	return s.packetCodec
}

func (s *service) GetTimeSystem() gactor.TimeSystem {
	return s.TimeSystem
}

func (s *service) GetMonitor() gactor.ServiceMonitor {
	return nil
}

func (s *service) OnNodeBytes(remoteNodeId string, b []byte) error {
	return s.Service.HandlePacket(remoteNodeId, b)
}
