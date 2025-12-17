package main

import (
	"context"
	stdnet "net"
	"os"
	"os/signal"
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
)

const (
	s1NodeId = "s1"
	s2NodeId = "s2"
	s1Addr   = "localhost:10001"
	s2Addr   = "localhost:10002"
)

var (
	actorDefines = []gactor.IActorDefine{
		&gactor.CActorDefine{
			ActorDefineCommon: &gactor.ActorDefineCommon{
				Name:                       "user",
				Category:                   actors.CategoryUser,
				Priority:                   10,
				MessageBoxSize:             10,
				MaxCompletedAsyncRPCAmount: 1,
				RecycleTime:                0,
				Handler:                    user.Handler,
			},
			BehaviorCreator: func(actor gactor.CActor) gactor.CActorBehavior {
				return actors.NewUser(actor)
			},
		},
		&gactor.ActorDefine{
			ActorDefineCommon: &gactor.ActorDefineCommon{
				Name:           "server",
				Category:       actors.CategoryServer,
				Priority:       1,
				MessageBoxSize: 1000,
				RecycleTime:    0,
				Handler:        server.Handler(),
			},
			BehaviorCreator: func(actor gactor.Actor) gactor.ActorBehavior {
				return actors.NewServer(actor)
			},
		},
	}

	s1, s2  *service
	metaMap map[gactor.ActorUID]*s2sMeta
	userIds []gactor.ActorUID
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
	if err := logger.Init(); err != nil {
		panic(pkgerrors.WithMessage(err, "init logger"))
	}

	metaMap = make(map[gactor.ActorUID]*s2sMeta)
	metaMap[gactor.ActorUID{
		Category: actors.CategoryServer,
		ID:       1,
	}] = &s2sMeta{
		uid: gactor.ActorUID{
			Category: actors.CategoryServer,
			ID:       1,
		},
		nodeId: s2NodeId,
	}
	userIds = make([]gactor.ActorUID, 8000)
	for i := 0; i < 8000; i++ {
		uid := gactor.ActorUID{
			Category: actors.CategoryUser,
			ID:       int64(i),
		}
		userIds[i] = uid
		metaMap[uid] = &s2sMeta{
			uid:    uid,
			nodeId: s1NodeId,
		}
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

	handshakeConfig := net.HandshakeConfig{
		Token:   "123",
		Timeout: 5 * time.Second,
	}
	sessionConfig := net.SessionConfig{
		PendingPacketQueueSize: 100,
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
		MaxPacketAmount: 10000,
		Timeout:         100 * time.Millisecond,
		MaxRetry:        1,
		TickInterval:    50 * time.Millisecond,
	}

	s1 = &service{
		metaDriver:  &metaDriver{metaMap},
		packetCodec: &packetCodec{},
		TimeSystem:  gactor.DefTimeSystem,
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
		metaDriver:  &metaDriver{metaMap},
		packetCodec: &packetCodec{},
		TimeSystem:  gactor.DefTimeSystem,
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

type metaDriver struct {
	metaMap map[gactor.ActorUID]*s2sMeta
}

func (d *metaDriver) GetMeta(uid gactor.ActorUID) (gactor.Meta, error) {
	meta, ok := d.metaMap[uid]
	if !ok {
		return nil, gactor.ErrMetaNotExists
	}
	return meta, nil
}

type netAgent struct {
	*gcluster.Agent
}

func (a *netAgent) Send(ctx context.Context, nodeId string, b []byte) error {
	return a.Send2Node(ctx, nodeId, b)
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
	*metaDriver
	*netAgent
	*packetCodec
	*gactor.Service
	gactor.TimeSystem
}

func (s *service) GetMetaDriver() gactor.MetaDriver {
	return s.metaDriver
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
