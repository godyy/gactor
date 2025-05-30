package main

import (
	"context"
	stdnet "net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/godyy/gactor/internal/examples/s2s/handlers/server"
	"github.com/godyy/gactor/internal/examples/s2s/handlers/user"
	"github.com/godyy/gtimewheel"

	"github.com/godyy/gcluster/center"

	pkgerrors "github.com/pkg/errors"

	"github.com/godyy/gactor/internal/examples/s2s/logger"

	"github.com/godyy/gactor/internal/examples/s2s/message"

	"github.com/godyy/gcluster/net"

	"github.com/godyy/gcluster"

	"github.com/godyy/gactor"
	"github.com/godyy/gactor/internal/examples/s2s/actors"
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
	metaMap map[gactor.ActorUID]*gactor.Meta
	userIds []gactor.ActorUID
)

func main() {
	if err := logger.Init(); err != nil {
		panic(pkgerrors.WithMessage(err, "init logger"))
	}

	metaMap = make(map[gactor.ActorUID]*gactor.Meta)
	metaMap[gactor.ActorUID{
		Category: actors.CategoryServer,
		ID:       1,
	}] = &gactor.Meta{
		Category:   actors.CategoryServer,
		ID:         1,
		Deployment: gactor.NewDeploymentOnNode(s2NodeId),
	}
	userIds = make([]gactor.ActorUID, 8000)
	for i := 0; i < 8000; i++ {
		uid := gactor.ActorUID{
			Category: actors.CategoryUser,
			ID:       int64(i),
		}
		userIds[i] = uid
		metaMap[uid] = &gactor.Meta{
			Category:   uid.Category,
			ID:         uid.ID,
			Deployment: gactor.NewDeploymentOnNode(s1NodeId),
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
		HeartbeatInterval:      2 * time.Second,
		InactiveTimeout:        60 * time.Second,
	}
	dialer := func(addr string) (stdnet.Conn, error) {
		return stdnet.Dial("tcp", addr)
	}
	createListener := func(addr string) (stdnet.Listener, error) {
		return stdnet.Listen("tcp", addr)
	}

	s1 = &service{
		metaDriver:  &metaDriver{metaMap},
		packetCodec: &packetCodec{},
		TimeSystem:  gactor.DefTimeSystem(),
	}
	if agent, err := gcluster.CreateService(&gcluster.ServiceConfig{
		Center: center,
		Net: &net.ServiceConfig{
			NodeId:          s1NodeId,
			Addr:            s1Addr,
			Handshake:       handshakeConfig,
			Session:         sessionConfig,
			Dialer:          dialer,
			ListenerCreator: createListener,
		},
		Handler: s1,
	}, gcluster.WithLogger(logger.Logger())); err != nil {
		panic(pkgerrors.WithMessage(err, "create service 1 cluster agent"))
	} else {
		s1.netAgent = &netAgent{agent}
	}
	s1.Service = gactor.NewService(&gactor.ServiceConfig{
		ActorDefines: actorDefines,
		TimeWheelLevels: []gtimewheel.LevelConfig{
			{Name: "100ms", Span: 100 * time.Millisecond, Slots: 10},
			{Name: "s", Span: 1 * time.Second, Slots: 60},
			{Name: "m", Span: 1 * time.Minute, Slots: 60},
			{Name: "hour", Span: 1 * time.Hour, Slots: 24},
		},
		DefRPCTimeout:                        0,
		ActorReceiveCompletedAsyncRPCTimeout: 0,
		MaxCompletedRPCAmount:                0,
		MaxRTT:                               0,
		Handler:                              s1,
	})
	if err := s1.Service.Start(); err != nil {
		panic(pkgerrors.WithMessage(err, "start service 1 actor"))
	}
	if err := s1.Agent.Start(); err != nil {
		panic(pkgerrors.WithMessage(err, "start service 1 agent"))
	}

	s2 = &service{
		metaDriver:  &metaDriver{metaMap},
		packetCodec: &packetCodec{},
		TimeSystem:  gactor.DefTimeSystem(),
	}
	if agent, err := gcluster.CreateService(&gcluster.ServiceConfig{
		Center: center,
		Net: &net.ServiceConfig{
			NodeId:          s2NodeId,
			Addr:            s2Addr,
			Handshake:       handshakeConfig,
			Session:         sessionConfig,
			Dialer:          dialer,
			ListenerCreator: createListener,
		},
		Handler: s2,
	}, gcluster.WithLogger(logger.Logger())); err != nil {
		panic(pkgerrors.WithMessage(err, "create service 2 cluster agent"))
	} else {
		s2.netAgent = &netAgent{agent}
	}
	s2.Service = gactor.NewService(&gactor.ServiceConfig{
		ActorDefines: actorDefines,
		TimeWheelLevels: []gtimewheel.LevelConfig{
			{Name: "100ms", Span: 100 * time.Millisecond, Slots: 10},
			{Name: "s", Span: 1 * time.Second, Slots: 60},
			{Name: "m", Span: 1 * time.Minute, Slots: 60},
			{Name: "hour", Span: 1 * time.Hour, Slots: 24},
		},
		DefRPCTimeout:                        0,
		ActorReceiveCompletedAsyncRPCTimeout: 0,
		MaxCompletedRPCAmount:                0,
		MaxRTT:                               0,
		Handler:                              s2,
	})
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
	metaMap map[gactor.ActorUID]*gactor.Meta
}

func (d *metaDriver) GetMeta(uid gactor.ActorUID) (*gactor.Meta, error) {
	meta, ok := d.metaMap[uid]
	if !ok {
		return nil, gactor.ErrMetaNotExists
	}
	return meta, nil
}

type netAgent struct {
	*gcluster.Agent
}

func (a *netAgent) NodeId() string {
	return a.Agent.NodeId()
}

func (a *netAgent) SendPacket(ctx context.Context, nodeId string, p gactor.Packet) error {
	return a.Send2Node(ctx, nodeId, p.(*net.RawPacket))
}

type packetCodec struct {
}

func (c *packetCodec) GetPacket(size int) gactor.Packet {
	return net.NewRawPacketWithCap(size)
}

func (c *packetCodec) PutPacket(p gactor.Packet) {
}

func (c *packetCodec) Encode(allocator gactor.PacketAllocator, payload any) (gactor.Packet, error) {
	return message.EncodePacket(allocator, payload.(*message.Msg))
}

func (c *packetCodec) EncodePayload(pt gactor.PacketType, payload any) (gactor.Packet, error) {
	return message.EncodeMsg(c, payload.(*message.Msg))
}

func (c *packetCodec) DecodePayload(pt gactor.PacketType, p gactor.Packet, v any) error {
	return message.DecodeMsgWithoutDecodePayload(p, v.(*message.Msg))
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

func (s *service) OnNodePacket(remoteNodeId string, p *net.RawPacket) error {
	return s.Service.HandlePacket(remoteNodeId, p)
}
