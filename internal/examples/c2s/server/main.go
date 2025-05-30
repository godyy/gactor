package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/godyy/gactor"
	"github.com/godyy/gactor/internal/examples/c2s/common"
	"github.com/godyy/gactor/internal/examples/c2s/common/consts"
	"github.com/godyy/gactor/internal/examples/c2s/common/logger"
	"github.com/godyy/gactor/internal/examples/c2s/common/message"
	"github.com/godyy/gactor/internal/examples/c2s/server/actors"
	"github.com/godyy/gactor/internal/examples/c2s/server/actors/define"
	"github.com/godyy/gcluster"
	"github.com/godyy/gcluster/net"
	"github.com/godyy/gtimewheel"
	"go.uber.org/zap"
)

type server struct {
	*common.MetaDriver
	agent   *gcluster.Agent
	svc     *gactor.Service
	timeSys gactor.TimeSystem
}

func (s *server) start() error {
	if err := s.svc.Start(); err != nil {
		return err
	}

	if err := s.agent.Start(); err != nil {
		return err
	}

	return nil
}

func (s *server) stop() error {
	if err := s.svc.Stop(); err != nil {
		return err
	}

	if err := s.agent.Close(); err != nil {
		return err
	}

	return nil
}

func (s *server) OnNodePacket(remoteNodeId string, p *net.RawPacket) error {
	return s.svc.HandlePacket(remoteNodeId, p)
}

func (s *server) GetMetaDriver() gactor.MetaDriver {
	return s.MetaDriver
}

func (s *server) GetNetAgent() gactor.NetAgent {
	return s
}

func (s *server) GetPacketCodec() gactor.PacketCodec {
	return s
}

func (s *server) GetTimeSystem() gactor.TimeSystem {
	return s.timeSys
}

func (s *server) GetMonitor() gactor.ServiceMonitor {
	return nil
}

// NodeId 返回本地节点ID.
func (s *server) NodeId() string {
	return s.agent.NodeId()
}

// SendPacket 发送数据包 p 到 nodeId 指定的节点.
func (s *server) SendPacket(ctx context.Context, nodeId string, p gactor.Packet) error {
	return s.agent.Send2Node(ctx, nodeId, p.(*net.RawPacket))
}

// GetPacket 获取容量为 size 的数据包.
func (s *server) GetPacket(size int) gactor.Packet {
	return net.NewRawPacketWithCap(size)
}

// PutPacket 回收数据包.
func (s *server) PutPacket(p gactor.Packet) {
}

// EncodePacket 编码数据包.
// allocator 提供了获取数据包类型和分配数据包实体的功能.
// 根据数据包类型编码 payload, 然后调用 allocator 分配
// 数据包实体, 将编码后的 payload 数据写入数据包中.
// 数据包类型包括:
//
//	PacketTypeRawResp, PacketTypeRawPush
//	PacketTypeS2SRpc, PacketTypeS2SRpcResp, PacketTypeS2SCast
func (s *server) Encode(allocator gactor.PacketAllocator, payload any) (gactor.Packet, error) {
	switch allocator.PacketType() {
	case gactor.PacketTypeRawReq:
		return payload.(*message.ReqMessage).EncodePacket(allocator)
	case gactor.PacketTypeRawResp:
		return payload.(*message.RespMessage).EncodePacket(allocator)
	case gactor.PacketTypeRawPush:
		return payload.(*message.PushMessage).EncodePacket(allocator)
	case gactor.PacketTypeS2SRpc:
		return payload.(*message.RpcMessage).EncodePacket(allocator)
	case gactor.PacketTypeS2SRpcResp:
		return payload.(*message.RpcRespMessage).EncodePacket(allocator)
	default:
		panic("not implemented") // TODO: Implement
	}
}

// EncodePayload 编码负载数据.
// 根据数据包类型 pt 编码 payload 并生成数据包返回.
// 数据包类型包括:
//
//	PacketTypeS2SRpc, PacketTypeS2SCast
func (s *server) EncodePayload(pt gactor.PacketType, payload any) (gactor.Packet, error) {
	switch pt {
	case gactor.PacketTypeRawReq:
		return payload.(*message.ReqMessage).Encode()
	case gactor.PacketTypeRawResp:
		return payload.(*message.RespMessage).Encode()
	case gactor.PacketTypeRawPush:
		return payload.(*message.PushMessage).Encode()
	case gactor.PacketTypeS2SRpc:
		return payload.(*message.RpcMessage).Encode()
	case gactor.PacketTypeS2SRpcResp:
		return payload.(*message.RpcRespMessage).Encode()
	default:
		panic("not implemented") // TODO: Implement
	}
}

// DecodePayload 解码负载数据.
// 根据数据包类型 pt 解码 p 中负载数据并生成负载数据对象.
// 数据包类型包括:
//
//	PacketTypeRawReq
//	PacketTypeS2SRpc, PacketTypeS2SRpcResp, PacketTypeS2SCast
//
// 返回 ErrPacketEscape, 系统内部将不再自动回收数据包 p.
func (s *server) DecodePayload(pt gactor.PacketType, p gactor.Packet, v any) error {
	switch pt {
	case gactor.PacketTypeRawReq:
		return v.(*message.ReqMessage).Decode(p)
	case gactor.PacketTypeRawResp:
		return v.(*message.RespMessage).Decode(p)
	case gactor.PacketTypeRawPush:
		return v.(*message.PushMessage).Decode(p)
	case gactor.PacketTypeS2SRpc:
		return v.(*message.RpcMessage).Decode(p)
	case gactor.PacketTypeS2SRpcResp:
		return v.(*message.RpcRespMessage).Decode(p)
	default:
		panic("not implemented") // TODO: Implement
	}
}

func main() {
	var clientInfos []string
	flag.Func("client-info", "client info", func(s string) error {
		clientInfos = append(clientInfos, s)
		return nil
	})
	flag.Parse()

	if err := logger.Init(); err != nil {
		panic(err)
	}

	metaDriver := common.NewMetaDriver()
	metaDriver.AddMeta(gactor.ActorUID{consts.CategoryServer, actors.ServerId}, &gactor.Meta{
		Category:   consts.CategoryServer,
		ID:         actors.ServerId,
		Deployment: gactor.NewDeploymentOnNode(consts.ServerNodeId),
	})
	metaDriver.AddMeta(gactor.ActorUID{consts.CategoryUser, 1}, &gactor.Meta{
		Category:   consts.CategoryUser,
		ID:         1,
		Deployment: gactor.NewDeploymentOnNode(consts.ServerNodeId),
	})

	s := &server{
		MetaDriver: metaDriver,
		timeSys:    gactor.DefTimeSystem(),
	}

	center := common.NewCenter()
	center.AddNode(&common.NodeInfo{NodeId: consts.ServerNodeId, Addr: consts.ServerAddr})
	if len(clientInfos) == 0 {
		logger.Logger().Fatal("client-info is required")
	}
	for _, clientInfo := range clientInfos {
		parts := strings.Split(clientInfo, ",")
		if len(parts) != 2 {
			logger.Logger().Fatal("invalid client-info", zap.String("client-info", clientInfo))
		}
		center.AddNode(&common.NodeInfo{NodeId: parts[0], Addr: parts[1]})
	}

	netServiceConfig := &net.ServiceConfig{
		NodeId: consts.ServerNodeId,
		Addr:   consts.ServerAddr,
		Handshake: net.HandshakeConfig{
			Token:   common.HandshakeToken,
			Timeout: common.HandshakeTimeout,
		},
		Session: net.SessionConfig{
			PendingPacketQueueSize: common.PendingPacketQueueSize,
			MaxPacketLength:        common.MaxPacketLength,
			ReadBufSize:            common.ReadBufSize,
			WriteBufSize:           common.WriteBufSize,
			ReadWriteTimeout:       common.ReadWriteTimeout,
			HeartbeatInterval:      common.HeartbeatInterval,
			InactiveTimeout:        common.InactiveTimeout,
		},
		Dialer:          common.Dialer,
		ListenerCreator: common.CreateListener,
	}

	if agent, err := gcluster.CreateService(&gcluster.ServiceConfig{
		Center:  center,
		Net:     netServiceConfig,
		Handler: s,
	}, gcluster.WithLogger(logger.Logger())); err != nil {
		logger.Logger().FatalFields("create gcluster.Service failed", zap.Error(err))
	} else {
		s.agent = agent
	}

	s.svc = gactor.NewService(&gactor.ServiceConfig{
		ActorDefines: define.Defines,
		TimeWheelLevels: []gtimewheel.LevelConfig{
			{Name: "100ms", Span: 100 * time.Millisecond, Slots: 10},
			{Name: "s", Span: 1 * time.Second, Slots: 60},
			{Name: "m", Span: 1 * time.Minute, Slots: 60},
			{Name: "hour", Span: 1 * time.Hour, Slots: 24},
		},
		DefRPCTimeout: 5 * time.Second,
		Handler:       s,
	}, gactor.WithServiceLogger(logger.Logger()))

	if err := s.start(); err != nil {
		logger.Logger().FatalFields("start failed", zap.Error(err))
	}

	cSignal := make(chan os.Signal, 1)
	signal.Notify(cSignal, syscall.SIGINT, syscall.SIGTERM)
	<-cSignal

	if err := s.stop(); err != nil {
		logger.Logger().ErrorFields("stop failed", zap.Error(err))
	}
}
