package gactor

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/godyy/glog"

	pkgerrors "github.com/pkg/errors"
)

// ClientRequest 客户端请求.
type ClientRequest struct {
	UID     ActorUID // 目标 Actor UID.
	SID     uint32   // 会话ID.
	Payload []byte   // 负载数据.
}

// ClientResponse 客户端响应.
// * 需自行回收负载数据包.
type ClientResponse struct {
	UID     ActorUID // 来源 Actor UID.
	SID     uint32   // 会话ID.
	Payload Packet   // 负载数据.
	Err     error    // 错误.
}

// ClientPush 客户端推送.
// * 需自行回收负载数据包.
type ClientPush struct {
	UID     ActorUID // 来源 Actor UID.
	SID     uint32   // 会话ID.
	Payload Packet   // 负载数据.
}

// ClientHandler Client 处理器.
type ClientHandler interface {
	// GetMetaDriver 获取 Meta 数据驱动.
	GetMetaDriver() MetaDriver

	// GetNetAgent 获取网络代理.
	GetNetAgent() NetAgent

	// GetPacketManager 获取数据包管理器.
	GetPacketManager() PacketManager

	// HandleResponse 处理 ClientResponse.
	HandleResponse(resp ClientResponse)

	// HandlePush 处理 ClientPush.
	HandlePush(push ClientPush)

	// HandleDisconnect 处理 Actor 断开连接.
	HandleDisconnect(uid ActorUID, sid uint32)
}

// ClientConfig Client 配置.
type ClientConfig struct {
	// Handler 处理器.
	Handler ClientHandler
}

func (c *ClientConfig) init() {
	if c.Handler == nil {
		panic("gactor: ClientConfig: Handler not specified")
	}

	if c.Handler.GetMetaDriver() == nil {
		panic("gactor: ClientConfig: Handler has no MetaDriver")
	}

	if c.Handler.GetNetAgent() == nil {
		panic("gactor: ClientConfig: Handler has no NetAgent")
	}

	if c.Handler.GetPacketManager() == nil {
		panic("gactor: ClientConfig: Handler has no PacketManager")
	}
}

// Client 代理用户向 Service 发送请求, 接收 Service 的响应数据并通知用户.
type Client struct {
	cfg     *ClientConfig // 配置.
	sidIncr uint32        // 会话ID自增键.
	logger  glog.Logger   // 日志工具.
}

func NewClient(cfg *ClientConfig, option ...ClientOption) *Client {
	cfg.init()

	c := &Client{
		cfg: cfg,
	}

	for _, o := range option {
		o(c)
	}

	c.initLogger()

	return c
}

// nodeId 获取节点ID.
func (c *Client) nodeId() string {
	return c.cfg.Handler.GetNetAgent().NodeId()
}

// initLogger 初始化日志工具.
func (c *Client) initLogger() {
	c.logger = createStdLogger(glog.DebugLevel).Named("Client").WithFields(lfdNodeId(c.nodeId()))
}

// setLogger 设置日志工具.
func (c *Client) setLogger(logger glog.Logger) {
	c.logger = logger.Named("Client").WithFields(lfdNodeId(c.nodeId()))
}

// getLogger 获取日志工具.
func (c *Client) getLogger() glog.Logger {
	return c.logger
}

// getPacket 获取数据包.
func (c *Client) getPacket(size int) Packet {
	return c.cfg.Handler.GetPacketManager().GetPacket(size)
}

// putPacket 回收数据包.
func (c *Client) putPacket(p Packet) {
	c.cfg.Handler.GetPacketManager().PutPacket(p)
}

// encodePacket 编码数据包. payload 为已编码的自定义负载数据.
func (c *Client) encodePacket(ph packetHead, payload []byte) (Packet, error) {
	p := c.getPacket(sizeOfPacketType + ph.size() + len(payload))
	if err := packetIOHelper.writePacketType(p, ph.pt()); err != nil {
		return nil, err
	}
	if err := ph.encode(p); err != nil {
		return nil, err
	}
	if len(payload) > 0 {
		if _, err := p.Write(payload); err != nil {
			return nil, pkgerrors.WithMessage(err, "write payload")
		}
	}
	return p, nil
}

// sendPacket 编码数据包并发送到 nodeId 指定的节点. payload 为已编码的自定义负载数据.
func (c *Client) sendPacket(ctx context.Context, nodeId string, ph packetHead, payload []byte) error {
	if p, err := c.encodePacket(ph, payload); err != nil {
		return pkgerrors.WithMessage(err, "encode packet")
	} else if err = c.cfg.Handler.GetNetAgent().SendPacket(ctx, nodeId, p); err != nil {
		return pkgerrors.WithMessage(err, "send packet")
	} else {
		return nil
	}
}

// getNodeIdOfActor 获取 Actor 所在的节点ID.
func (c *Client) getNodeIdOfActor(uid ActorUID) (string, error) {
	return getNodeIdOfActor(c.cfg.Handler.GetMetaDriver(), uid)
}

// GenSessionId 生成会话ID.
func (c *Client) GenSessionId() uint32 {
	return atomic.AddUint32(&c.sidIncr, 1)
}

// SendRequest 发送请求.
func (c *Client) SendRequest(ctx context.Context, req ClientRequest) error {
	nodeId, err := c.getNodeIdOfActor(req.UID)
	if err != nil {
		return err
	}

	// 优先检查 ctx 是否done.
	if err := ctx.Err(); err != nil {
		return err
	}

	// 获取deadline.
	deadline, ok := ctx.Deadline()
	if !ok {
		return errors.New("context deadline not set")
	}

	// 编码并发送消息.
	ph := rawReqPacketHead{
		toId:    req.UID,
		sid:     req.SID,
		timeout: uint32(time.Until(deadline).Milliseconds()),
	}
	return c.sendPacket(ctx, nodeId, &ph, req.Payload)
}

// NotifyDisconnect 通知 uid 指定的 Actor 断开连接. 数据包类型为 PacketTypeS2SDisconnected.
func (c *Client) NotifyDisconnect(ctx context.Context, uid ActorUID, sid uint32) error {
	nodeId, err := c.getNodeIdOfActor(uid)
	if err != nil {
		return err
	}

	// 优先检查 ctx 是否done.
	if err := ctx.Err(); err != nil {
		return err
	}

	// 编码并发送消息.
	ph := s2sDisconnectedPacketHead{uid: uid, sid: sid}
	return c.sendPacket(ctx, nodeId, &ph, nil)
}

func (c *Client) handleResponse(resp ClientResponse) {
	c.cfg.Handler.HandleResponse(resp)
}

func (c *Client) handlePush(push ClientPush) {
	c.cfg.Handler.HandlePush(push)
}

func (c *Client) handleDisconnect(uid ActorUID, sid uint32) {
	c.cfg.Handler.HandleDisconnect(uid, sid)
}
