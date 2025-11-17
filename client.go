package gactor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/godyy/gactor/internal/utils"
	"github.com/godyy/glog"

	pkgerrors "github.com/pkg/errors"
)

// ClientRequest 客户端请求.
type ClientRequest struct {
	ID      int64         // 目标 Actor ID.
	SID     uint32        // 会话ID.
	Timeout time.Duration // 超时. 精度为毫秒, 向下取证.
	Payload []byte        // 负载数据.
}

// ClientResponse 客户端响应.
// PS: 需自行回收 Payload 中的字节切片.
type ClientResponse struct {
	ID      int64  // 来源 Actor ID.
	SID     uint32 // 会话ID.
	Payload Buffer // 负载数据.
	Err     error  // 错误.
}

// ClientPush 客户端推送.
// PS: 需自行回收 Payload 中的字节切片.
type ClientPush struct {
	ID      int64  // 来源 Actor ID.
	SID     uint32 // 会话ID.
	Payload Buffer // 负载数据.
}

// ClientHandler Client 处理器.
type ClientHandler interface {
	// GetMetaDriver 获取 Meta 数据驱动.
	GetMetaDriver() MetaDriver

	// GetNetAgent 获取网络代理.
	GetNetAgent() NetAgent

	// GetBytesManager 获取字节切片管理器.
	GetBytesManager() BytesManager

	// HandleResponse 处理 ClientResponse.
	HandleResponse(resp ClientResponse)

	// HandlePush 处理 ClientPush.
	HandlePush(push ClientPush)

	// HandleDisconnect 处理 Actor 断开连接.
	HandleDisconnect(id int64, sid uint32)
}

// ClientConfig Client 配置.
type ClientConfig struct {
	// ActorCategory 客户端与之通信的默认目标actor分类.
	// 一般情况下, 用户只会与同一分类的actor通信. 例如Player.
	// PS: 其值必须大于0.
	ActorCategory uint16

	// DefCtxTimeout 默认上下文超时时间.
	// 默认值 5s.
	DefCtxTimeout time.Duration

	// DefRequestTimeout 默认请求超时时间.
	// 默认值 5s.
	DefRequestTimeout time.Duration

	// Handler 处理器.
	Handler ClientHandler
}

func (c *ClientConfig) init() {
	if c.ActorCategory == 0 {
		panic("gactor: ClientConfig: ActorCategory must > 0")
	}

	if c.DefCtxTimeout <= 0 {
		c.DefCtxTimeout = 5 * time.Second
	}

	if c.DefRequestTimeout <= 0 {
		c.DefRequestTimeout = 5 * time.Second
	}

	if c.Handler == nil {
		panic("gactor: ClientConfig: Handler not specified")
	}

	if c.Handler.GetMetaDriver() == nil {
		panic("gactor: ClientConfig: Handler has no MetaDriver")
	}

	if c.Handler.GetNetAgent() == nil {
		panic("gactor: ClientConfig: Handler has no NetAgent")
	}

	if c.Handler.GetBytesManager() == nil {
		panic("gactor: ClientConfig: Handler has no BytesManager")
	}
}

// ErrClientStopped Client 已停机.
var ErrClientStopped = errors.New("gactor: client stopped")

// Client 代理用户向 Service 发送请求, 接收 Service 的响应数据并通知用户.
type Client struct {
	cfg         *ClientConfig // 配置.
	sidIncr     uint32        // 会话ID自增键.
	seqIncr     uint32        // 序号自增键.
	*ackManager               // Ack 管理器.
	logger      glog.Logger   // 日志工具.

	mtx      sync.RWMutex    // 读写锁.
	stopped  bool            // 是否已停机.
	stopWait *utils.StopWait // 停机工具.
}

func NewClient(cfg *ClientConfig, option ...ClientOption) *Client {
	cfg.init()

	c := &Client{
		cfg:      cfg,
		stopWait: utils.NewStopWait(),
	}

	for _, o := range option {
		o(c)
	}

	c.initLogger()
	c.start()

	return c
}

// lockRunning 锁定运行状态.
func (c *Client) lockRunning(read bool) error {
	if read {
		c.mtx.RLock()
	} else {
		c.mtx.Lock()
	}

	if !c.stopped {
		return nil
	}

	if read {
		c.mtx.RUnlock()
	} else {
		c.mtx.Unlock()
	}

	return ErrClientStopped
}

// unlock 解锁状态.
func (c *Client) unlockState(read bool) {
	if read {
		c.mtx.RUnlock()
	} else {
		c.mtx.Unlock()
	}
}

// nodeId 获取节点ID.
func (c *Client) nodeId() string {
	return c.cfg.Handler.GetNetAgent().NodeId()
}

// initLogger 初始化日志工具.
func (c *Client) initLogger() {
	if c.logger == nil {
		c.logger = createStdLogger(glog.DebugLevel).Named("Client").WithFields(lfdNodeId(c.nodeId()))
	}
}

// setLogger 设置日志工具.
func (c *Client) setLogger(logger glog.Logger) {
	c.logger = logger.Named("Client").WithFields(lfdNodeId(c.nodeId()))
}

// getLogger 获取日志工具.
func (c *Client) getLogger() glog.Logger {
	return c.logger
}

func (c *Client) getStopWait() *utils.StopWait {
	return c.stopWait
}

// getBytes 获取容量为 cap 的字节切片.
func (c *Client) getBytes(cap int) []byte {
	return c.cfg.Handler.GetBytesManager().GetBytes(cap)
}

// putBytes 回收字节切片.
func (c *Client) putBytes(b []byte) {
	c.cfg.Handler.GetBytesManager().PutBytes(b)
}

// allocBuffer 分配 Buffer.
func (c *Client) allocBuffer(buf *Buffer, size int) {
	b := c.cfg.Handler.GetBytesManager().GetBytes(size)
	buf.SetBuf(b)
}

// freeBuffer 释放 Buffer.
func (c *Client) freeBuffer(buf *Buffer) {
	if b := buf.Data(); b != nil {
		c.cfg.Handler.GetBytesManager().PutBytes(b)
		buf.SetBuf(nil)
	}
}

// genSeq 生成序号.
func (c *Client) genSeq() uint32 {
	return atomic.AddUint32(&c.seqIncr, 1)
}

// encodePacket 编码数据包. payload 为已编码的自定义负载数据.
func (c *Client) encodePacket(ph packetHead, payload []byte) ([]byte, error) {
	// 分配缓冲区.
	var buf Buffer
	c.allocBuffer(&buf, sizeOfPacketType+ph.getSize()+len(payload))

	// 编码填充数据.
	if err := buf.writePacketType(ph.getPt()); err != nil {
		return nil, err
	}
	if err := ph.encode(&buf); err != nil {
		return nil, err
	}
	if len(payload) > 0 {
		if _, err := buf.Write(payload); err != nil {
			return nil, pkgerrors.WithMessage(err, "write payload")
		}
	}

	return buf.Data(), nil
}

// send 发送字节数据.
func (c *Client) send(ctx context.Context, nodeId string, b []byte) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.cfg.DefCtxTimeout)
		defer cancel()
	}
	return c.cfg.Handler.GetNetAgent().Send(ctx, nodeId, b)
}

// sendPacket 编码数据包并发送到 nodeId 指定的节点. payload 为已编码的自定义负载数据.
func (c *Client) sendPacket(ctx context.Context, nodeId string, ph packetHead, payload []byte) error {
	// 编码数据.
	b, err := c.encodePacket(ph, payload)
	if err != nil {
		return pkgerrors.WithMessage(err, "encode packet")
	}

	// 添加待确认数据包.
	c.addPacket2Ack(nodeId, ph.getPt(), ph.getSeq(), b)

	// 发送数据.
	if err = c.send(ctx, nodeId, b); err != nil {
		// 若发送失败, 直接移除待确认数据包.
		c.remPacket2Ack(ph.getPt(), ph.getSeq())
		return pkgerrors.WithMessage(err, "send packet")
	}

	return nil
}

// getNodeIdOfActor 获取 Actor 所在的节点ID.
func (c *Client) getNodeIdOfActor(id int64) (string, error) {
	uid := c.makeActorUID(id)
	return getNodeIdOfActor(c.cfg.Handler.GetMetaDriver(), uid)
}

// start 内部启动逻辑.
func (c *Client) start() {
	c.startAckManager()
}

// Stop 停机.
func (c *Client) Stop() {
	if err := c.lockRunning(false); err != nil {
		return
	}
	c.stopped = true
	c.unlockState(false)

	c.stopWait.Stop(true)
}

// GenSessionId 生成会话ID.
func (c *Client) GenSessionId() uint32 {
	return atomic.AddUint32(&c.sidIncr, 1)
}

// makeActorUID 构造Actor唯一ID.
func (c *Client) makeActorUID(id int64) ActorUID {
	return ActorUID{
		Category: c.cfg.ActorCategory,
		ID:       id,
	}
}

// Connnect 连接 uid 指定的 Actor.
func (c *Client) Connect(ctx context.Context, id int64, sid uint32) error {
	// 获取目标节点.
	nodeId, err := c.getNodeIdOfActor(id)
	if err != nil {
		return err
	}

	// 锁定状态.
	if err := c.lockRunning(true); err != nil {
		return err
	}
	defer c.unlockState(true)

	// 优先检查 ctx 是否done.
	if err := ctx.Err(); err != nil {
		return err
	}

	// 编码并发送消息.
	ph := connectPacketHead{
		id:  id,
		sid: sid,
	}
	return c.sendPacket(ctx, nodeId, &ph, nil)
}

// Disconnect 通知 uid 指定的 Actor 断开连接.
func (c *Client) Disconnect(ctx context.Context, id int64, sid uint32) error {
	// 获取目标节点.
	nodeId, err := c.getNodeIdOfActor(id)
	if err != nil {
		return err
	}

	// 锁定状态.
	if err := c.lockRunning(true); err != nil {
		return err
	}
	defer c.unlockState(true)

	// 优先检查 ctx 是否done.
	if err := ctx.Err(); err != nil {
		return err
	}

	// 编码并发送消息.
	ph := disconnectPacketHead{
		seq: c.genSeq(),
		id:  id,
		sid: sid,
	}
	return c.sendPacket(ctx, nodeId, &ph, nil)
}

// SendRequest 发送请求.
// ctx 只协同到请求的发送, 请求超时独立设置.
func (c *Client) SendRequest(ctx context.Context, req ClientRequest) error {
	// 若未设置超时, 采用默认值
	if req.Timeout <= 0 {
		req.Timeout = c.cfg.DefRequestTimeout
	}

	// 获取目标节点.
	nodeId, err := c.getNodeIdOfActor(req.ID)
	if err != nil {
		return err
	}

	// 锁定状态.
	if err := c.lockRunning(true); err != nil {
		return err
	}
	defer c.unlockState(true)

	// 优先检查 ctx 是否done.
	if err := ctx.Err(); err != nil {
		return err
	}

	// 编码并发送消息.
	ph := rawReqPacketHead{
		seq:     c.genSeq(),
		toId:    req.ID,
		sid:     req.SID,
		timeout: uint32(req.Timeout.Milliseconds()),
	}
	return c.sendPacket(ctx, nodeId, &ph, req.Payload)
}

// handleResponse 处理响应.
func (c *Client) handleResponse(resp ClientResponse) {
	c.cfg.Handler.HandleResponse(resp)
}

// handlePush 处理推送.
func (c *Client) handlePush(push ClientPush) {
	c.cfg.Handler.HandlePush(push)
}

// handleDisconnect 处理断开连接.
func (c *Client) handleDisconnect(id int64, sid uint32) {
	c.cfg.Handler.HandleDisconnect(id, sid)
}
