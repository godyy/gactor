package main

import (
	"context"
	"errors"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/godyy/gactor"
	"github.com/godyy/gactor/internal/examples/c2s/common"
	"github.com/godyy/gactor/internal/examples/c2s/common/consts"
	"github.com/godyy/gactor/internal/examples/c2s/common/logger"
	"github.com/godyy/gactor/internal/examples/c2s/common/message"
	"github.com/godyy/gcluster"
	"github.com/godyy/gcluster/net"
	"go.uber.org/zap"
)

type client struct {
	*common.MetaDriver
	agent *gcluster.Agent
	cli   *gactor.Client
	users map[int64]*user
}

func (c *client) start() error {
	if err := c.agent.Start(); err != nil {
		return err
	}

	return nil
}

func (c *client) stop() error {
	if err := c.agent.Close(); err != nil {
		return err
	}

	c.cli.Stop()

	return nil
}

func (c *client) getUser(uid int64) *user {
	if u, ok := c.users[uid]; ok {
		return u
	}
	return nil
}

func (c *client) OnNodeBytes(nodeId string, b []byte) error {
	return c.cli.HandlePacket(nodeId, b)
}

func (c *client) GetMetaDriver() gactor.MetaDriver {
	return c.MetaDriver
}

func (c *client) GetNetAgent() gactor.NetAgent {
	return c
}

func (c *client) GetBytesManager() gactor.BytesManager {
	return c
}

// NodeId 返回本地节点ID.
func (c *client) NodeId() string {
	return c.agent.NodeId()
}

// SendBytes 发送字节流到 nodeId 指定的节点.
func (c *client) Send(ctx context.Context, nodeId string, b []byte) error {
	return c.agent.Send2Node(ctx, nodeId, b)
}

// GetBytes 获取容量为 size 的字节流.
func (c *client) GetBytes(size int) []byte {
	return make([]byte, 0, size)
}

// PutBytes 回收字节流.
func (c *client) PutBytes(b []byte) {
}

// HandleResponse 处理 ClientResponse.
func (c *client) HandleResponse(resp gactor.ClientResponse) {
	u := c.getUser(resp.UID.ID)
	if u == nil {
		logger.Logger().ErrorFields("user not found", zap.Int64("id", resp.UID.ID))
		return
	}

	u.receiveMessage(&msgResp{
		sid:     resp.SID,
		payload: resp.Payload,
		err:     resp.Err,
	})
}

// HandlePush 处理 ClientPush.
func (c *client) HandlePush(push gactor.ClientPush) {
	u := c.getUser(push.UID.ID)
	if u == nil {
		logger.Logger().ErrorFields("user not found", zap.Int64("id", push.UID.ID))
		return
	}

	u.receiveMessage(&msgPush{sid: push.SID, payload: push.Payload})
}

// HandleDisconnect 处理 Actor 断开连接.
func (c *client) HandleDisconnect(uid gactor.ActorUID, sid uint32) {
	u := c.getUser(uid.ID)
	if u == nil {
		logger.Logger().ErrorFields("user not found", zap.Int64("id", uid.ID))
		return
	}

	u.receiveMessage(&msgDisconnected{sid: sid})
}

type msg interface {
	handle(u *user)
}

type msgResp struct {
	sid     uint32
	payload gactor.Buffer
	err     error
}

func (m *msgResp) handle(u *user) {
	if m.sid != u.sid {
		logger.Logger().ErrorFields("user receive resp from oth session")
		return
	}

	if m.err != nil {
		logger.Logger().ErrorFields("user receive resp with error", zap.Error(m.err))
		return
	}

	var respMsg message.RespMessage
	if err := respMsg.Decode(&m.payload); err != nil && !errors.Is(err, gactor.ErrBytesEscape) {
		logger.Logger().ErrorFields("user decode resp failed", zap.Error(err))
		return
	}

	u.handleResp(&respMsg)
}

type msgPush struct {
	sid     uint32
	payload gactor.Buffer
}

func (m *msgPush) handle(u *user) {
	if m.sid != u.sid {
		logger.Logger().ErrorFields("user receive push from other session")
		return
	}

	var pushMsg message.PushMessage
	if err := pushMsg.Decode(&m.payload); err != nil && !errors.Is(err, gactor.ErrBytesEscape) {
		logger.Logger().ErrorFields("user decode push failed", zap.Error(err))
		return
	}

	u.handlePush(&pushMsg)
}

type msgDisconnected struct {
	sid uint32
}

func (m *msgDisconnected) handle(u *user) {
	if m.sid != u.sid {
		logger.Logger().ErrorFields("user receive disconnected from other session")
		return
	}

	u.handleDisconnected(m.sid)
}

type user struct {
	uid          int64
	sid          uint32
	userName     string
	password     string
	reqIdIncr    uint32
	cMessage     chan msg
	isLogin      bool
	reloginTimer *time.Timer
}

func newUser(uid int64, userName, password string) *user {
	u := &user{
		uid:          uid,
		userName:     userName,
		password:     password,
		cMessage:     make(chan msg, 10),
		reloginTimer: time.NewTimer(0),
	}

	u.stopReloginTimer()

	return u
}

func (u *user) start() {
	go u.loop()
}

func (u *user) receiveMessage(m msg) {
	u.cMessage <- m
}

func (u *user) genReqId() uint32 {
	u.reqIdIncr++
	return u.reqIdIncr
}

func (u *user) login() error {
	loginReq := message.NewReqMessageWithPayload(u.genReqId(), &message.LoginReq{
		Username: u.userName,
		Password: u.password,
	})
	b, err := loginReq.Encode()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return cli.cli.SendRequest(ctx, gactor.ClientRequest{
		UID:     gactor.ActorUID{consts.CategoryUser, u.uid},
		SID:     u.sid,
		Payload: b,
	})
}

func (u *user) heartbeat() error {
	heartbeatReq := message.NewReqMessageWithPayload(u.genReqId(), &message.HeartbeatReq{
		Ts: time.Now().Unix(),
	})
	b, err := heartbeatReq.Encode()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return cli.cli.SendRequest(ctx, gactor.ClientRequest{
		UID:     gactor.ActorUID{consts.CategoryUser, u.uid},
		SID:     u.sid,
		Payload: b,
	})
}

func (u *user) stopReloginTimer() {
	if !u.reloginTimer.Stop() {
		select {
		case <-u.reloginTimer.C:
		default:
		}
	}
}

func (u *user) startReloginTimer() {
	u.reloginTimer.Reset(10 * time.Second)
}

func (u *user) loop() {
	if err := u.login(); err != nil {
		logger.Logger().ErrorFields("user login failed", zap.Int64("id", u.uid), zap.Error(err))
		return
	}

	ticker := time.NewTicker(5 * time.Second)
	heartbeatCount := 0
	for {
		select {
		case <-ticker.C:
			if !u.isLogin {
				continue
			}
			// if heartbeatCount >= 5 {
			// 	continue
			// }
			if err := u.heartbeat(); err != nil {
				logger.Logger().ErrorFields("user heartbeat failed", zap.Int64("id", u.uid), zap.Error(err))
			}
			heartbeatCount++
		case <-u.reloginTimer.C:
			if err := u.login(); err != nil {
				logger.Logger().ErrorFields("user relogin failed", zap.Int64("id", u.uid), zap.Error(err))
				u.startReloginTimer()
			}
		case msg := <-u.cMessage:
			msg.handle(u)
		}
	}
}

func (u *user) handleResp(resp *message.RespMessage) {
	switch resp.MsgId {
	case message.MsgIdLoginResp:
		var m message.LoginResp
		if err := resp.DecodePayload(&m); err != nil {
			logger.Logger().ErrorFields("user decode login resp failed", zap.Int64("id", u.uid), zap.Error(err))
			return
		}
		if m.Err != "" {
			logger.Logger().ErrorFields("user login failed", zap.Int64("id", u.uid), zap.String("err", m.Err))
			return
		}

		u.isLogin = true
		logger.Logger().InfoFields("user login success", zap.Int64("id", u.uid))

	case message.MsgIdHeartbeatResp:
		var m message.HeartbeatResp
		if err := resp.DecodePayload(&m); err != nil {
			logger.Logger().ErrorFields("user decode heartbeat resp failed", zap.Int64("id", u.uid), zap.Error(err))
			return
		}

		logger.Logger().InfoFields("user heartbeat success", zap.Int64("id", u.uid))

	case message.MsgIdError:
		var m message.Error
		if err := resp.DecodePayload(&m); err != nil {
			logger.Logger().ErrorFields("user decode error failed", zap.Int64("id", u.uid), zap.Error(err))
			return
		}

		logger.Logger().InfoFields("user receive error", zap.Int64("id", u.uid), zap.Error(m.Code))
		if m.Code == common.ErrCodeNotLogin {
			u.isLogin = false
			u.startReloginTimer()
		}

	default:
		logger.Logger().ErrorFields("user receive unknown resp", zap.Int64("id", u.uid), zap.Uint16("msgId", resp.MsgId))
	}

}

func (u *user) handlePush(push *message.PushMessage) {
	switch push.MsgId {
	case message.MsgIdNotify:
		var m message.Notify
		if err := push.DecodePayload(&m); err != nil {
			logger.Logger().ErrorFields("user decode notify failed", zap.Int64("id", u.uid), zap.Error(err))
			return
		}

		logger.Logger().InfoFields("user receive notify", zap.Int64("id", u.uid), zap.String("msg", m.Msg))

	default:
		logger.Logger().ErrorFields("user receive unknown push", zap.Int64("id", u.uid), zap.Uint16("msgId", push.MsgId))
	}
}

func (u *user) handleDisconnected(sid uint32) {
	if sid != u.sid {
		logger.Logger().ErrorFields("user receive disconnected from other session")
		return
	}

	u.isLogin = false
	u.startReloginTimer()
	logger.Logger().InfoFields("user disconnected", zap.Int64("id", u.uid))
}

var (
	cli *client
)

func main() {
	nodeId := flag.String("node-id", "", "node id")
	addr := flag.String("addr", "", "addr")
	flag.Parse()

	if err := logger.Init(); err != nil {
		panic(err)
	}

	metaDriver := common.NewMetaDriver()
	metaDriver.AddMeta(gactor.ActorUID{consts.CategoryUser, 1}, &gactor.Meta{
		Category:   consts.CategoryUser,
		ID:         1,
		Deployment: gactor.NewDeploymentOnNode(consts.ServerNodeId),
	})

	u := newUser(1, "user1", "password1")

	cli = &client{
		MetaDriver: metaDriver,
		users: map[int64]*user{
			u.uid: u,
		},
	}

	center := common.NewCenter()
	center.AddNode(&common.NodeInfo{NodeId: *nodeId, Addr: *addr})
	center.AddNode(&common.NodeInfo{NodeId: consts.ServerNodeId, Addr: consts.ServerAddr})

	netServiceConfig := &net.ServiceConfig{
		NodeId: *nodeId,
		Addr:   *addr,
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
			HeartbeatTimeout:       common.HeartbeatTimeout,
			InactiveTimeout:        common.InactiveTimeout,
			TickInterval:           common.TickInterval,
		},
		Dialer:          common.Dialer,
		ListenerCreator: common.CreateListener,
		TimerSystem:     net.NewTimerHeap(),
	}

	if agent, err := gcluster.CreateAgent(&gcluster.AgentConfig{
		Center:  center,
		Net:     netServiceConfig,
		Handler: cli,
	}, gcluster.WithLogger(logger.Logger())); err != nil {
		logger.Logger().FatalFields("create gcluster.Service failed", zap.Error(err))
	} else {
		cli.agent = agent
	}

	cli.cli = gactor.NewClient(&gactor.ClientConfig{
		Handler: cli,
	}, gactor.WithClientLogger(logger.Logger()),
		gactor.WithClientAckManager(&gactor.AckConfig{
			Timeout:      common.AckTimeout,
			MaxRetry:     common.AckRetry,
			TickInterval: common.AckTickInterval,
		}),
	)

	if err := cli.start(); err != nil {
		logger.Logger().FatalFields("start failed", zap.Error(err))
	}

	u.start()

	cSignal := make(chan os.Signal, 1)
	signal.Notify(cSignal, syscall.SIGINT, syscall.SIGTERM)
	<-cSignal

	if err := cli.stop(); err != nil {
		logger.Logger().ErrorFields("stop failed", zap.Error(err))
	}
}
