package gactor

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/godyy/glog"
	"github.com/godyy/gtimewheel"
	"github.com/rs/xid"

	pkgerrors "github.com/pkg/errors"
)

var (
	logger    glog.Logger
	actorUIDs []ActorUID
)

func initLogger() error {
	l := glog.NewLogger(&glog.Config{
		Level:        glog.DebugLevel,
		EnableCaller: true,
		CallerSkip:   0,
		// Development:  true,
		Cores: []glog.CoreConfig{glog.NewStdCoreConfig()},
	})
	logger = l
	return nil
}

func TestService(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	if err := initLogger(); err != nil {
		t.Fatal(err)
	}

	actorDefines := []ActorDefine{
		NewActorDefine(ActorDefineConfig{
			Name:           "test",
			Category:       1,
			Priority:       1,
			MessageBoxSize: 1000,
			BehaviorCreator: func(a Actor) ActorBehavior {
				return &testActor{Actor: a}
			},
		},
			WithMaxAsyncRPCAmount(1),
		),
	}

	actorAmount := 10
	actorUIDs = make([]ActorUID, actorAmount)
	for i := 0; i < actorAmount; i++ {
		actorUIDs[i] = ActorUID{
			Category: 1,
			ID:       int64(i + 1),
		}
	}

	actorRegistry := &testActorRegistry{
		actorMap: make(map[ActorUID]*testActorLocation),
	}
	actorRouter := &testActorRouter{
		nodes: []string{"test"},
	}
	svcHandler := &testServiceHandler{
		testActorRegistry: actorRegistry,
		testActorRouter:   actorRouter,
		testNetAgent:      &testNetAgent{},
		testPacketCodec:   &testPacketCodec{},
		TimeSystem:        DefTimeSystem,
	}

	svcConfig := &ServiceConfig{
		NodeId: "test",
		ActorConfig: ActorConfig{
			ActorDefines: actorDefines,
			RegistryTTL:  6,
			Handler:      testHandlerChain.Handle,
		},
		TimerConfig: TimerConfig{
			TimeWheelLevels: []gtimewheel.LevelConfig{
				{Name: "100 ms", Span: 100 * time.Millisecond, Slots: 10},
				{Name: "s", Span: 1 * time.Second, Slots: 60},
				{Name: "min", Span: 1 * time.Minute, Slots: 60},
				{Name: "h", Span: 1 * time.Hour, Slots: 24},
			},
		},
		RPCConfig: RPCConfig{
			DefRPCTimeout: 100 * time.Millisecond,
		},
		MaxRTT:  200,
		Handler: svcHandler,
	}

	svc := NewService(svcConfig, WithServiceLogger(logger))
	if err := svc.Start(); err != nil {
		t.Fatal(err)
	}

	for _, uid := range actorUIDs {
		ta, err := svc.startActor(uid, "")
		if err != nil {
			t.Fatalf("start actor %s, %v", uid, err)
		}
		t.Logf("start actor %s", uid)
		_ = ta.core().deref()
	}

	time.Sleep(5 * time.Second)

	if err := svc.Stop(); err != nil {
		t.Fatal("doStop failed: ", err)
	}
}

type testActorLocation struct {
	nodeId   string
	leaseId  string
	expireAt int64
}

type testActorRegistry struct {
	mtx      sync.RWMutex
	actorMap map[ActorUID]*testActorLocation
}

// MakeLeaseID 生成全剧唯一租约ID.
func (r *testActorRegistry) MakeLeaseID() string {
	return xid.New().String()
}

// Register 注册 Actor.
// 若 Actor 已注册, 不论是否当前节点注册, 均通过 ActorRegisterResult 返回所在节点和过期
// 时间.
// 若 Actor 已注册, 且所在节点ID与当前节点ID不同, 返回 ErrActorAlreadyRegistered 错误,
// 否则, 使用当前租约覆盖旧租约, 并更新存续时间.
func (r *testActorRegistry) RegisterActor(params ActorRegisterParams) (ActorRegisterResult, error) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	// fmt.Printf("registe %+v\n", params)
	location := r.actorMap[params.UID]
	if location == nil {
		location = &testActorLocation{
			nodeId:  params.NodeId,
			leaseId: params.LeaseId,
		}
		if params.TTL > 0 {
			location.expireAt = time.Now().Unix() + params.TTL
		}
		r.actorMap[params.UID] = location
		return ActorRegisterResult{
			NodeId:   params.NodeId,
			ExpireAt: location.expireAt,
		}, nil
	}
	if params.NodeId != location.nodeId {
		return ActorRegisterResult{
			NodeId:   location.nodeId,
			ExpireAt: location.expireAt,
		}, ErrActorAlreadyRegistered
	}
	location.leaseId = params.LeaseId
	if params.TTL > 0 {
		location.expireAt = time.Now().Unix() + params.TTL
	}
	return ActorRegisterResult{
		NodeId:   location.nodeId,
		ExpireAt: location.expireAt,
	}, nil
}

// UnregisterActor 注销 Actor.
// 若 Actor 未注册, 返回 ErrActorNotExists 错误.
// 若节点ID和租约ID匹配, 则注销 Actor, 否则返回 ErrLeaseMismatch 错误.
func (r *testActorRegistry) UnregisterActor(params ActorUnregisterParams) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	location := r.actorMap[params.UID]
	if location == nil {
		return ErrActorNotExists
	}
	if params.NodeId != location.nodeId {
		return ErrLeaseMismatch
	}
	if params.LeaseId != location.leaseId {
		// fmt.Println(456, params.LeaseId, location.leaseId)
		return ErrLeaseMismatch
	}
	delete(r.actorMap, params.UID)
	return nil
}

// KeepActorAlive 保持 Actor 存续.
// 若 Actor 未注册, 返回 ErrActorNotExists 错误,
// 否则, 若节点ID和租约ID匹配, 则更新 Actor 存续时间, 否则返回 ErrLeaseMismatch 错误.
func (r *testActorRegistry) KeepActorAlive(params ActorKeepAliveParams) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	location := r.actorMap[params.UID]
	if location == nil {
		return ErrActorNotExists
	}
	if params.NodeId != location.nodeId {
		return ErrLeaseMismatch
	}
	if params.LeaseId != location.leaseId {
		return ErrLeaseMismatch
	}
	location.expireAt = time.Now().Unix() + params.TTL
	return nil
}

// GetActorLocation 获取 Actor 位置信息.
// 若 Actor 未注册, 返回 ErrActorNotExists 错误.
func (r *testActorRegistry) GetActorLocation(uid ActorUID) (ActorLocation, error) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()
	location := r.actorMap[uid]
	if location == nil {
		return ActorLocation{}, ErrActorNotExists
	}
	return ActorLocation{
		NodeId:   location.nodeId,
		ExpireAt: location.expireAt,
	}, nil
}

type testActorRouter struct {
	nodes []string
}

// PickActorNode 选择节点.
func (r *testActorRouter) PickActorNode(uid ActorUID) (string, error) {
	if len(r.nodes) == 0 {
		return "", errors.New("no nodes")
	}
	return r.nodes[uid.ID%int64(len(r.nodes))], nil
}

type testNetAgent struct {
}

func (na *testNetAgent) Send2Node(nodeId string, b []byte) error {
	return nil
}

type testPacketCodec struct {
}

func (pc *testPacketCodec) GetBytes(size int) []byte {
	return make([]byte, 0, size)
}

func (pc *testPacketCodec) PutBytes(b []byte) {
}

func (pc *testPacketCodec) Encode(allocator PacketAllocator, payload any) ([]byte, error) {
	switch allocator.PacketType() {
	case PacketTypeS2SRpcResp, PacketTypeS2SRpc, PacketTypeS2SCast:
		s2sMsg, ok := payload.(*testS2SMessage)
		if !ok {
			return nil, errors.New("invalid payload type")
		}
		bytes, err := json.Marshal(s2sMsg.payload)
		if err != nil {
			return nil, pkgerrors.WithMessage(err, "marshal RPC payload")
		}
		var buf Buffer
		if err := allocator.AllocBuf(&buf, 4+len(bytes)); err != nil {
			return nil, pkgerrors.WithMessage(err, "allocate buffer")
		}
		if err := buf.WriteUint32(uint32(s2sMsg.msgId)); err != nil {
			return nil, pkgerrors.WithMessage(err, "write msg id")
		}
		if _, err := buf.Write(bytes); err != nil {
			return nil, pkgerrors.WithMessage(err, "write msg bytes")
		}
		return buf.Data(), nil
	default:
		return nil, fmt.Errorf("invalid packet type %d", allocator.PacketType())
	}
}

func (pc *testPacketCodec) EncodePayload(pt PacketType, payload any) ([]byte, error) {
	switch pt {
	case PacketTypeS2SRpcResp, PacketTypeS2SRpc, PacketTypeS2SCast:
		s2sMsg, ok := payload.(*testS2SMessage)
		if !ok {
			return nil, errors.New("invalid payload type")
		}
		bytes, err := json.Marshal(s2sMsg.payload)
		if err != nil {
			return nil, pkgerrors.WithMessage(err, "marshal RPC payload")
		}
		b := pc.GetBytes(4 + len(bytes))
		var buf Buffer
		buf.SetBuf(b)
		if err := buf.WriteUint32(uint32(s2sMsg.msgId)); err != nil {
			return nil, pkgerrors.WithMessage(err, "write msg id")
		}
		if _, err := buf.Write(bytes); err != nil {
			return nil, pkgerrors.WithMessage(err, "write msg bytes")
		}
		return buf.Data(), nil
	default:
		return nil, fmt.Errorf("invalid packet type %d", pt)
	}
}

func (pc *testPacketCodec) DecodePayload(pt PacketType, b *Buffer, v any) error {
	switch pt {
	case PacketTypeS2SRpcResp, PacketTypeS2SRpc, PacketTypeS2SCast:
		s2sMsg, ok := v.(*testS2SMessage)
		if !ok {
			return errors.New("invalid payload type")
		}
		return pc.decodeS2SMessage(b, s2sMsg)
	default:
		return fmt.Errorf("invalid packet type %d", pt)
	}
}

func (pc *testPacketCodec) decodeS2SMessage(b *Buffer, s2sMsg *testS2SMessage) error {
	msgId, err := b.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read msg id")
	}
	s2sMsg.msgId = int(msgId)
	switch msgId {
	case msgIdPing:
		s2sMsg.payload = &testMessagePing{}
	case msgIdPong:
		s2sMsg.payload = &testMessagePong{}
	case msgIdCast:
		s2sMsg.payload = &testMessageCast{}
	default:
		return fmt.Errorf("invalid msg id %d", msgId)
	}
	return json.Unmarshal(b.UnreadData(), s2sMsg.payload)
}

type testServiceHandler struct {
	*testActorRegistry
	*testActorRouter
	*testNetAgent
	*testPacketCodec
	TimeSystem
}

func (sh *testServiceHandler) GetActorRegistry() ActorRegistry {
	return sh.testActorRegistry
}

func (sh *testServiceHandler) GetActorRouter() ActorRouter {
	return sh.testActorRouter
}

func (sh *testServiceHandler) GetNetAgent() NetAgent {
	return sh.testNetAgent
}

func (sh *testServiceHandler) GetPacketCodec() PacketCodec {
	return sh.testPacketCodec
}

func (sh *testServiceHandler) GetTimeSystem() TimeSystem {
	return sh.TimeSystem
}

func (sh *testServiceHandler) GetMonitor() ServiceMonitor {
	return nil
}

type testActor struct {
	Actor
	name   string
	tCount int
}

func (a *testActor) GetActor() Actor {
	return a
}

func (a *testActor) OnStart() error {
	a.name = "test:" + a.ActorUID().String()
	a.StartTimer(100*time.Millisecond, true, nil, func(args *ActorTimerArgs) {
		ta := args.Actor.Behavior().(*testActor)
		ta.tCount++
		logger.Debugf("actor %s tick", a.ActorUID())

		targetUID := ta.ActorUID()
		for targetUID == ta.ActorUID() {
			targetUID = actorUIDs[rand.Intn(len(actorUIDs))]
		}

		if err := ta.Cast(targetUID, &testS2SMessage{
			msgId:   msgIdCast,
			payload: &testMessageCast{Msg: fmt.Sprintf("hello, i am %s", ta.ActorUID())},
		}); err != nil {
			logger.Errorf("actor %s cast to %s, %v", a.ActorUID(), targetUID, err)
		} else {
			logger.Debugf("actor %s cast to %s success", a.ActorUID(), targetUID)
		}

		{
			params := testS2SMessage{
				msgId:   msgIdPing,
				payload: &testMessagePing{Time: time.Now()},
			}
			if err := ta.AsyncRPC(targetUID, &params, func(_ Actor, resp *RPCResp) {
				reply := testS2SMessage{}
				if err := resp.DecodeReply(&reply); err != nil {
					logger.Errorf("actor %s decode rpc async payload, %v", a.ActorUID(), err)
				}
			}); err != nil {
				logger.Errorf("actor %s rpc async to %s, %v", a.ActorUID(), targetUID, err)
			} else {
				logger.Debugf("actor %s rpc async to %s success", a.ActorUID(), targetUID)
			}
		}

	})
	return nil
}

func (a *testActor) OnStop() error {
	return nil
}

type testS2SMessage struct {
	msgId   int
	payload any
}

const (
	msgIdPing = 1
	msgIdPong = 2
	msgIdCast = 3
)

type testMessagePing struct {
	Time time.Time `json:"time"`
}

type testMessagePong struct {
	Time time.Time `json:"time"`
}

type testMessageCast struct {
	Msg string `json:"msg"`
}

var testHandlerChain = NewHandlersChain(
	func(ctx *Context) {
		var s2sMsg testS2SMessage
		if err := ctx.Decode(&s2sMsg); err != nil {
			ctx.Abort()
			ctx.ReplyDecodeError()
			return
		}
		ctx.Set("msg", &s2sMsg)
	},
	func(ctx *Context) {
		msgVal, exists := ctx.Get("msg")
		if !exists {
			panic("msg not found")
		}
		msg := msgVal.(*testS2SMessage)
		switch msg.msgId {
		case msgIdPing:
			logger.Debugf("actor %s receive rpc ping", ctx.Actor().ActorUID())
			ctx.Reply(&testS2SMessage{
				msgId:   msgIdPong,
				payload: &testMessagePong{Time: time.Now()},
			})

		case msgIdCast:
			logger.Debugf("actor %s receive cast message: %s", ctx.Actor().ActorUID(), msg.payload.(*testMessageCast).Msg)
			ctx.Reply(nil)

		default:
			panic(fmt.Errorf("wrong msg id %d", msg.msgId))
		}
	},
)
