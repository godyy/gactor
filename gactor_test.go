package gactor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/godyy/glog"
	"github.com/godyy/gtimewheel"

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
		Development:  true,
		Cores:        []glog.CoreConfig{glog.NewStdCoreConfig()},
	})
	logger = l
	return nil
}

func TestService(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	if err := initLogger(); err != nil {
		t.Fatal(err)
	}

	actorDefines := []IActorDefine{
		&ActorDefine{
			ActorDefineCommon: &ActorDefineCommon{
				Name:                       "test",
				Category:                   1,
				Priority:                   1,
				MessageBoxSize:             1000,
				MaxCompletedAsyncRPCAmount: 1,
				RecycleTime:                0,
				Handler:                    testHandlerChain.Handle,
			},
			BehaviorCreator: func(a Actor) ActorBehavior {
				return &testActor{Actor: a}
			},
		},
	}

	actorAmount := 10
	actorUIDs = make([]ActorUID, actorAmount)
	for i := 0; i < actorAmount; i++ {
		actorUIDs[i] = ActorUID{
			Category: 1,
			ID:       int64(i + 1),
		}
	}

	metaDriver := &testMetaDriver{
		metaMap: make(map[ActorUID]*Meta, len(actorUIDs)),
	}
	for _, uid := range actorUIDs {
		metaDriver.metaMap[uid] = &Meta{
			Category:   uid.Category,
			ID:         uid.ID,
			Deployment: NewDeploymentOnNode("test"),
		}
	}
	svcHandler := &testServiceHandler{
		testMetaDriver:  metaDriver,
		testNetAgent:    &testNetAgent{nodeId: "test"},
		testPacketCodec: &testPacketCodec{},
		TimeSystem:      DefTimeSystem,
	}

	svcConfig := &ServiceConfig{
		ActorConfig: ActorConfig{
			ActorDefines: actorDefines,
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, uid := range actorUIDs {
		ta, err := svc.startActor(ctx, uid)
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

type testMetaDriver struct {
	metaMap map[ActorUID]*Meta
}

func (md *testMetaDriver) GetMeta(uid ActorUID) (*Meta, error) {
	meta, ok := md.metaMap[uid]
	if !ok {
		return nil, ErrMetaNotExists
	}
	return meta, nil
}

type testNetAgent struct {
	nodeId string
}

func (na *testNetAgent) NodeId() string {
	return na.nodeId
}

func (na *testNetAgent) Send(ctx context.Context, nodeId string, b []byte) error {
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
	*testMetaDriver
	*testNetAgent
	*testPacketCodec
	TimeSystem
}

func (sh *testServiceHandler) GetMetaDriver() MetaDriver {
	return sh.testMetaDriver
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

		if err := ta.Cast(context.Background(), targetUID, &testS2SMessage{
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
			if err := ta.AsyncRPC(context.Background(), targetUID, &params, func(_ Actor, resp *RPCResp) {
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
