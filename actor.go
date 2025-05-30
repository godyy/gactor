package gactor

import (
	"context"
	"fmt"
	"time"
	"unsafe"
)

// ActorUID 表示 Actor 的唯一标识.
type ActorUID struct {
	Category uint16 // The actor category from ActorDefine
	ID       int64  // Unique instance ID within the category
}

const sizeOfActorUID = int(unsafe.Sizeof(uint16(0)) + unsafe.Sizeof(int64(0)))

func (uid ActorUID) String() string {
	return fmt.Sprintf("%d-%d", uid.Category, uid.ID)
}

// ActorBehavior Actor 行为.
type ActorBehavior interface {
	// GetActor 获取 Actor.
	GetActor() Actor

	// OnStart 启动行为.
	OnStart() error

	// OnStop 停机行为.
	OnStop() error
}

// ActorBehaviorCreator Actor 行为构造器.
type ActorBehaviorCreator func(Actor) ActorBehavior

// ActorTimerArgs Actor 定时器参数.
type ActorTimerArgs struct {
	Actor Actor   // Actor.
	TID   TimerId // 定时器ID
	Args  any     // 参数.
}

// ActorTimerFunc Actor 定时器回调函数.
type ActorTimerFunc func(args *ActorTimerArgs)

// ActorRPCFunc Actor RPC 回调.
type ActorRPCFunc func(a Actor, resp *RPCResp)

// Actor 封装 Actor 接口.
type Actor interface {
	// ActorUID 获取 Actor 的唯一标识.
	ActorUID() ActorUID

	// Behavior 获取 ActorBehavior.
	Behavior() ActorBehavior

	// StartTimer 启动定时器.
	StartTimer(d time.Duration, periodic bool, args any, cb ActorTimerFunc) TimerId

	// StopTimer 停止定时器.
	StopTimer(TimerId)

	// RPC 向 to 指向的 Actor 发起 RPC 调用.
	// ctx 用于控制超时时间. params 为请求参数, reply 用于接收响应参数.
	RPC(ctx context.Context, to ActorUID, params, reply any) error

	// AsyncRPC 向 to 指向的 Actor 发起异步 RPC 调用.
	// ctx 用于控制超时时间. params 为请求参数. cb 为异步回调.
	AsyncRPC(ctx context.Context, to ActorUID, params any, cb ActorRPCFunc) error

	// Cast 向 to 指向的 Actor 投递消息.
	// ctx 用于控制超时时间. payload 为投递的负载消息.
	Cast(ctx context.Context, to ActorUID, payload any) error
}

// CActorBehavior CActor 行为.
type CActorBehavior interface {
	ActorBehavior

	// GetCActor 获取 CActor.
	GetCActor() CActor

	// OnConnected 已连接行为.
	OnConnected()

	// OnDisconnected 已断开连接行为.
	OnDisconnected()
}

// CActorBehaviorCreator CActor 行为构造器.
type CActorBehaviorCreator func(c CActor) CActorBehavior

// ActorSession Actor 网络会话信息.
type ActorSession struct {
	NodeId string // 节点ID.
	SID    uint32 // 会话ID.
}

// reset 重置.
func (as *ActorSession) reset() {
	as.NodeId = ""
	as.SID = 0
}

// IsConnected 是否已连接.
func (as *ActorSession) IsConnected() bool {
	return as.NodeId != ""
}

// CActor 面向客户端的 Actor 接口.
type CActor interface {
	Actor

	// CBehavior 获取 CActorBehavior.
	CBehavior() CActorBehavior

	// Session 获取 Actor 网络会话信息.
	Session() ActorSession

	// PushRawMessage 向客户端推送消息.
	PushRawMessage(ctx context.Context, payload any) error

	// Disconnect 断开与客户端的连接.
	Disconnect(ctx context.Context)
}
