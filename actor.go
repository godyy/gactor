package gactor

import (
	"context"
	"fmt"
	"time"
	"unsafe"

	"go.uber.org/zap/zapcore"
)

// ActorCategory Actor 分类.
type ActorCategory = uint16

// ActorID Actor 实例ID.
type ActorID = int64

// ActorUID 表示 Actor 的唯一标识.
type ActorUID struct {
	Category ActorCategory // The actor category from ActorDefine
	ID       ActorID       // Unique instance ID within the category
}

const sizeOfActorUID = int(unsafe.Sizeof(ActorCategory(0)) + unsafe.Sizeof(ActorID(0)))

func (uid ActorUID) String() string {
	return fmt.Sprintf("%d-%d", uid.Category, uid.ID)
}

func (uid ActorUID) IsZero() bool {
	return uid.Category == 0 && uid.ID == 0
}

func (uid *ActorUID) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddUint16("category", uint16(uid.Category))
	enc.AddInt64("id", int64(uid.ID))
	return nil
}

// ActorBehavior Actor 行为.
type ActorBehavior interface {
	// OnStart 启动行为.
	OnStart() error

	// OnStop 停机行为.
	OnStop() error
}

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
	// Category 获取 Actor 的分类.
	Category() ActorCategory

	// ActorUID 获取 Actor 的唯一标识.
	ActorUID() ActorUID

	// Behavior 获取 ActorBehavior.
	Behavior() ActorBehavior

	// StartTimer 启动定时器.
	StartTimer(d time.Duration, periodic bool, args any, cb ActorTimerFunc) TimerId

	// StopTimer 停止定时器.
	StopTimer(TimerId)

	// RPCWithDeadline 向 to 指向的 Actor 发起 RPC 调用.
	// deadline 为超时时间.
	RPCWithDeadline(to ActorUID, params, reply any, deadline time.Time) error

	// RPCWithTimeout 向 to 指向的 Actor 发起 RPC 调用.
	// timeout 为超时间隔.
	RPCWithTimeout(to ActorUID, params, reply any, timeout time.Duration) error

	// RPC 向 to 指向的 Actor 发起 RPC 调用.
	// 使用配置的默认超时间隔.
	RPC(to ActorUID, params, reply any) error

	// RPCWithContext 向 to 指向的 Actor 发起 RPC 调用.
	// 超时 deadline 从 ctx 获取，若未设置, 使用默认超时时间.
	RPCWithContext(ctx context.Context, to ActorUID, params, reply any) error

	// AsyncRPCWithDeadline 向 to 指向的 Actor 发起异步 RPC 调用.
	// deadline 为超时时间.
	AsyncRPCWithDeadline(to ActorUID, params any, cb ActorRPCFunc, deadline time.Time) error

	// AsyncRPCWithTimeout 向 to 指向的 Actor 发起异步 RPC 调用.
	// timeout 为超时间隔.
	AsyncRPCWithTimeout(to ActorUID, params any, cb ActorRPCFunc, timeout time.Duration) error

	// AsyncRPC 向 to 指向的 Actor 发起异步 RPC 调用.
	// 使用配置的默认超时间隔.
	AsyncRPC(to ActorUID, params any, cb ActorRPCFunc) error

	// AsyncRPCWithContext 向 to 指向的 Actor 发起异步 RPC 调用.
	// 超时 deadline 从 ctx 获取，若未设置, 使用默认超时时间.
	AsyncRPCWithContext(ctx context.Context, to ActorUID, params any, cb ActorRPCFunc) error

	// Cast 向 to 指向的 Actor 投递消息.
	// payload 为投递的负载消息.
	Cast(to ActorUID, payload any) error
}

// CActorBehavior CActor 行为.
type CActorBehavior interface {
	ActorBehavior

	// OnConnected 已连接行为.
	OnConnected()

	// OnDisconnected 已断开连接行为.
	OnDisconnected()
}

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

	// Session 获取 Actor 网络会话信息.
	Session() ActorSession

	// PushRawMessage 向客户端推送消息.
	PushRawMessage(payload any) error

	// Disconnect 断开与客户端的连接.
	Disconnect()
}
