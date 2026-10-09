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

const lenOfActorUID = int(unsafe.Sizeof(ActorCategory(0)) + unsafe.Sizeof(ActorID(0)))

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

	// OnStop 停机行为。
	// ctx.Reason 用于区分正常回收、Service 停机和迁移成功后的源端回收。
	// 迁移回收时仍应释放本地资源，但通常不应再次持久化业务状态。
	OnStop(ctx ActorStopContext) error
}

// ActorStopReason 描述 Actor 停机的原因。
type ActorStopReason int8

const (
	// ActorStopReasonNormal 表示普通回收或异常终止。
	ActorStopReasonNormal ActorStopReason = iota
	// ActorStopReasonShutdown 表示 Service 正在停机。
	ActorStopReasonShutdown
	// ActorStopReasonMigrated 表示源 Actor 已完成迁移后的本地回收。
	ActorStopReasonMigrated
)

// ActorStopContext 是传递给 OnStop 的停机上下文。
// Reason 为 ActorStopReasonMigrated 时，业务状态已在迁移流程中转移到目标节点。
type ActorStopContext struct {
	Reason ActorStopReason
}

// ActorMigrationContext 描述一次 Actor 迁移的不可变业务上下文。
// 业务应使用 SourceLeaseID 作为快照读写与外部副作用的幂等键。
type ActorMigrationContext struct {
	UID           ActorUID     // 正在迁移的 Actor。
	SourceNodeID  string       // 迁移源节点。
	SourceLeaseID string       // 迁移源 Actor 的租约，也是本轮迁移代际。
	TargetNodeID  string       // 迁移目标节点。
	TargetLeaseID string       // 目标 Actor 的新租约；仅 OnMigrateIn 时有效。
	StartSeq      uint32       // 本次迁移开始包序号。
	Session       ActorSession // CActor 会话副本；普通 Actor 为零值。
}

// ActorMigrationBehavior Actor 迁移行为扩展。
// 该接口为可选实现，用于迁移前后由业务自行保存和恢复快照。
// 迁移失败会自动重试，因此实现必须具备幂等性；尤其是 OnMigrateOut
// 不得因重复调用重复扣减资源、覆盖新状态或产生不可逆的外部副作用。
type ActorMigrationBehavior interface {
	// OnMigrateOut 在源 Actor 每次尝试向目标节点发起迁移前调用。
	// 同一轮迁移可能多次调用，应按 ctx.SourceLeaseID 保存可被 OnMigrateIn 幂等读取的快照。
	OnMigrateOut(ctx ActorMigrationContext) error

	// OnMigrateIn 在目标 Actor 完成租约接管后调用。
	// 实现应按 ctx.SourceLeaseID 幂等恢复快照，并能容忍重复的迁移开始包。
	OnMigrateIn(ctx ActorMigrationContext) error
}

// ActorTimerArgs Actor 定时器参数.
type ActorTimerArgs struct {
	Actor Actor   // Actor.
	TID   TimerId // 定时器ID
	Args  any     // 参数.
}

// ActorTimerFunc Actor 定时器回调函数.
type ActorTimerFunc func(args ActorTimerArgs)

// ActorRPCFunc Actor RPC 回调.
type ActorRPCFunc func(a Actor, resp RPCResp)

// ActorFuncArgs Actor 回调函数参数.
type ActorFuncArgs struct {
	Actor Actor // Actor.
	Value any   // 返回值.
	Err   error // 错误.
}

// ActorFunc Actor 回调函数.
type ActorFunc func(ActorFuncArgs)

// ActorAsyncCaller Actor 异步函数调用器.
// 通过调用该函数, 通知 Actor 调用相应的回调函数.
type ActorAsyncCaller func(value any, err error) error

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

	// Forward 向 to 指向的 Actor 透传消息.
	Forward(to ActorUID, payload any) error

	// AsyncCall 异步调用.
	// timeout 为超时间隔.
	// 返回 ActorFuncCaller, 用于调用该回调函数.
	// 如果返回的调用函数在 timeout 内未被调用, 则系统自动使用 ErrTimeout 错误调用回调函数.
	AsyncCall(f ActorFunc, timeout time.Duration) (ActorAsyncCaller, error)
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
