package gactor

import (
	"errors"
)

// ErrActorNotExists Actor 不存在.
var ErrActorNotExists = errors.New("gactor: actor not exists")

// ErrActorAlreadyRegistered Actor 已注册.
var ErrActorAlreadyRegistered = errors.New("gactor: actor already registered")

// ErrLeaseMismatch 租约不匹配.
var ErrLeaseMismatch = errors.New("gactor: lease mismatch")

// actorExpireThreshold Actor 过期阈值, 单位秒.
const actorExpireThreshold = 2

// actorUnregisterThreshold Actor 默认注销阈值, 单位秒.
const actorUnregisterThreshold = 5

// ActorRegisterParams Actor 注册参数.
type ActorRegisterParams struct {
	// Actor 唯一 ID.
	UID ActorUID

	// 节点ID.
	NodeId string

	// 租约ID.
	LeaseId string

	// Actor 存续时间, 秒级.
	// 若大于0, TTL后Actor自动注销.
	// 否则, 需要手动注销.
	TTL int64
}

// ActorRegisterResult Actor 注册结果.
type ActorRegisterResult struct {
	NodeId   string // Actor 所在节点ID.
	ExpireAt int64  // Actor 过期时间, 秒级时间戳. 大于0表示在该时刻后会自动注销.
}

// ActorUnregisterParams Actor 注销参数.
type ActorUnregisterParams struct {
	UID     ActorUID // Actor 唯一 ID.
	NodeId  string   // 节点ID.
	LeaseId string   // 租约ID.
}

// ActorKeepAliveParams Actor 保持存续参数.
type ActorKeepAliveParams struct {
	// Actor 唯一 ID.
	UID ActorUID

	// 节点ID.
	NodeId string

	// 租约ID.
	LeaseId string

	// Actor 存续时间, 单位秒.
	// 若大于0, TTL后Actor自动注销.
	// 否则, 需要手动注销.
	TTL int64
}

// ActorLocation Actor 位置信息.
type ActorLocation struct {
	NodeId   string // Actor 所在节点ID.
	ExpireAt int64  // Actor 过期时间, 秒级时间戳. 大于0表示在该时刻后会自动注销.
}

// ActorRegistry Actor 注册表.
// 用于 Actor 的注册/注销/存续, 以及节点信息查询.
type ActorRegistry interface {
	// MakeLeaseID 生成全剧唯一租约ID.
	MakeLeaseID() string

	// Register 注册 Actor.
	// 若 Actor 已注册, 不论是否当前节点注册, 均通过 ActorRegisterResult 返回所在节点和过期
	// 时间.
	// 若 Actor 已注册, 且所在节点ID与当前节点ID不同, 返回 ErrActorAlreadyRegistered 错误,
	// 否则, 使用当前租约覆盖旧租约, 并更新存续时间.
	RegisterActor(params ActorRegisterParams) (ActorRegisterResult, error)

	// UnregisterActor 注销 Actor.
	// 若 Actor 未注册, 返回 ErrActorNotExists 错误.
	// 若节点ID和租约ID匹配, 则注销 Actor, 否则返回 ErrLeaseMismatch 错误.
	UnregisterActor(params ActorUnregisterParams) error

	// KeepActorAlive 保持 Actor 存续.
	// 若 Actor 未注册, 返回 ErrActorNotExists 错误,
	// 否则, 若节点ID和租约ID匹配, 则更新 Actor 存续时间, 否则返回 ErrLeaseMismatch 错误.
	KeepActorAlive(params ActorKeepAliveParams) error

	// GetActorLocation 获取 Actor 位置信息.
	// 若 Actor 未注册, 返回 ErrActorNotExists 错误.
	GetActorLocation(uid ActorUID) (ActorLocation, error)
}

// ErrNoAvailableNode 无可用节点.
var ErrNoAvailableNode = errors.New("gactor: no available node")

// ActorRouter Actor 路由.
// 用于在 Actor 未注册时, 选择合适的节点.
type ActorRouter interface {
	// PickActorNode 选择节点.
	// 若无可用节点, 返回 ErrNoAvailableNode 错误.
	PickActorNode(uid ActorUID) (string, error)
}
