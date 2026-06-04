package common

import (
	"fmt"
	"sync"

	"github.com/godyy/gactor"
	"github.com/rs/xid"
)

type actorLocation struct {
	nodeId   string
	leaseId  string
	expireAt int64
}

type ActorRegistry struct {
	mtx      sync.RWMutex
	actorMap map[gactor.ActorUID]*actorLocation
}

// NewActorRegistry 创建 ActorRegistry.
func NewActorRegistry() *ActorRegistry {
	return &ActorRegistry{
		actorMap: make(map[gactor.ActorUID]*actorLocation),
	}
}

// MakeLeaseID 生成全剧唯一租约ID.
func (r *ActorRegistry) MakeLeaseID() string {
	return xid.New().String()
}

// Register 注册 Actor.
// 若 Actor 已注册, 不论是否当前节点注册, 均通过 ActorRegisterResult 返回所在节点和过期
// 时间.
// 若 Actor 已注册, 且所在节点ID与当前节点ID不同, 返回 ErrActorAlreadyRegistered 错误,
// 否则, 使用当前租约覆盖旧租约, 并更新存续时间.
func (r *ActorRegistry) RegisterActor(params gactor.ActorRegisterParams) (gactor.ActorRegisterResult, error) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	// fmt.Printf("registe %+v\n", params)
	location := r.actorMap[params.UID]
	if location == nil {
		location = &actorLocation{
			nodeId:  params.NodeId,
			leaseId: params.LeaseId,
		}
		r.actorMap[params.UID] = location
		return gactor.ActorRegisterResult{
			NodeId:   params.NodeId,
			ExpireAt: location.expireAt,
		}, nil
	}
	if params.NodeId != location.nodeId {
		return gactor.ActorRegisterResult{
			NodeId:   location.nodeId,
			ExpireAt: location.expireAt,
		}, gactor.ErrActorAlreadyRegistered
	}
	location.leaseId = params.LeaseId
	return gactor.ActorRegisterResult{
		NodeId:   location.nodeId,
		ExpireAt: location.expireAt,
	}, nil
}

// UnregisterActor 注销 Actor.
// 若 Actor 未注册, 返回 ErrActorNotExists 错误.
// 若节点ID和租约ID匹配, 则注销 Actor, 否则返回 ErrLeaseMismatch 错误.
func (r *ActorRegistry) UnregisterActor(params gactor.ActorUnregisterParams) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	location := r.actorMap[params.UID]
	if location == nil {
		return gactor.ErrActorNotExists
	}
	if params.NodeId != location.nodeId || params.LeaseId != location.leaseId {
		return gactor.ErrLeaseMismatch
	}
	delete(r.actorMap, params.UID)
	return nil
}

// KeepActorAlive 保持 Actor 存续.
// 若 Actor 未注册, 返回 ErrActorNotExists 错误,
// 否则, 若节点ID和租约ID匹配, 则更新 Actor 存续时间, 否则返回 ErrLeaseMismatch 错误.
func (r *ActorRegistry) KeepActorAlive(params gactor.ActorKeepAliveParams) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	location := r.actorMap[params.UID]
	if location == nil {
		return gactor.ErrActorNotExists
	}
	if params.NodeId != location.nodeId {
		fmt.Printf("nodeId not match, expect %s, got %s\n", location.nodeId, params.NodeId)
		return gactor.ErrLeaseMismatch
	}
	if params.LeaseId != location.leaseId {
		fmt.Printf("leaseId not match, expect %s, got %s\n", location.leaseId, params.LeaseId)
		return gactor.ErrLeaseMismatch
	}
	return nil
}

// GetActorLocation 获取 Actor 位置信息.
// 若 Actor 未注册, 返回 ErrActorNotExists 错误.
func (r *ActorRegistry) GetActorLocation(uid gactor.ActorUID) (gactor.ActorLocation, error) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()
	location := r.actorMap[uid]
	if location == nil {
		return gactor.ActorLocation{}, gactor.ErrActorNotExists
	}
	return gactor.ActorLocation{
		NodeId:   location.nodeId,
		ExpireAt: location.expireAt,
	}, nil
}

type ActorRouter struct {
	nodes map[gactor.ActorUID]string
}

// NewActorRouter 创建 ActorRouter.
func NewActorRouter() *ActorRouter {
	return &ActorRouter{
		nodes: make(map[gactor.ActorUID]string),
	}
}

func (r *ActorRouter) AddNode(uid gactor.ActorUID, nodeId string) {
	r.nodes[uid] = nodeId
}

// PickActorNode 选择节点.
func (r *ActorRouter) PickActorNode(uid gactor.ActorUID) (string, error) {
	nodeId, ok := r.nodes[uid]
	if !ok {
		return "", gactor.ErrActorNotExists
	}
	return nodeId, nil
}
