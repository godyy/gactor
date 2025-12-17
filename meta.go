package gactor

import (
	"errors"
)

// ErrMetaNotExists 指示 Actor 元数据不存在.
var ErrMetaNotExists = errors.New("gactor: meta not exists")

// MetaDriver Meta数据驱动.
type MetaDriver interface {
	// GetMeta 获取 Actor Meta 数据.
	// 当 Meta 数据不存在时返回 ErrMetaNotExists.
	GetMeta(ActorUID) (Meta, error)
}

// Meta Actor元数据接口.
type Meta interface {
	// GetActorUID 获取 ActorUID.
	GetActorUID() ActorUID
	// GetNodeId 获取 Actor 所在的节点ID.
	GetNodeId() string
}

// getNodeIdOfActor 获取 Actor 所在的节点ID.
func getNodeIdOfActor(md MetaDriver, uid ActorUID) (string, error) {
	// 获取 Actor Meta.
	meta, err := md.GetMeta(uid)
	if err != nil {
		return "", err
	}

	return meta.GetNodeId(), nil
}
