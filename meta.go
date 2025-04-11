package gactor

import (
	"encoding/binary"
	"errors"
	"fmt"

	pkgerrors "github.com/pkg/errors"
)

// ErrMetaNotExists 指示 Actor 元数据不存在.
var ErrMetaNotExists = errors.New("gactor: meta not exists")

// MetaDriver Meta数据驱动.
type MetaDriver interface {
	// GetMeta 获取 Actor Meta 数据.
	// 当 Meta 数据不存在时返回 ErrMetaNotExists.
	GetMeta(ActorUID) (*Meta, error)
}

// Meta Actor Metadata.
type Meta struct {
	Category   uint16     `json:"category,omitempty"`   // Actor 类别.
	ID         int64      `json:"id,omitempty"`         // Actor 在分类下的 ID.
	Deployment Deployment `json:"deployment,omitempty"` // 部署信息.
}

// DeployType 部署类型.
type DeployType int8

const (
	DeployTypeOnNode = DeployType(1) // 部署于节点.
	DeployTypeFollow = DeployType(2) // 跟随其它 Actor.
)

// Deployment 部署信息.
type Deployment struct {
	// Type 部署的类型.
	Type DeployType

	// Info 具体的部署信息.
	Info []byte
}

// NewDeploymentOnNode 创建部署于节点的 Deployment.
func NewDeploymentOnNode(nodeId string) Deployment {
	return Deployment{
		Type: DeployTypeOnNode,
		Info: []byte(nodeId),
	}
}

// NewDeploymentFollow 创建跟随其它 Actor 的 Deployment.
func NewDeploymentFollow(uid ActorUID) Deployment {
	d := Deployment{
		Type: DeployTypeFollow,
	}
	d.Info = make([]byte, sizeOfActorUID)
	binary.BigEndian.PutUint16(d.Info, uid.Category)
	binary.BigEndian.PutUint64(d.Info[2:], uint64(uid.ID))
	return d
}

// NodeID 获取节点ID. 部署类型必须为 DeployTypeOnNode.
func (d *Deployment) NodeID() string {
	if d.Type != DeployTypeOnNode {
		panic("gactor: deployment type is not DeployTypeOnNode")
	}
	return string(d.Info)
}

// ActorUID 获取 ActorUID. 部署类型必须为 DeployTypeFollow.
func (d *Deployment) ActorUID() (uid ActorUID) {
	if d.Type != DeployTypeFollow {
		panic("gactor: deployment type is not DeployTypeFollow")
	}

	uid.Category = binary.BigEndian.Uint16(d.Info)
	uid.ID = int64(binary.BigEndian.Uint64(d.Info[2:]))
	return
}

// getNodeIdOfActor 获取 Actor 所在的节点ID.
func getNodeIdOfActor(md MetaDriver, uid ActorUID) (string, error) {
	// 获取 Actor Meta.
	meta, err := md.GetMeta(uid)
	if err != nil {
		return "", err
	}

	switch meta.Deployment.Type {
	case DeployTypeOnNode:
		return meta.Deployment.NodeID(), nil
	case DeployTypeFollow:
		if nodeId, err := getNodeIdOfActor(md, meta.Deployment.ActorUID()); err != nil {
			return "", pkgerrors.WithMessagef(err, "get nodeId of actor %s failed", meta.Deployment.ActorUID())
		} else {
			return nodeId, nil
		}
	default:
		return "", fmt.Errorf("unknown deploy type %d", meta.Deployment.Type)
	}
}
