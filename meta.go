package gactor

// Meta Actor 元信息.
type Meta struct {
	Category   int                           `json:"category,omitempty"` // Actor 类别.
	ID         int64                         `json:"id,omitempty"`       // Actor 在分类下的 ID.
	Deployment `json:"deployment,omitempty"` // 部署信息.
}

type DeploymentType int

type Deployment struct {
	Type   DeploymentType // 部署类型.
	NodeID string         // 所在节点 ID.
}

const (
	DeployNone   = DeploymentType(0)
	DeployOnNode = DeploymentType(1)
)
