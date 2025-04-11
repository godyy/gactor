package common

import (
	"errors"

	"github.com/godyy/gcluster/center"
)

type NodeInfo struct {
	NodeId string
	Addr   string
}

func (n *NodeInfo) GetNodeId() string {
	return n.NodeId
}

func (n *NodeInfo) GetNodeAddr() string {
	return n.Addr
}

type Center struct {
	nodes map[string]*NodeInfo
}

func NewCenter() *Center {
	return &Center{
		nodes: make(map[string]*NodeInfo),
	}
}

func (c *Center) AddNode(n *NodeInfo) {
	c.nodes[n.NodeId] = n
}

// GetNode 获取节点信息.
func (c *Center) GetNode(nodeId string) (center.Node, error) {
	node, ok := c.nodes[nodeId]
	if !ok {
		return nil, errors.New("node not found")
	}
	return node, nil
}
