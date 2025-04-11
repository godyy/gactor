package gactor

import "context"

// NetAgent 网络代理.
type NetAgent interface {
	// NodeId 返回本地节点ID.
	NodeId() string

	// SendPacket 发送数据包 p 到 nodeId 指定的节点.
	SendPacket(ctx context.Context, nodeId string, p Packet) error
}
