package gactor

import "context"

// NetAgent 网络代理.
type NetAgent interface {
	// Send2Node 发送字节数据 b 到 nodeId 指定的节点.
	Send2Node(ctx context.Context, nodeId string, b []byte) error
}
