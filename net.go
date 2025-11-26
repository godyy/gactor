package gactor

import "context"

// NetAgent 网络代理.
type NetAgent interface {
	// Send 发送字节数据 b 到 nodeId 指定的节点.
	Send(ctx context.Context, nodeId string, b []byte) error
}
