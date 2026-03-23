package gactor

import "errors"

// ErrNetworkBusy 表示网络繁忙的错误. 例如, 待发送队列已满.
var ErrNetworkBusy = errors.New("gactor: network busy")

// NetAgent 网络代理.
type NetAgent interface {
	// Send2Node 发送字节数据 b 到 nodeId 指定的节点.
	// 如果网络繁忙, 例如待发送队列已满, 返回 ErrNetworkBusy.
	Send2Node(nodeId string, b []byte) error
}
