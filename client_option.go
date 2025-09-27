package gactor

import (
	"github.com/godyy/glog"
)

// ClientOption Client 选项.
type ClientOption func(*Client)

// WithClientLogger 日志工具选项.
func WithClientLogger(logger glog.Logger) ClientOption {
	return func(c *Client) {
		c.setLogger(logger)
	}
}

// WithClientAckManager Ack 管理器选项.
func WithClientAckManager(cfg *AckConfig) ClientOption {
	return func(c *Client) {
		c.ackManager = newAckManager(cfg, c)
	}
}
