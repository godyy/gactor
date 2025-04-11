package gactor

import "github.com/godyy/gutils/log"

// ClientOption Client 选项.
type ClientOption func(*Client)

// WithClientLogger 日志工具选项.
func WithClientLogger(logger log.Logger) ClientOption {
	return func(c *Client) {
		c.setLogger(logger)
	}
}
