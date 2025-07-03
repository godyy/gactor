package gactor

import (
	"time"

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
		c.ackM = newAckManager(cfg, c)
		go func() {
			ticker := time.NewTicker(c.ackM.getCfg().TickInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					c.ackM.tick()
				case <-c.cStopped:
					return
				}
			}
		}()
	}
}
