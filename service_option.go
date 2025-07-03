package gactor

import (
	"time"

	"github.com/godyy/glog"
)

// ServiceOption Service 选项.
type ServiceOption func(*Service)

// WithServiceLogger 日志工具选项.
func WithServiceLogger(logger glog.Logger) ServiceOption {
	return func(s *Service) {
		s.setLogger(logger)
	}
}

// WithClientAckManager Ack 管理器选项.
func WithServiceAckManager(cfg *AckConfig) ServiceOption {
	return func(s *Service) {
		s.ackM = newAckManager(cfg, s)
		go func() {
			ticker := time.NewTicker(s.ackM.getCfg().TickInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					s.ackM.tick()
				case <-s.cStopped:
					return
				}
			}
		}()
	}
}
