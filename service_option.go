package gactor

import "github.com/godyy/glog"

// ServiceOption Service 选项.
type ServiceOption func(*Service)

// WithServiceLogger 日志工具选项.
func WithServiceLogger(logger glog.Logger) ServiceOption {
	return func(s *Service) {
		s.setLogger(logger)
	}
}
