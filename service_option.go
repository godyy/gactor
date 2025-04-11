package gactor

import "github.com/godyy/gutils/log"

// ServiceOption Service 选项.
type ServiceOption func(*Service)

// WithServiceLogger 日志工具选项.
func WithServiceLogger(logger log.Logger) ServiceOption {
	return func(s *Service) {
		s.setLogger(logger)
	}
}
