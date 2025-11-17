package gactor

import (
	"github.com/godyy/glog"
)

// recoverAndLog 捕获panic并打印日志.
// 如果callback 不为nil, 则在打印日志后调用callback.
func recoverAndLog(msg string, logger glog.Logger, callback func()) {
	if err := recover(); err != nil {
		logger.ErrorFields(msg, lfdPanic(err))
		if callback != nil {
			callback()
		}
	}
}
