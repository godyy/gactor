package gactor

import (
	"github.com/godyy/glog"
	"github.com/godyy/gutils/debug"
)

// recoverAndLog 捕获panic并打印日志.
// 如果callback 不为nil, 则在打印日志后调用callback.
func recoverAndLog(msg string, logger glog.Logger, callback func()) {
	if err := recover(); err != nil {
		stack := debug.StackTrace(1, 0)
		logger.Errorf("[%s] %v\n%s\n", msg, err, stack)
		if callback != nil {
			callback()
		}
	}
}
