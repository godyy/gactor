package logger

import (
	"github.com/godyy/glog"
)

var logger glog.Logger

func Init() error {
	l := glog.NewLogger(&glog.Config{
		Level:        glog.InfoLevel,
		EnableCaller: true,
		CallerSkip:   0,
		// Development:  true,
		Cores: []glog.CoreConfig{glog.NewStdCoreConfig()},
	})
	logger = l
	return nil
}

func Logger() glog.Logger {
	return logger
}
