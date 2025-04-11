package logger

import (
	"github.com/godyy/gutils/log"
)

var logger log.Logger

func Init() error {
	l := log.NewLogger(&log.Config{
		Level:        log.DebugLevel,
		EnableCaller: true,
		CallerSkip:   0,
		Development:  true,
		Cores:        []log.CoreConfig{log.NewStdCoreConfig()},
	})
	logger = l
	return nil
}

func Logger() log.Logger {
	return logger
}
