package gactor

import (
	"time"

	"github.com/godyy/gtimewheel"
)

// TimeSystem 时间系统.
type TimeSystem interface {
	// Now 返回当前时间.
	Now() time.Time

	// Until 返回 t 距离当前时间的时间差.
	Until(t time.Time) time.Duration
}

// defTimeSystem 默认时间系统.
type defTimeSystem struct{}

func (d *defTimeSystem) Now() time.Time {
	return time.Now()
}

func (d *defTimeSystem) Until(t time.Time) time.Duration {
	return time.Until(t)
}

// DefTimeSystem 返回默认时间系统.
func DefTimeSystem() TimeSystem {
	return &defTimeSystem{}
}

// TimerId 定时器ID.
type TimerId = uint64

// TimerIdNone 表示无效的定时器ID.
const TimerIdNone = gtimewheel.TimerIdNone

// TimerFunc 定时器回调函数.
type TimerFunc = gtimewheel.TimerFunc

// TimerArgs 定时器参数.
type TimerArgs = gtimewheel.TimerArgs
