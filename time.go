package gactor

import "time"

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

// DefTimeSystem 默认时间系统.
var DefTimeSystem = &defTimeSystem{}
