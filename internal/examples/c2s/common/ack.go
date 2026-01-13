package common

import "time"

const (
	AckTimeout      = 500 * time.Millisecond
	AckRetry        = 1
	AckTickInterval = 100 * time.Millisecond
)
