package common

import "time"

const (
	AckTimeout      = 100 * time.Millisecond
	AckRetry        = 1
	AckTickInterval = 100 * time.Millisecond
)
