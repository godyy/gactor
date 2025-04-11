package common

import "fmt"

type ErrCode int32

const (
	ErrCodeNotLogin ErrCode = 10000
)

var errCodeStrings = map[ErrCode]string{
	ErrCodeNotLogin: "not login",
}

func (ec ErrCode) String() string {
	s, ok := errCodeStrings[ec]
	if !ok {
		return fmt.Sprintf("unknown error code: %d", ec)
	}
	return s
}

func (ec ErrCode) Error() string {
	s, ok := errCodeStrings[ec]
	if !ok {
		s = "unknown"
	}
	return fmt.Sprintf("%d - %s", ec, s)
}
