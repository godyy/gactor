package gactor

import (
	"fmt"
	"unsafe"
)

// errCode 错误码.
type errCode uint16

const sizeOfErrCode = int(unsafe.Sizeof(errCodeOK))

const (
	errCodeOK            = errCode(0) // OK.
	errCodeInternalError = errCode(1) // 内部错误.

	errCodeEncodePacketFailed = errCode(101) // 编码数据包失败.
	errCodeDecodePacketFailed = errCode(102) // 解码数据包失败.

	errCodeStartActorFailed = errCode(201) // 启动 Actor 失败.
	errCodeActorLoopError   = errCode(202) // Actor 循环错误.
)

// errCodeStrings 错误码文本.
var errCodeStrings = map[errCode]string{
	errCodeOK:                 "ok",
	errCodeInternalError:      "internal error",
	errCodeEncodePacketFailed: "encode packet failed",
	errCodeDecodePacketFailed: "decode packet failed",
	errCodeStartActorFailed:   "start actor failed",
	errCodeActorLoopError:     "actor loop error",
}

func (ec errCode) Error() string {
	if s, ok := errCodeStrings[ec]; ok {
		return fmt.Sprintf("gactor: %d - %s", ec, s)
	} else {
		return fmt.Sprintf("gactor: unknown error code %d", ec)
	}
}
