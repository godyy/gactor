package gactor

import (
	"errors"
	"fmt"
	"unsafe"
)

// ErrCode 错误码.
type ErrCode uint16

const sizeOfErrCode = int(unsafe.Sizeof(ErrCodeOK))

const (
	ErrCodeOK            = ErrCode(0) // OK.
	ErrCodeInternalError = ErrCode(1) // 内部错误.
	ErrCodeServiceStop   = ErrCode(2) // 服务停止.

	ErrCodeEncodePacketFailed   = ErrCode(101) // 编码数据包失败.
	ErrCodeDecodePacketFailed   = ErrCode(102) // 解码数据包失败.
	ErrCodeActorRegisterByOther = ErrCode(103) // Actor 被其它节点注册.

	ErrCodeStartActorFailed  = ErrCode(201) // 启动 Actor 失败.
	ErrCodeActorLoopError    = ErrCode(202) // Actor 循环错误.
	ErrCodeActorNotConnect   = ErrCode(203) // Actor 未连接.
	ErrCodeActorOtherConnect = ErrCode(204) // Actor 被other连接.
)

// errCodeStrings 错误码文本.
var errCodeStrings = map[ErrCode]string{
	ErrCodeOK:                   "ok",
	ErrCodeInternalError:        "internal error",
	ErrCodeServiceStop:          "service stop",
	ErrCodeActorRegisterByOther: "actor register by other",
	ErrCodeEncodePacketFailed:   "encode packet failed",
	ErrCodeDecodePacketFailed:   "decode packet failed",
	ErrCodeStartActorFailed:     "start actor failed",
	ErrCodeActorLoopError:       "actor loop error",
	ErrCodeActorNotConnect:      "actor not connect",
	ErrCodeActorOtherConnect:    "actor other connect",
}

func (ec ErrCode) String() string {
	return errCodeStrings[ec]
}

func (ec ErrCode) Error() string {
	if s, ok := errCodeStrings[ec]; ok {
		return fmt.Sprintf("gactor: %d - %s", ec, s)
	} else {
		return fmt.Sprintf("gactor: unknown error code %d", ec)
	}
}

// Err2ErrCode 将 error 转换为 errCode.
func Err2ErrCode(err error) (code ErrCode) {
	if errors.As(err, &code) {
		return
	} else if ErrIsServiceStop(err) {
		return ErrCodeServiceStop
	} else {
		return ErrCodeInternalError
	}
}
