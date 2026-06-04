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
	ErrCodeOK                 = ErrCode(iota) // OK.
	ErrCodeInternalError                      // 内部错误.
	ErrCodeEncodePacketFailed                 // 编码数据包失败.
	ErrCodeDecodePacketFailed                 // 解码数据包失败.
)

const (
	_                        = ErrCode(iota + 10000)
	ErrCodeServiceNotStarted // 服务未启动
	ErrCodeServiceStopped    // 服务已停止
)

const (
	_                               = ErrCode(iota + 20000)
	ErrCodeActorUndefined                   // Actor 未定义.
	ErrCodeActorNotExists                   // Actor 不存在.
	ErrCodeActorRegisteredByAnother         // Actor 被其它节点注册.
	ErrCodeActorStartFailed                 // Actor 启动失败.
	ErrCodeActorBusy                        // Actor 繁忙.
	ErrCodeActorLoopError                   // Actor 循环错误.
	ErrCodeActorNotConnected                // Actor 未连接.
	ErrCodeActorConnectedByAnother  = 20008 // Actor 被其他人连接.
)

// errCodeStrings 错误码文本.
var errCodeStrings = map[ErrCode]string{
	ErrCodeOK:                 "ok",
	ErrCodeInternalError:      "internal error",
	ErrCodeEncodePacketFailed: "encode packet failed",
	ErrCodeDecodePacketFailed: "decode packet failed",

	ErrCodeServiceNotStarted: "service not started",
	ErrCodeServiceStopped:    "service stopped",

	ErrCodeActorUndefined:           "actor undefined",
	ErrCodeActorNotExists:           "actor not exists",
	ErrCodeActorRegisteredByAnother: "actor registered by another",
	ErrCodeActorStartFailed:         "actor start failed",
	ErrCodeActorBusy:                "actor busy",
	ErrCodeActorLoopError:           "actor loop error",
	ErrCodeActorNotConnected:        "actor not connected",
	ErrCodeActorConnectedByAnother:  "actor connected by another",
}

func (ec ErrCode) String() string {
	return fmt.Sprintf("%d - %s", ec, errCodeStrings[ec])
}

func (ec ErrCode) Error() string {
	if _, ok := errCodeStrings[ec]; ok {
		return "gactor: " + ec.String()
	} else {
		return fmt.Sprintf("gactor: unknown error code %d", ec)
	}
}

var err2ErrCode = map[error]ErrCode{
	ErrActorNotExists:    ErrCodeActorNotExists,
	errServiceNotStarted: ErrCodeServiceNotStarted,
	errServiceStopping:   ErrCodeServiceStopped,
	errServiceStopped:    ErrCodeServiceStopped,
}

// Err2ErrCode 将 error 转换为 errCode.
func Err2ErrCode(err error) (code ErrCode) {
	if errors.As(err, &code) {
		return
	}
	if code, ok := err2ErrCode[err]; ok {
		return code
	}
	return ErrCodeInternalError
}
