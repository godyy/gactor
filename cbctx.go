package gactor

import (
	"sync"

	"github.com/godyy/gutils/container/kv"
)

var (
	cbctxKeyUID      = kv.NewK[ActorUID]("uid")       // Actor UID.
	cbctxKeyDone     = kv.NewK[chan struct{}]("done") // 回调完成通道.
	cbctxKeyPayload  = kv.NewK[*Buffer]("payload")    // 负载数据.
	cbctxKeyErr      = kv.NewK[error]("err")          // 错误信息.
	cbctxKeyCallback = kv.NewK[any]("callback")       // 回调函数.
	cbctxKeyCbCtx    = kv.NewK[any]("cbCtx")          // 用于嵌套回调上下文.
	cbctxKeySvc      = kv.NewK[*Service]("svc")       // Service.
)

// poolOfCbCtx 回调上下文池.
var poolOfCbCtx = &sync.Pool{
	New: func() any {
		return kv.NewM()
	},
}

// NewCbCtx 创建新的回调上下文.
// pairs 为键值对, 用于初始化上下文.
// 若 pairs 为 nil, 则不初始化上下文.
func NewCbCtx(pairs ...any) *kv.M {
	cc := poolOfCbCtx.Get().(*kv.M)
	cc.SetPairs(pairs...)
	return cc
}

// CbCtxRelease 释放回调上下文.
func CbCtxRelease(km *kv.M) {
	km.Clear()
	poolOfCbCtx.Put(km)
}
