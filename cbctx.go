package gactor

import (
	"fmt"
	"reflect"
	"sync"
)

const (
	cbctxKeyUID      = "uid"      // Actor UID.
	cbctxKeyDone     = "done"     // 回调完成通道.
	cbctxKeyPayload  = "payload"  // 负载数据
	cbctxKeyErr      = "err"      // 错误信息
	cbctxKeyCallback = "callback" // 回调函数.
	cbctxKeyCbCtx    = "cbCtx"    // 用于嵌套回调上下文.
	cbctxKeySvc      = "svc"      // Service.
)

// CbCtx 回调上下文.
type CbCtx map[any]any

// poolOfCbCtx 回调上下文池.
var poolOfCbCtx = &sync.Pool{
	New: func() any {
		return make(CbCtx)
	},
}

// NewCbCtx 创建新的回调上下文.
// pairs 为键值对, 用于初始化上下文.
// 若 pairs 为 nil, 则不初始化上下文.
func NewCbCtx(pairs ...any) CbCtx {
	if len(pairs)%2 != 0 {
		panic("gactor: NewCbCtx: pairs must be even")
	}
	cc := poolOfCbCtx.Get().(CbCtx)
	for i := 0; i < len(pairs); i += 2 {
		cc[pairs[i]] = pairs[i+1]
	}
	return cc
}

// Release 释放回调上下文.
func (cc CbCtx) Release() {
	clear(cc)
	poolOfCbCtx.Put(cc)
}

// Set 设置回调上下文中的值.
func (cc CbCtx) Set(key, value any) CbCtx {
	cc[key] = value
	return cc
}

// CbCtxGet 获取回调上下文中的值.
// 若值不存在, 则返回零值和 false.
// 若值存在, 则返回值和 true.
func CbCtxGet[Value any](cc CbCtx, key any) (v Value, ok bool) {
	if vv, exist := cc[key]; !exist {
		return v, false
	} else if vv != nil {
		return vv.(Value), true
	} else {
		// 存入的是 nil: 只有可 nil 的类型才能正确承载, 否则说明 key 对应的类型不匹配
		if !canBeNil[Value]() {
			panic(fmt.Sprintf("gactor: CbCtxGet: value of key %v is nil, but %T cannot be nil", key, v))
		}
		return v, true
	}
}

// canBeNil 判断类型 T 的零值是否为 nil, 即 T 能否被赋值为 nil.
// 只有 chan、func、interface、map、pointer、slice、unsafe.Pointer 这几种类型可赋 nil.
func canBeNil[T any]() bool {
	switch reflect.TypeOf((*T)(nil)).Elem().Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice, reflect.UnsafePointer:
		return true
	default:
		return false
	}
}
