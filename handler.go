package gactor

import (
	"math"
)

// HandlerFunc Actor 请求处理器函数.
type HandlerFunc func(*Context)

// maxHandlers 最大 HandlerFunc 数量.
const maxHandlers = math.MaxInt8 >> 1

// HandlersChain 是 HandlerFunc 的链式调用封装.
type HandlersChain struct {
	handlers []HandlerFunc
}

// NewHandlersChain 构造 HandlersChain.
func NewHandlersChain(handlers ...HandlerFunc) HandlersChain {
	return HandlersChain{}.Append(handlers...)
}

// Append 追加 HandlerFunc 到 HandlersChain.
func (h HandlersChain) Append(handlers ...HandlerFunc) HandlersChain {
	if len(h.handlers)+len(handlers) > maxHandlers {
		panic("gactor: too many handlers")
	}
	h.handlers = append(h.handlers, handlers...)
	return h
}

// Handle 执行 HandlersChain.
func (hc HandlersChain) Handle(ctx *Context) {
	ctx.handlers = hc.handlers
	ctx.Next()
}
