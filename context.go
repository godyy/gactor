package gactor

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Context 表示 Actor 需要处理的请求上下文.
type Context struct {
	// 集成 context.Context
	ctx context.Context

	svc   *Service       // Service.
	req   request        // request.
	actor actorImpl      // actor.
	kv    map[string]any // kv mapping.

	handlers   []HandlerFunc // handlers.
	handlerIdx int8          // handler index.
	suspend    bool          // 是否挂起.
}

var poolOfContext = &sync.Pool{
	New: func() any {
		return &Context{
			handlerIdx: -1,
		}
	},
}

func newContext(svc *Service, req request) *Context {
	return newContextWithCtx(context.Background(), svc, req)
}

func newContextWithCtx(ctx context.Context, svc *Service, req request) *Context {
	cp := poolOfContext.Get().(*Context)
	cp.ctx = ctx
	cp.svc = svc
	cp.req = req
	cp.kv = make(map[string]any)
	cp.suspend = false
	return cp
}

// service 返回 Service.
func (c *Context) service() *Service {
	return c.svc
}

// Actor 返回 Actor.
func (c *Context) Actor() Actor {
	return c.actor
}

// Set 设置 k->v.
func (c *Context) Set(k string, v any) {
	c.kv[k] = v
}

// Get 获取 k->v.
func (c *Context) Get(k string) (v any, exists bool) {
	v, exists = c.kv[k]
	return
}

// RequestType 返回请求类型.
func (c *Context) RequestType() RequestType {
	return c.req.requestType()
}

// Decode 解码请求负载数据到 v 指向的数据结构中.
// * 不可重复调用.
func (c *Context) Decode(v any) error {
	return c.req.decode(c, v)
}

// Reply 回复请求. 参数为负载数据实体.
func (c *Context) Reply(v any) error {
	return c.req.reply(c, v)
}

// ReplyDecodeError 回复解码错误.
func (c *Context) ReplyDecodeError() error {
	return c.req.replyDecodeError(c)
}

// Abort 终止 HandlerChain.
func (c *Context) Abort() {
	c.handlerIdx = maxHandlers
}

// Next 执行下一个 Handler.
func (c *Context) Next() {
	if c.suspend || c.handlerIdx >= int8(len(c.handlers)-1) {
		return
	}

	c.handlerIdx++
	for c.handlerIdx < int8(len(c.handlers)) {
		c.handlers[c.handlerIdx](c)
		if c.suspend {
			return
		}
		c.handlerIdx++
	}
}

// RPC Service.rpc 的快捷方式.
func (c *Context) RPC(to ActorUID, params any, reply any) error {
	return c.RPCWithContext(c, to, params, reply)
}

// RPCWithContext Service.rpc 的快捷方式, 可提供其它 context.
func (c *Context) RPCWithContext(ctx context.Context, to ActorUID, params any, reply any) error {
	return c.svc.rpc(ctx, to, params, reply)
}

// ContextRPCFunc 基于 Context 的 RPC 回调.
type ContextRPCFunc func(ctx *Context, resp *RPCResp)

// contextAsyncRPCFunc 基于 Context 的异步 RPC 回调.
type contextAsyncRPCFunc struct {
	ctx *Context
	cb  ContextRPCFunc
}

func (f *contextAsyncRPCFunc) invoke(_ Actor, resp *RPCResp) {
	// 优先执行回调.
	f.cb(f.ctx, resp)
	// 继续执行 Handler.
	f.ctx.suspend = false
	f.ctx.Next()
	// 回收.
	f.ctx.release()
}

// AsyncRPC 基于 Context 的异步 RPC 调用.
func (c *Context) AsyncRPC(to ActorUID, params any, cb ContextRPCFunc) error {
	return c.AsyncRPCWithContext(c, to, params, cb)
}

// AsyncRPCWithContext Service.asyncRPC 的快捷方式, 可提供其它 context.
func (c *Context) AsyncRPCWithContext(ctx context.Context, to ActorUID, params any, cb ContextRPCFunc) error {
	if c.actor == nil {
		return errors.New("gactor: context is not bound to actor")
	}
	c.suspend = true
	asyncFunc := &contextAsyncRPCFunc{
		ctx: c,
		cb:  cb,
	}
	if err := c.actor.AsyncRPC(c, to, params, asyncFunc.invoke); err != nil {
		c.suspend = false
		return err
	}
	return nil
}

// Cast Service.cast 的快捷方式.
func (c *Context) Cast(to ActorUID, payload any) error {
	return c.CastWithContext(c, to, payload)
}

// CastWithContext Service.cast 的快捷方式, 可提供其它 context.
func (c *Context) CastWithContext(ctx context.Context, to ActorUID, payload any) error {
	return c.svc.cast(ctx, to, payload)
}

// Clone 复制 Context.
func (c *Context) Clone() *Context {
	cp := &Context{}
	cp.ctx = c.ctx
	cp.svc = c.svc
	cp.actor = c.actor
	cp.req = c.req.clone(c)
	cp.kv = c.cloneKV()
	cp.handlers = c.handlers
	cp.handlerIdx = c.handlerIdx
	cp.suspend = false

	return cp
}

// cloneKV 复制 kv mapping.
func (c *Context) cloneKV() map[string]any {
	kv := make(map[string]any, len(c.kv))
	for k, v := range c.kv {
		kv[k] = v
	}
	return kv
}

// release 回收.
func (c *Context) release() {
	if c.suspend {
		return
	}
	c.req.release(c)
	c.svc = nil
	c.req = nil
	c.actor = nil
	c.kv = nil
	c.handlers = nil
	c.handlerIdx = -1
	c.ctx = nil
	poolOfContext.Put(c)
}

// beforeHandle 处理请求前的检查.
func (c *Context) beforeHandle(a actorImpl) bool {
	if c.ctx != nil {
		if err := c.ctx.Err(); err != nil {
			a.core().getLogger().WarnFields("[BeforeHandleRequest] context canceled", zap.Object("req", c.req), lfdError(err))
			return false
		}
	}

	if c.req.isTimeout(time.Now().UnixMilli()) {
		a.core().getLogger().WarnFields("[BeforeHandleRequest] request timeout", zap.Object("req", c.req), lfdRequestType(c.req.requestType()))
		return false
	}

	c.actor = a
	return true
}

// handle 实现 message.
func (c *Context) handle(a actorImpl) {
	if !c.beforeHandle(a) {
		return
	}

	if err := c.req.beforeHandle(c); err != nil {
		if errors.Is(err, errSkipHandleRequest) {
			err = nil
		}
		if err != nil {
			a.core().getLogger().ErrorFields("[HandleRequest] request before-handle check failed", zap.Object("req", c.req), lfdError(err))
		}
		return
	}

	core := a.core()
	core.Handler(c)
}

// handleError 实现 message.
func (c *Context) handleError(a actorImpl, err error) {
	if !c.beforeHandle(a) {
		return
	}

	// 如果返回的是错误码.
	var ec errCode
	if errors.As(err, &ec) {
		if err := c.req.replyError(c, ec); err != nil {
			a.core().getLogger().ErrorFields("[HandleRequest] reply error", zap.Object("req", c.req), lfdError(err))
		}
		return
	}

	if err := c.req.replyError(c, errCodeInternalError); err != nil {
		a.core().getLogger().ErrorFields("[HandleRequest] reply internal error", zap.Object("req", c.req), lfdError(err))
	}
}

func (c *Context) Deadline() (deadline time.Time, ok bool) {
	return
}

func (c *Context) Done() <-chan struct{} {
	return nil
}

func (c *Context) Err() error {
	return nil
}

func (c *Context) Value(key any) any {
	return nil
}
