package gactor

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Context 表示 Actor 需要处理的请求上下文.
type Context struct {
	// 集成 context.Context
	ctx    context.Context
	cancel context.CancelFunc

	svc      *Service       // Service.
	req      request        // request.
	actor    actorImpl      // actor.
	kv       map[string]any // kv mapping.
	refCount int            // 引用计数.

	handlers   []HandlerFunc // handlers.
	handlerIdx int8          // handler index.
}

var poolOfContext = &sync.Pool{
	New: func() any {
		return &Context{
			handlerIdx: -1,
		}
	},
}

func newContext(ctx context.Context, cancel context.CancelFunc, svc *Service, req request) *Context {
	cp := poolOfContext.Get().(*Context)
	cp.ctx = ctx
	cp.cancel = cancel
	cp.svc = svc
	cp.req = req
	cp.kv = make(map[string]any)
	cp.refCount = 1
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

// ref 增加引用计数.
func (c *Context) ref() {
	c.refCount++
}

// deref 减少引用计数.
func (c *Context) deref() {
	if c.refCount > 0 {
		c.refCount--
	}
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
func (c *Context) Next() error {
	if c.handlerIdx >= int8(len(c.handlers)-1) {
		return nil
	}

	c.handlerIdx++
	for c.handlerIdx < int8(len(c.handlers)) {
		if err := c.handlers[c.handlerIdx](c); err != nil {
			if errors.Is(err, ErrSuspendNextHandlers) {
				return nil
			}
			c.Abort()
			return err
		}
		c.handlerIdx++
	}

	return nil
}

// RPC Service.rpc 的快捷方式.
func (c *Context) RPC(to ActorUID, params any, reply any) error {
	return c.RPCWithContext(c, to, params, reply)
}

// RPCWithContext Service.rpc 的快捷方式, 可提供其它 context.
func (c *Context) RPCWithContext(ctx context.Context, to ActorUID, params any, reply any) error {
	return c.svc.rpc(ctx, to, params, reply)
}

// ContextRPCCallback 基于 Context 的 RPC 回调.
type ContextRPCCallback func(ctx *Context, call RPCCall)

// contextAsyncRPCCallback 基于 Context 的异步 RPC 回调.
type contextAsyncRPCCallback struct {
	ctx *Context
	cb  ContextRPCCallback
}

func (cb *contextAsyncRPCCallback) callback(_ Actor, call RPCCall) {
	// 优先执行回调.
	cb.cb(cb.ctx, call)
	// 继续执行 Handler.
	if err := cb.ctx.Next(); err != nil {
		cb.ctx.actor.core().getLogger().ErrorFields("continue handle request failed inside async rpc callback", lfdRequestType(cb.ctx.req.requestType()), lfdError(err))
	}
	// 回收.
	cb.ctx.release()
}

// AsyncRPC 基于 Context 的异步 RPC 调用.
func (c *Context) AsyncRPC(to ActorUID, params any, callback ContextRPCCallback) error {
	return c.AsyncRPCWithContext(c, to, params, callback)
}

// AsyncRPCWithContext Service.asyncRPC 的快捷方式, 可提供其它 context.
func (c *Context) AsyncRPCWithContext(ctx context.Context, to ActorUID, params any, callback ContextRPCCallback) error {
	if c.actor == nil {
		return errors.New("gactor: context is not bound to actor")
	}
	c.ref()
	asyncCallback := &contextAsyncRPCCallback{
		ctx: c,
		cb:  callback,
	}
	if err := c.actor.AsyncRPC(c, to, params, asyncCallback.callback); err != nil {
		c.deref()
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

	ctx, cancel := c.cloneContext()
	cp.ctx = ctx
	cp.cancel = cancel

	cp.svc = c.svc
	cp.actor = c.actor
	cp.req = c.req.clone(c)
	cp.kv = c.cloneKV()
	cp.refCount = 1
	cp.handlers = c.handlers
	cp.handlerIdx = c.handlerIdx

	return cp
}

// cloneContext 复制 context.Context.
func (c *Context) cloneContext() (context.Context, context.CancelFunc) {
	if c.cancel == nil {
		return c.ctx, nil
	} else {
		deadline, _ := c.ctx.Deadline()
		return context.WithDeadline(context.Background(), deadline)
	}
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
	if c.refCount <= 0 {
		return
	}
	c.refCount--
	if c.refCount > 0 {
		return
	}
	c.req.release(c)
	c.svc = nil
	c.req = nil
	c.actor = nil
	c.kv = nil
	c.handlers = nil
	c.handlerIdx = -1
	if c.cancel != nil {
		c.cancel()
		c.cancel = nil
	}
	c.ctx = nil

	poolOfContext.Put(c)
}

// handle 实现 message.
func (c *Context) handle(a actorImpl) error {
	if err := c.ctx.Err(); err != nil {
		return err
	}

	c.actor = a
	if err := c.req.beforeHandle(c); err != nil {
		return err
	}

	core := a.core()
	if err := core.Handler(c); err != nil {
		core.getLogger().ErrorFields("handle request failed", lfdRequestType(c.req.requestType()), lfdError(err))
	}

	return nil
}

// handleError 实现 message.
func (c *Context) handleError(a actorImpl, err error) error {
	if err := c.ctx.Err(); err != nil {
		return err
	}

	c.actor = a

	// 如果返回的是错误码.
	var ec errCode
	if errors.As(err, &ec) {
		return c.req.replyError(c, ec)
	}

	a.core().getLogger().ErrorFields("handle request with error", lfdRequestType(c.req.requestType()), lfdError(err))
	return c.req.replyError(c, errCodeInternalError)
}

func (c *Context) Deadline() (deadline time.Time, ok bool) {
	return c.ctx.Deadline()
}

func (c *Context) Done() <-chan struct{} {
	return c.ctx.Done()
}

func (c *Context) Err() error {
	return c.ctx.Err()
}

func (c *Context) Value(key any) any {
	return c.ctx.Value(key)
}
