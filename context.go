package gactor

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/godyy/gutils/deepcopy"
	"go.uber.org/zap"
)

// Context 表示 Actor 需要处理的请求上下文.
type Context struct {
	svc   *Service  // Service.
	req   request   // request.
	actor actorImpl // actor.
	kv    contextKV // kv mapping.

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
	cp := poolOfContext.Get().(*Context)
	cp.svc = svc
	cp.req = req
	cp.kv = make(contextKV)
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
func (c *Context) Set(k any, v any) {
	c.kv[k] = v
}

// Get 获取 k->v.
func (c *Context) Get(k any) (v any, exists bool) {
	v, exists = c.kv[k]
	return
}

// RequestType 返回请求类型.
func (c *Context) RequestType() RequestType {
	return c.req.requestType()
}

// FromActorUID 返回请求来源 Actor ID.
func (c *Context) FromActorUID() ActorUID {
	return c.req.fromActorUID()
}

// Decode 解码请求负载数据到 v 指向的数据结构中.
// * 不可重复调用.
func (c *Context) Decode(v any) error {
	return c.req.decode(c, v)
}

// Reply 回复请求. 参数为负载数据实体.
func (c *Context) Reply(v any) error {
	return c.req.reply(c, v, ErrCodeOK)
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

// RPCWithDeadline Actor.RPCWithDeadline 的快捷方式.
func (c *Context) RPCWithDeadline(to ActorUID, params any, reply any, deadline time.Time) error {
	return c.actor.RPCWithDeadline(to, params, reply, deadline)
}

// RPCWithTimeout Actor.RPCWithTimeout 的快捷方式.
func (c *Context) RPCWithTimeout(to ActorUID, params any, reply any, timeout time.Duration) error {
	return c.actor.RPCWithTimeout(to, params, reply, timeout)
}

// RPC 基于 Context 的 RPC 调用.
func (c *Context) RPC(to ActorUID, params any, reply any) error {
	if deadline, ok := c.Deadline(); ok {
		return c.actor.RPCWithDeadline(to, params, reply, deadline)
	}
	return c.actor.RPC(to, params, reply)
}

// RPCWithContext Actor.RPCWithContext 的快捷方式.
func (c *Context) RPCWithContext(ctx context.Context, to ActorUID, params any, reply any) error {
	return c.actor.RPCWithContext(ctx, to, params, reply)
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

// AsyncRPCWithDeadline 基于 Context 的异步 RPC 调用.
func (c *Context) AsyncRPCWithDeadline(to ActorUID, params any, cb ContextRPCFunc, deadline time.Time) error {
	c.suspend = true
	asyncFunc := &contextAsyncRPCFunc{
		ctx: c,
		cb:  cb,
	}
	if err := c.actor.AsyncRPCWithDeadline(to, params, asyncFunc.invoke, deadline); err != nil {
		c.suspend = false
		return err
	}
	return nil
}

// AsyncRPCWithTimeout 基于 Context 的异步 RPC 调用.
func (c *Context) AsyncRPCWithTimeout(to ActorUID, params any, cb ContextRPCFunc, timeout time.Duration) error {
	return c.AsyncRPCWithDeadline(to, params, cb, time.Now().Add(timeout))
}

// AsyncRPC 基于 Context 的异步 RPC 调用.
func (c *Context) AsyncRPC(to ActorUID, params any, cb ContextRPCFunc) error {
	if deadline, ok := c.Deadline(); ok {
		return c.AsyncRPCWithDeadline(to, params, cb, deadline)
	}
	c.suspend = true
	asyncFunc := &contextAsyncRPCFunc{
		ctx: c,
		cb:  cb,
	}
	if err := c.actor.AsyncRPC(to, params, asyncFunc.invoke); err != nil {
		c.suspend = false
		return err
	}
	return nil
}

// AsyncRPCWithContext 基于 Context 的异步 RPC 调用.
func (c *Context) AsyncRPCWithContext(ctx context.Context, to ActorUID, params any, cb ContextRPCFunc) error {
	c.suspend = true
	asyncFunc := &contextAsyncRPCFunc{
		ctx: c,
		cb:  cb,
	}
	if err := c.actor.AsyncRPCWithContext(ctx, to, params, asyncFunc.invoke); err != nil {
		c.suspend = false
		return err
	}
	return nil
}

// Cast Service.cast 的快捷方式.
func (c *Context) Cast(to ActorUID, payload any) error {
	return c.svc.cast(c.actor.ActorUID(), to, payload)
}

// Clone 复制 Context.
func (c *Context) Clone() *Context {
	cp := &Context{}
	cp.svc = c.svc
	cp.actor = c.actor
	cp.req = c.req.clone(c)
	cp.kv = c.kv.clone()
	cp.handlers = c.handlers
	cp.handlerIdx = c.handlerIdx
	cp.suspend = false

	return cp
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
	poolOfContext.Put(c)
}

// beforeHandle 处理请求前的检查.
func (c *Context) beforeHandle(a actorImpl) bool {
	if c.req.isTimeout(time.Now()) {
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

	c.svc.getCfg().ActorConfig.Handler(c)
}

// handleError 实现 message.
func (c *Context) handleError(a actorImpl, err error) {
	if !c.beforeHandle(a) {
		return
	}

	// 返回错误码.
	errCode := Err2ErrCode(err)
	if err := c.req.reply(c, nil, errCode); err != nil {
		a.core().getLogger().ErrorFields("[HandleRequest] reply error", zap.Object("req", c.req), lfdError(err))
	}
}

func (c *Context) Deadline() (deadline time.Time, ok bool) {
	return c.req.deadline()
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

// contextKV 上下文键值对.
type contextKV map[any]any

func (kv contextKV) clone() contextKV {
	cp := make(contextKV, len(kv))
	for k, v := range kv {
		cp[k] = deepcopy.Copy(v)
	}
	return cp
}
