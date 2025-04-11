package user

import (
	"fmt"
	"time"

	"github.com/godyy/gactor"
	"github.com/godyy/gactor/internal/examples/c2s/common"
	"github.com/godyy/gactor/internal/examples/c2s/common/consts"
	"github.com/godyy/gactor/internal/examples/c2s/common/logger"
	"github.com/godyy/gactor/internal/examples/c2s/common/message"
	"github.com/godyy/gactor/internal/examples/c2s/server/actors"
	"github.com/godyy/gactor/internal/examples/c2s/server/handlers"
	"go.uber.org/zap"
)

type handler struct {
	*handlers.RawHandler
	*handlers.RpcHandler

	rpcGetServerNameCallback gactor.ContextRPCCallback
}

func (h *handler) user(ctx *gactor.Context) *actors.User {
	return ctx.Actor().Behavior().(*actors.User)
}

func (h *handler) initRPCCallback() {
	h.rpcGetServerNameCallback = actors.WrapContextRPCCallback(h.rpcServerNameCallback)
}

func (h *handler) registerHandlers() {
	h.RawHandler.RegisterHandler(message.MsgIdLoginReq, h.wrapHandler(handlers.WrapRawMessageHandler(h.handleLogin)))
	h.RawHandler.RegisterHandler(message.MsgIdHeartbeatReq, h.wrapHandlerWithCheckLogin(handlers.WrapRawMessageHandler(h.handleHeartbeat)))

	h.RpcHandler.RegisterHandler(message.MsgIdGetNameReq, h.wrapHandler(handlers.WrapRpcMessageHandler(h.handleGetName)))
}

func (h *handler) Handle(ctx *gactor.Context) error {
	switch ctx.RequestType() {
	case gactor.RequestTypeReq:
		return h.RawHandler.Handle(ctx)
	case gactor.RequestTypeRPC:
		return h.RpcHandler.Handle(ctx)
	default:
		return fmt.Errorf("user handle unknown request type: %s", ctx.RequestType())
	}
}

func (h *handler) begin(ctx *gactor.Context) error {
	logger.Logger().DebugFields("user handler begin", zap.Int64("id", ctx.Actor().ActorUID().ID))
	return nil
}

func (h *handler) end(ctx *gactor.Context) error {
	logger.Logger().DebugFields("user handler end", zap.Int64("id", ctx.Actor().ActorUID().ID))
	return nil
}

func (h *handler) wrapHandler(handlers ...gactor.HandlerFunc) gactor.HandlersChain {
	return gactor.NewHandlersChain(h.begin).Append(handlers...).Append(h.end)
}

func (h *handler) wrapHandlerWithCheckLogin(handlers ...gactor.HandlerFunc) gactor.HandlersChain {
	return gactor.NewHandlersChain(h.begin, h.checkLogin).Append(handlers...).Append(h.end)
}

func (h *handler) checkLogin(ctx *gactor.Context) error {
	u := h.user(ctx)
	if !u.IsLogin {
		ctx.Abort()
		err := h.RawHandler.ReplyError(ctx, common.ErrCodeNotLogin)
		u.Disconnect(ctx)
		return err
	}
	return nil
}

func (h *handler) handleLogin(ctx *gactor.Context, params *message.LoginReq) error {
	logger.Logger().DebugFields("user handle login", zap.Int64("id", ctx.Actor().ActorUID().ID))
	if params.Password != fmt.Sprintf("password%d", ctx.Actor().ActorUID().ID) {
		respMsg := message.LoginResp{
			Err: "password error",
		}
		return h.RawHandler.Reply(ctx, &respMsg)
	}

	u := h.user(ctx)
	u.SetName(params.Username)
	u.StartNotifyTimer()
	u.IsLogin = true

	respMsg := message.LoginResp{}
	return h.RawHandler.Reply(ctx, &respMsg)
}

func (h *handler) handleHeartbeat(ctx *gactor.Context, params *message.HeartbeatReq) error {
	logger.Logger().DebugFields("user handle heartbeat", zap.Int64("id", ctx.Actor().ActorUID().ID))
	if err := actors.ContextAsyncRPC(
		ctx,
		gactor.ActorUID{Category: consts.CategoryServer, ID: actors.ServerId},
		&message.GetNameReq{},
		h.rpcGetServerNameCallback); err != nil {
		logger.Logger().ErrorFields("user async rpc call server name failed", zap.Int64("id", ctx.Actor().ActorUID().ID), zap.Error(err))
		ctx.Abort()
		return nil
	} else {
		return gactor.ErrSuspendNextHandlers
	}
}

func (h *handler) handleGetName(ctx *gactor.Context, params *message.GetNameReq) error {
	logger.Logger().DebugFields("user handle get name", zap.Int64("id", ctx.Actor().ActorUID().ID))
	u := h.user(ctx)
	return h.RpcHandler.Reply(ctx, &message.GetNameResp{Name: u.GetName()})
}

func (h *handler) rpcServerNameCallback(ctx *gactor.Context, resp *message.GetNameResp, err error) {
	if err != nil {
		logger.Logger().ErrorFields("user on get server name callback", zap.Int64("id", ctx.Actor().ActorUID().ID), zap.Error(err))
		ctx.Abort()
		return
	}

	logger.Logger().DebugFields("user get server name", zap.Int64("id", ctx.Actor().ActorUID().ID), zap.String("server name", resp.Name))
	h.RawHandler.Reply(ctx, &message.HeartbeatResp{Ts: time.Now().Unix()})
}

var userHandler = &handler{
	RawHandler: handlers.NewRawHandler(),
	RpcHandler: handlers.NewRpcHandler(),
}

func Handler() gactor.HandlerFunc {
	return userHandler.Handle
}

func init() {
	userHandler.initRPCCallback()
	userHandler.registerHandlers()
}
