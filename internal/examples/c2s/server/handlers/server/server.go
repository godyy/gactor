package server

import (
	"fmt"

	"github.com/godyy/gactor"
	"github.com/godyy/gactor/internal/examples/c2s/common/consts"
	"github.com/godyy/gactor/internal/examples/c2s/common/logger"
	"github.com/godyy/gactor/internal/examples/c2s/common/message"
	"github.com/godyy/gactor/internal/examples/c2s/server/actors"
	"github.com/godyy/gactor/internal/examples/c2s/server/handlers"
	"go.uber.org/zap"
)

type handler struct {
	*handlers.RpcHandler

	rpcGetUserNameCallback gactor.ContextRPCFunc
}

func (h *handler) server(ctx *gactor.Context) *actors.Server {
	return ctx.Actor().Behavior().(*actors.Server)
}

func (h *handler) initRPCCallback() {
	h.rpcGetUserNameCallback = actors.WrapContextRPCCallback(h.rpcUserNameCallback)
}

func (h *handler) registerHandlers() {
	h.RegisterHandler(message.MsgIdGetNameReq, h.wrapHandler(handlers.WrapRpcMessageHandler(h.handleServerName)))
}

func (h *handler) Handle(ctx *gactor.Context) {
	switch ctx.RequestType() {
	case gactor.RequestTypeRPC:
		h.RpcHandler.Handle(ctx)
	default:
		panic(fmt.Errorf("server handle unknown request type: %s", ctx.RequestType()))
	}
}

func (h *handler) begin(ctx *gactor.Context) {
	logger.Logger().DebugFields("server handler begin", zap.Int64("id", ctx.Actor().ActorUID().ID))
}

func (h *handler) end(ctx *gactor.Context) {
	logger.Logger().DebugFields("server handler end", zap.Int64("id", ctx.Actor().ActorUID().ID))
}

func (h *handler) wrapHandler(handlers ...gactor.HandlerFunc) gactor.HandlersChain {
	return gactor.NewHandlersChain(h.begin).Append(handlers...).Append(h.end)
}

func (h *handler) handleServerName(ctx *gactor.Context, msg *message.GetNameReq) {
	logger.Logger().DebugFields("server handle serverName", zap.Int64("id", ctx.Actor().ActorUID().ID))
	if err := actors.ContextAsyncRPC(
		ctx,
		gactor.ActorUID{Category: consts.CategoryUser, ID: 1},
		&message.GetNameReq{},
		h.rpcGetUserNameCallback); err != nil {
		logger.Logger().ErrorFields("server async rpc call user name failed", zap.Int64("id", ctx.Actor().ActorUID().ID), zap.Error(err))
		ctx.Abort()
	}
}

func (h *handler) rpcUserNameCallback(ctx *gactor.Context, resp *message.GetNameResp, err error) {
	if err != nil {
		logger.Logger().ErrorFields("server on get user name callback", zap.Int64("id", ctx.Actor().ActorUID().ID), zap.Error(err))
		ctx.Abort()
		return
	}

	logger.Logger().DebugFields("server get user name", zap.Int64("id", ctx.Actor().ActorUID().ID), zap.String("user name", resp.Name))
	server := h.server(ctx)
	h.RpcHandler.Reply(ctx, &message.GetNameResp{Name: server.Name()})
}

var serverHandler = &handler{
	RpcHandler: handlers.NewRpcHandler(),
}

func Handler() gactor.HandlerFunc {
	return serverHandler.Handle
}

func init() {
	serverHandler.initRPCCallback()
	serverHandler.registerHandlers()
}
