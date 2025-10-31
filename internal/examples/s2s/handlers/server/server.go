package server

import (
	"github.com/godyy/gactor"
	"github.com/godyy/gactor/internal/examples/s2s/actors"
	"github.com/godyy/gactor/internal/examples/s2s/handlers"
	"github.com/godyy/gactor/internal/examples/s2s/logger"
	"github.com/godyy/gactor/internal/examples/s2s/message"
	"go.uber.org/zap"
)

type handler struct {
	*handlers.Handler
}

var serverHandler = &handler{
	Handler: handlers.NewHandler(),
}

func (h *handler) server(ctx *gactor.Context) *actors.Server {
	return ctx.Actor().Behavior().(*actors.Server)
}

func (h *handler) registerHandlers() {
	h.RegisterHandler(message.MsgIDGetServerNameReq, h.wrapHandler(handlers.WrapMessageHandler(h.handleGetServerName)))
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

func (h *handler) handleGetServerName(ctx *gactor.Context, params *message.GetServerNameReq) {
	// logger.Logger().DebugFields("server handle get server name", zap.Int64("id", ctx.Actor().ActorUID().ID))
	server := h.server(ctx)
	reply := message.GetServerNameResp{ServerName: server.GetName()}
	h.Reply(ctx, &reply)
}

func Handler() gactor.HandlerFunc {
	return serverHandler.Handle
}

func init() {
	serverHandler.registerHandlers()
}
