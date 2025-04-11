package handlers

import (
	"errors"
	"fmt"

	"github.com/godyy/gactor"
	"github.com/godyy/gactor/internal/examples/s2s/logger"
	"github.com/godyy/gactor/internal/examples/s2s/message"
)

type Handler struct {
	messageHandlers map[int]gactor.HandlersChain
}

func NewHandler() *Handler {
	return &Handler{
		messageHandlers: make(map[int]gactor.HandlersChain),
	}
}

func (h *Handler) Reply(ctx *gactor.Context, v any) error {
	msg := message.NewMsgWithPayload(v)
	return ctx.Reply(&msg)
}

func (h *Handler) RegisterHandler(mgdId int, handler gactor.HandlersChain) {
	h.messageHandlers[mgdId] = handler
}

func (h *Handler) Handle(ctx *gactor.Context) error {
	var msg message.Msg
	if err := ctx.Decode(&msg); err != nil {
		logger.Logger().Errorf("actor %s decode [%s]Request payload, %v", ctx.Actor().ActorUID(), ctx.RequestType(), err)
		return ctx.ReplyDecodeError()
	}

	messageHandler, ok := h.messageHandlers[msg.ID]
	if !ok {
		return fmt.Errorf("actor %s, msgId %d handler not found", ctx.Actor().ActorUID(), msg.ID)
	}

	setMsg(ctx, &msg)
	return messageHandler.Handle(ctx)
}

func setMsg(ctx *gactor.Context, m *message.Msg) {
	ctx.Set("msg", m)
}

func getMsg(ctx *gactor.Context) *message.Msg {
	val, exists := ctx.Get("msg")
	if !exists {
		return nil
	}
	return val.(*message.Msg)
}

func WrapMessageHandler[MM any](h func(ctx *gactor.Context, msg *MM) error) gactor.HandlerFunc {
	return func(ctx *gactor.Context) error {
		m := getMsg(ctx)
		if m == nil {
			return errors.New("msg not found")
		}

		var mm MM
		if err := m.DecodePayload(&mm); err != nil {
			ctx.Abort()
			return ctx.ReplyDecodeError()
		}

		return h(ctx, &mm)
	}
}
