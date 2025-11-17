package handlers

import (
	"fmt"

	"github.com/godyy/gactor"
	"github.com/godyy/gactor/internal/examples/c2s/common"
	"github.com/godyy/gactor/internal/examples/c2s/common/logger"
	"github.com/godyy/gactor/internal/examples/c2s/common/message"
)

type RawHandler struct {
	messageHandlers map[uint16]gactor.HandlersChain
}

func NewRawHandler() *RawHandler {
	return &RawHandler{
		messageHandlers: make(map[uint16]gactor.HandlersChain),
	}
}

func (h *RawHandler) RegisterHandler(mgdId uint16, handler gactor.HandlersChain) {
	h.messageHandlers[mgdId] = handler
}

func (h *RawHandler) Handle(ctx *gactor.Context) {
	var msg message.ReqMessage
	if err := ctx.Decode(&msg); err != nil {
		logger.Logger().Errorf("actor %s decode [%s]Request payload, %v", ctx.Actor().ActorUID(), ctx.RequestType(), err)
		ctx.ReplyDecodeError()
		return
	}

	messageHandler, ok := h.messageHandlers[msg.MsgId]
	if !ok {
		panic(fmt.Errorf("actor %s, msgId %d handler not found", ctx.Actor().ActorUID(), msg.MsgId))
	}

	setReqMsg(ctx, &msg)
	messageHandler.Handle(ctx)
}

func (h *RawHandler) Reply(ctx *gactor.Context, v any) error {
	reqMsg := getReqMsg(ctx)
	respMsg := message.NewRespMessageWithPayload(reqMsg.ReqId, v)
	return ctx.Reply(&respMsg)
}

func (h *RawHandler) ReplyError(ctx *gactor.Context, code common.ErrCode) error {
	return h.Reply(ctx, &message.Error{Code: code})
}

func setReqMsg(ctx *gactor.Context, m *message.ReqMessage) {
	ctx.Set("reqMsg", m)
}

func getReqMsg(ctx *gactor.Context) *message.ReqMessage {
	val, exists := ctx.Get("reqMsg")
	if !exists {
		return nil
	}
	return val.(*message.ReqMessage)
}

func WrapRawMessageHandler[MM any](h func(ctx *gactor.Context, msg *MM)) gactor.HandlerFunc {
	return func(ctx *gactor.Context) {
		m := getReqMsg(ctx)
		if m == nil {
			panic("msg not found")
		}

		var mm MM
		if err := m.DecodePayload(&mm); err != nil {
			ctx.Abort()
			ctx.ReplyDecodeError()
			return
		}

		h(ctx, &mm)
	}
}

type RpcHandler struct {
	messageHandlers map[uint16]gactor.HandlersChain
}

func NewRpcHandler() *RpcHandler {
	return &RpcHandler{
		messageHandlers: make(map[uint16]gactor.HandlersChain),
	}
}

func (h *RpcHandler) RegisterHandler(msgId uint16, handler gactor.HandlersChain) {
	h.messageHandlers[msgId] = handler
}

func (h *RpcHandler) Handle(ctx *gactor.Context) {
	var msg message.RpcMessage
	if err := ctx.Decode(&msg); err != nil {
		logger.Logger().Errorf("actor %s decode [%s]Request payload, %v", ctx.Actor().ActorUID(), ctx.RequestType(), err)
		ctx.ReplyDecodeError()
		return
	}

	messageHandler, ok := h.messageHandlers[msg.MsgId]
	if !ok {
		panic(fmt.Errorf("actor %s, msgId %d handler not found", ctx.Actor().ActorUID(), msg.MsgId))
	}

	setRpcMsg(ctx, &msg)
	messageHandler.Handle(ctx)
}

func (h *RpcHandler) Reply(ctx *gactor.Context, v any) error {
	respMsg := message.NewRpcRespMessageWithPayload(v)
	return ctx.Reply(&respMsg)
}

func (h *RpcHandler) ReplyError(ctx *gactor.Context, code common.ErrCode) error {
	return h.Reply(ctx, &message.Error{Code: code})
}

func setRpcMsg(ctx *gactor.Context, m *message.RpcMessage) {
	ctx.Set("rpcMsg", m)
}

func getRpcMsg(ctx *gactor.Context) *message.RpcMessage {
	val, exists := ctx.Get("rpcMsg")
	if !exists {
		return nil
	}
	return val.(*message.RpcMessage)
}

func WrapRpcMessageHandler[MM any](h func(ctx *gactor.Context, msg *MM)) gactor.HandlerFunc {
	return func(ctx *gactor.Context) {
		m := getRpcMsg(ctx)
		if m == nil {
			panic("msg not found")
		}

		var mm MM
		if err := m.DecodePayload(&mm); err != nil {
			ctx.Abort()
			ctx.ReplyDecodeError()
			return
		}

		h(ctx, &mm)
	}
}
