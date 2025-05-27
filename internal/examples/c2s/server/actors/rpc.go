package actors

import (
	"context"

	"github.com/godyy/gactor"
	"github.com/godyy/gactor/internal/examples/c2s/common/message"
)

func RPC(a gactor.Actor, ctx context.Context, to gactor.ActorUID, params any, reply any) error {
	var msgParams message.RpcMessage
	var msgReply message.RpcRespMessage
	msgParams = message.NewRpcMessageWithPayload(params)
	if err := a.RPC(ctx, to, &msgParams, &msgReply); err != nil {
		return err
	} else if err := msgReply.DecodePayload(reply); err != nil {
		return err
	} else {
		return nil
	}
}

func AsyncRPC(a gactor.Actor, ctx context.Context, to gactor.ActorUID, params any, callback gactor.ActorRPCFunc) error {
	var msgParams message.RpcMessage
	msgParams = message.NewRpcMessageWithPayload(params)
	return a.AsyncRPC(ctx, to, &msgParams, callback)
}

func WrapRPCCallback[AB gactor.ActorBehavior, R any](callback func(ab AB, reply *R, err error)) gactor.ActorRPCFunc {
	return func(a gactor.Actor, resp *gactor.RPCResp) {
		var msgReply message.RpcRespMessage
		var reply R
		if err := resp.DecodeReply(&msgReply); err != nil {
			callback(a.Behavior().(AB), nil, err)
		} else if err := msgReply.DecodePayload(&reply); err != nil {
			callback(a.Behavior().(AB), nil, err)
		} else {
			callback(a.Behavior().(AB), &reply, nil)
		}
	}
}

func ContextRPC(ctx *gactor.Context, to gactor.ActorUID, params any, reply any) error {
	var msgParams message.RpcMessage
	var msgReply message.RpcRespMessage
	msgParams = message.NewRpcMessageWithPayload(params)
	if err := ctx.RPC(to, &msgParams, &msgReply); err != nil {
		return err
	} else if err := msgReply.DecodePayload(reply); err != nil {
		return err
	} else {
		return nil
	}
}

func ContextAsyncRPC(ctx *gactor.Context, to gactor.ActorUID, params any, callback gactor.ContextRPCFunc) error {
	var msgParams message.RpcMessage
	msgParams = message.NewRpcMessageWithPayload(params)
	return ctx.AsyncRPC(to, &msgParams, callback)
}

func WrapContextRPCCallback[R any](callback func(ctx *gactor.Context, reply *R, err error)) gactor.ContextRPCFunc {
	return func(ctx *gactor.Context, resp *gactor.RPCResp) {
		var msgReply message.RpcRespMessage
		var reply R
		if err := resp.DecodeReply(&msgReply); err != nil {
			callback(ctx, nil, err)
		} else if err := msgReply.DecodePayload(&reply); err != nil {
			callback(ctx, nil, err)
		} else {
			callback(ctx, &reply, nil)
		}
	}
}
