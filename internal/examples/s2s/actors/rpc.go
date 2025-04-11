package actors

import (
	"context"

	"github.com/godyy/gactor"
	"github.com/godyy/gactor/internal/examples/s2s/message"
)

func WrapRPCCallback[AB gactor.ActorBehavior, R any](callback func(ab AB, reply *R, err error)) gactor.ActorRPCCallback {
	return func(a gactor.Actor, call gactor.RPCCall) {
		var msgReply message.Msg
		var reply R
		if err := call.DecodePayload(&msgReply); err != nil {
			callback(a.Behavior().(AB), nil, err)
		} else if err := msgReply.DecodePayload(&reply); err != nil {
			callback(a.Behavior().(AB), nil, err)
		} else {
			callback(a.Behavior().(AB), &reply, nil)
		}
	}
}

func RPC(a gactor.Actor, ctx context.Context, to gactor.ActorUID, params any, reply any) error {
	var msgParams message.Msg
	var msgReply message.Msg
	msgParams = message.NewMsgWithPayload(params)
	if err := a.RPC(ctx, to, &msgParams, &msgReply); err != nil {
		return err
	} else if err := msgReply.DecodePayload(reply); err != nil {
		return err
	} else {
		return nil
	}
}

func RPCAsync(a gactor.Actor, ctx context.Context, to gactor.ActorUID, params any, callback gactor.ActorRPCCallback) error {
	var msgParams message.Msg
	msgParams = message.NewMsgWithPayload(params)
	return a.AsyncRPC(ctx, to, &msgParams, callback)
}
