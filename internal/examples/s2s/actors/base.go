package actors

import (
	"context"

	"github.com/godyy/gactor"
)

type actor struct {
	gactor.Actor
}

func (a *actor) RPC(ctx context.Context, to gactor.ActorUID, params any, reply any) error {
	return RPC(a.Actor, ctx, to, params, reply)
}

func (a *actor) AsyncRPC(ctx context.Context, to gactor.ActorUID, params any, callback gactor.ActorRPCCallback) error {
	return RPCAsync(a.Actor, ctx, to, params, callback)
}

type cActor struct {
	gactor.CActor
}

func (a *cActor) RPC(ctx context.Context, to gactor.ActorUID, params any, reply any) error {
	return RPC(a.CActor, ctx, to, params, reply)
}

func (a *cActor) AsyncRPC(ctx context.Context, to gactor.ActorUID, params any, callback gactor.ActorRPCCallback) error {
	return RPCAsync(a.CActor, ctx, to, params, callback)
}
