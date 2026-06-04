package actors

import (
	"context"

	"github.com/godyy/gactor"
)

type actor struct {
	gactor.Actor
}

func (a *actor) RPCWithContext(ctx context.Context, to gactor.ActorUID, params any, reply any) error {
	return RPCWithContext(a.Actor, ctx, to, params, reply)
}

func (a *actor) AsyncRPCWithContext(ctx context.Context, to gactor.ActorUID, params any, callback gactor.ActorRPCFunc) error {
	return AsyncRPCWithContext(a.Actor, ctx, to, params, callback)
}

type cActor struct {
	gactor.CActor
}

func (a *cActor) RPCWithContext(ctx context.Context, to gactor.ActorUID, params any, reply any) error {
	return RPCWithContext(a.CActor, ctx, to, params, reply)
}

func (a *cActor) AsyncRPCWithContext(ctx context.Context, to gactor.ActorUID, params any, callback gactor.ActorRPCFunc) error {
	return AsyncRPCWithContext(a.CActor, ctx, to, params, callback)
}
