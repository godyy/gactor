package actors

import "github.com/godyy/gactor"

type actor struct {
	gactor.Actor
}

func newActor(a gactor.Actor) actor {
	return actor{
		Actor: a,
	}
}

// GetActor 获取 Actor.
func (a *actor) GetActor() gactor.Actor {
	return a.Actor
}

// OnStart 启动行为.
func (a *actor) OnStart() error {
	return nil
}

// OnStop 停机行为.
func (a *actor) OnStop() error {
	return nil
}

type cActor struct {
	gactor.CActor
}

func newCActor(c gactor.CActor) cActor {
	return cActor{
		CActor: c,
	}
}

// GetActor 获取 Actor.
func (a *cActor) GetActor() gactor.Actor {
	return a.CActor
}

// GetCActor 获取 CActor.
func (a *cActor) GetCActor() gactor.CActor {
	return a.CActor
}

// OnStart 启动行为.
func (a *cActor) OnStart() error {
	return nil
}

// OnStop 停机行为.
func (a *cActor) OnStop() error {
	return nil
}

// OnConnected 已连接行为.
func (a *cActor) OnConnected() {
}

// OnDisconnected 已断开连接行为.
func (a *cActor) OnDisconnected() {
}
