package gactor

type message interface {
	handle(a *actor)
}

type messageStop struct{}

func (m *messageStop) handle(a *actor) {
	a.doStop()
}
