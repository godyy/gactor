package gactor

type Center interface {
	CreateMeta(*Meta) error
	GetMeta(ActorID) (*Meta, error)
	SaveMeta(*Meta) error
}
