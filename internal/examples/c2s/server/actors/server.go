package actors

import (
	"fmt"

	"github.com/godyy/gactor"
)

const ServerId = 1

type Server struct {
	actor
	name string
}

func NewServer(a gactor.Actor) gactor.ActorBehavior {
	return &Server{
		actor: newActor(a),
		name:  fmt.Sprintf("server-%d", a.ActorUID().ID),
	}
}

func (s *Server) Name() string {
	return s.name
}
