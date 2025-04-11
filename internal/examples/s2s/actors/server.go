package actors

import (
	"fmt"

	"github.com/godyy/gactor"
)

var serverUID gactor.ActorUID

type Server struct {
	actor
	name string
}

func NewServer(a gactor.Actor) *Server {
	return &Server{
		actor: actor{a},
	}
}

func (s *Server) GetActor() gactor.Actor {
	return s.Actor
}

func (s *Server) OnStart() error {
	serverUID = s.ActorUID()
	s.name = fmt.Sprintf("Server:%d", s.ActorUID().ID)
	return nil
}

func (s *Server) OnStop() error {
	return nil
}

func (s *Server) GetName() string {
	return s.name
}
