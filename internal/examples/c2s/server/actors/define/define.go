package define

import (
	"time"

	"github.com/godyy/gactor"
	"github.com/godyy/gactor/internal/examples/c2s/common/consts"
	"github.com/godyy/gactor/internal/examples/c2s/server/actors"
	"github.com/godyy/gactor/internal/examples/c2s/server/handlers/server"
	"github.com/godyy/gactor/internal/examples/c2s/server/handlers/user"
)

var Defines = []gactor.ActorDefineImpl{
	&gactor.CActorDefine{
		ActorDefineCommon: &gactor.ActorDefineCommon{
			Name:                  consts.CategoryNameUser,
			Category:              consts.CategoryUser,
			Priority:              consts.CategoryPriorityUser,
			MessageBoxSize:        10,
			AsyncRPCCallQueueSize: 1,
			RecycleTime:           10 * time.Second,
			Handler:               user.Handler(),
		},
		BehaviorCreator: func(c gactor.CActor) gactor.CActorBehavior {
			return actors.NewUser(c)
		},
	},
	&gactor.ActorDefine{
		ActorDefineCommon: &gactor.ActorDefineCommon{
			Name:                  consts.CategoryNameServer,
			Category:              consts.CategoryServer,
			Priority:              consts.CategoryPriorityServer,
			MessageBoxSize:        100,
			AsyncRPCCallQueueSize: 10,
			RecycleTime:           0,
			Handler:               server.Handler(),
		},
		BehaviorCreator: func(a gactor.Actor) gactor.ActorBehavior {
			return actors.NewServer(a)
		},
	},
}
