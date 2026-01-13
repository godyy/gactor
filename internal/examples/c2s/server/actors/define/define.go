package define

import (
	"time"

	"github.com/godyy/gactor"
	"github.com/godyy/gactor/internal/examples/c2s/common/consts"
	"github.com/godyy/gactor/internal/examples/c2s/server/actors"
)

var Defines = []gactor.ActorDefine{
	gactor.NewCActorDefine(gactor.CActorDefineConfig{
		Name:           consts.CategoryNameUser,
		Category:       consts.CategoryUser,
		Priority:       consts.CategoryPriorityUser,
		MessageBoxSize: 10,
		RecycleTime:    10 * time.Second,
		BehaviorCreator: func(c gactor.CActor) gactor.CActorBehavior {
			return actors.NewUser(c)
		},
	},
		gactor.WithMaxCompletedAsyncRPCAmount(1),
	),

	gactor.NewActorDefine(gactor.ActorDefineConfig{
		Name:           consts.CategoryNameServer,
		Category:       consts.CategoryServer,
		Priority:       consts.CategoryPriorityServer,
		MessageBoxSize: 100,
		BehaviorCreator: func(a gactor.Actor) gactor.ActorBehavior {
			return actors.NewServer(a)
		},
	},
		gactor.WithMaxCompletedAsyncRPCAmount(10),
	),
}
