package actors

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/godyy/gactor/internal/examples/s2s/logger"

	"github.com/godyy/gactor/internal/examples/s2s/message"

	"github.com/godyy/gactor"
)

type User struct {
	cActor
	name string
}

func NewUser(a gactor.CActor) *User {
	return &User{
		cActor: cActor{a},
	}
}

func (u *User) GetActor() gactor.Actor {
	return u.CActor
}

func (u *User) GetCActor() gactor.CActor {
	return u.CActor
}

func (u *User) OnStart() error {
	u.name = fmt.Sprintf("User:%d", u.ActorUID().ID)
	randomGetServerName(u)
	return nil
}

func (u *User) OnStop() error {
	return nil
}

func (u *User) OnConnected() {
}

func (u *User) OnDisconnected() {
}

func randomGetServerName(u *User) {
	d := time.Duration(100+rd.Intn(400)) * time.Millisecond
	u.StartTimer(d, false, nil, onRandomGetServerName)
}

func onRandomGetServerName(args *gactor.ActorTimerArgs) {
	u := args.Actor.Behavior().(*User)

	if rand.Intn(2) > 0 {
		if err := u.AsyncRPC(context.Background(), serverUID, &message.GetServerNameReq{}, getServerNameRPCCallback); err != nil {
			logger.Logger().Errorf("User %d async get Server name, %v", u.ActorUID().ID, err)
		}
		randomGetServerName(u)
	} else {
		var reply message.GetServerNameResp
		if err := u.RPC(context.Background(), serverUID, &message.GetServerNameReq{}, &reply); err != nil {
			logger.Logger().Errorf("User %d get Server name, %v", u.ActorUID().ID, err)
		} else {
			logger.Logger().Debugf("User %d get Server name %s", u.ActorUID().ID, reply.ServerName)
		}
		randomGetServerName(u)
	}
}

var getServerNameRPCCallback = WrapRPCCallback(func(u *User, reply *message.GetServerNameResp, err error) {
	if err != nil {
		logger.Logger().Errorf("User %d async get Server name, %v", u.ActorUID().ID, err)
	} else {
		logger.Logger().Debugf("User %d async get Server name %s", u.ActorUID().ID, reply.ServerName)
	}
	//randomGetServerName(u)
})

var rd *rand.Rand

func init() {
	rd = rand.New(rand.NewSource(time.Now().UnixNano()))
}
