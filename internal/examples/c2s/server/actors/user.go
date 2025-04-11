package actors

import (
	"context"
	"fmt"
	"time"

	"github.com/godyy/gactor"
	"github.com/godyy/gactor/internal/examples/c2s/common/message"
)

// User 用户.
type User struct {
	cActor
	name          string
	notifyTimerId gactor.TimerID
	IsLogin       bool
}

func NewUser(a gactor.CActor) gactor.CActorBehavior {
	return &User{
		cActor: newCActor(a),
	}
}

func (u *User) OnDisconnected() {
	if u.IsLogin {
		u.IsLogin = false
	}
}

func (u *User) SetName(name string) {
	u.name = name
}

func (u *User) GetName() string {
	return u.name
}

func (u *User) StartNotifyTimer() {
	u.notifyTimerId = u.StartTimerRepeat(5*time.Second, nil, onNotifyTimer)
}

func (u *User) StopNotifyTimer() {
	if u.notifyTimerId.None() {
		return
	}

	u.StopTimer(u.notifyTimerId)
	u.notifyTimerId.SetNone()
}

func onNotifyTimer(args *gactor.TimerCallbackArgs) error {
	u := args.Actor.Behavior().(*User)
	if args.TID != u.notifyTimerId {
		return nil
	}

	notifyMsg := message.NewPushMessageWithPayload(&message.Notify{
		Msg: fmt.Sprintf("notify at %v", time.Now()),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return u.PushRawMessage(ctx, &notifyMsg)
}
