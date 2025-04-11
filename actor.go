package gactor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrActorNotStarted = errors.New("gactor: actor not started")
	ErrActorStarted    = errors.New("gactor: actor started")
	ErrActorStopping   = errors.New("gactor: actor stopping")
	ErrActorStopped    = errors.New("gactor: actor stopped")
)

type ActorBehaviorCreator func(Actor) ActorBehavior

// ActorDefine 定义一个 Actor 分类.
type ActorDefine struct {
	// Actor 名称.
	Name string

	// Category 表示 Actor 的类别.
	Category int // 类别.

	// Priority 表示 Actor 的优先级. 值越小优先级越高.
	Priority int

	// BehaviorCreator 构造函数.
	BehaviorCreator ActorBehaviorCreator

	// MessageBoxSize 表示 Actor 的信箱大小.
	MessageBoxSize int

	// RecycleTime 表示 Actor 的回收时间. Actor 空闲超过该时间后会被系统回收.
	RecycleTime time.Duration

	// PersistDelay 表示 Actor 的持久化延迟时间. 当 Actor 的数据发生变化后，会在该时间后进行持久化.
	PersistDelay time.Duration
}

// ActorFactory Actor 工厂.
type ActorFactory struct {
	actorDefineMap map[int]*ActorDefine // Actor 定义.
}

func NewActorFactory(actorDefines []*ActorDefine) *ActorFactory {
	actorDefineMap := make(map[int]*ActorDefine, len(actorDefines))
	for _, actorDefine := range actorDefines {
		if _, ok := actorDefineMap[actorDefine.Category]; ok {
			panic(fmt.Sprintf("actor define of category %d already exists", actorDefine.Category))
		} else {
			actorDefineMap[actorDefine.Category] = actorDefine
		}
	}
	return &ActorFactory{
		actorDefineMap: actorDefineMap,
	}
}

func (af *ActorFactory) createActor(category int, svc *Service, id int64) (*actor, error) {
	actorDefine, ok := af.actorDefineMap[category]
	if !ok {
		return nil, fmt.Errorf("actor define of category %d not found", category)
	}

	a := &actor{
		ActorDefine:     actorDefine,
		id:              id,
		svc:             svc,
		chMessageBox:    make(chan message, actorDefine.MessageBoxSize),
		state:           stateInit,
		needPersistFlag: false,
		timerManager:    newTimerManager(),
	}
	a.ActorBehavior = actorDefine.BehaviorCreator(a)

	return a, nil
}

// ActorBehavior 封装 ActorBehavior 的行为.
type ActorBehavior interface {
	// OnStart 启动.
	OnStart() error

	// OnStop 停止.
	OnStop() error

	// OnPersist 持久化.
	OnPersist() error

	// HandleRequest 处理请求.
	HandleRequest() error
}

// ActorID 表示 Actor 的唯一标识.
type ActorID struct {
	Category int   // The actor category from ActorDefine
	ID       int64 // Unique instance ID within the category
}

type Actor interface {
	// ActorID 获取 Actor 的唯一标识.
	ActorID() ActorID

	// NeedPersist 启动持久化任务.
	NeedPersist() error

	// StartTimer 启动定时器.
	StartTimer(expireAt int64, args any, callback TimerCallback) TimerID

	// StartTimerRepeat 启动重复定时器.
	StartTimerRepeat(d time.Duration, args any, callback TimerCallback) TimerID
}

const (
	stateInit     = 0
	stateStarted  = 1
	stateStopping = 2
	stateStopped  = 3
)

type actor struct {
	*ActorDefine
	ActorBehavior
	id           int64
	svc          *Service
	chMessageBox chan message

	mtx             sync.RWMutex
	state           int32
	needPersistFlag bool
	*timerManager
}

func (a *actor) ActorID() ActorID {
	return ActorID{
		Category: a.Category,
		ID:       a.id,
	}
}

type actorLock struct {
	read bool // 是否读锁.
}

func (a *actor) lock(needState int32, read bool) (actorLock, error) {
	if read {
		a.mtx.RLock()
	} else {
		a.mtx.Lock()
	}

	if a.state == needState {
		return actorLock{read: read}, nil
	}

	if read {
		a.mtx.RUnlock()
	} else {
		a.mtx.Unlock()
	}

	var err error
	switch a.state {
	case stateInit:
		err = ErrActorNotStarted
	case stateStarted:
		err = ErrActorStarted
	case stateStopping:
		err = ErrActorStopping
	case stateStopped:
		err = ErrActorStopped
	default:
		panic(fmt.Sprintf("invalid actor state %d", a.state))
	}

	return actorLock{}, err
}

func (a *actor) unlock(l actorLock) {
	if l.read {
		a.mtx.RUnlock()
	} else {
		a.mtx.Unlock()
	}
}

func (a *actor) start() error {
	al, err := a.lock(stateInit, false)
	if err != nil {
		return err
	}
	defer a.unlock(al)
	if err := a.ActorBehavior.OnStart(); err != nil {
		return err
	}
	go a.loop()
	a.resetRecycleTimer()
	a.state = stateStarted
	return nil
}

func (a *actor) stop() error {
	al, err := a.lock(stateStarted, false)
	if err != nil {
		return err
	}
	defer a.unlock(al)

	ctx, cancel := context.WithTimeoutCause(context.Background(), 5*time.Second, errors.New("timeout"))
	defer cancel()

	if err := a.doReceiveMessage(ctx, &messageStop{}); err != nil {
		return err
	}

	a.timerManager.stopAllTimer()
	a.state = stateStopping
	return nil
}

func (a *actor) doStop() {
	al, err := a.lock(stateStarted, false)
	if err == nil {
		a.timerManager.stopAllTimer()
	} else if al, err = a.lock(stateStarted, false); err != nil {
		return
	}

	drain := false
	for !drain {
		select {
		case msg := <-a.chMessageBox:
			msg.handle(a)
		default:
			drain = true
		}
	}

	if err := a.ActorBehavior.OnStop(); err != nil {
		// todo
	}

	svc := a.svc
	a.ActorBehavior = nil
	a.svc = nil
	a.state = stateStopped
	a.unlock(al)

	svc.onActorStopped(a)
}

func (a *actor) NeedPersist() error {
	a.mtx.Lock()
	defer a.mtx.Unlock()

	if a.needPersistFlag {
		return nil
	}

	if a.state < stateStarted {
		return ErrActorNotStarted
	} else if a.state == stateStarted {
		a.timerManager.startTimer(time.Now().Add(a.PersistDelay).UnixNano(), nil, a.onPersist)
	} else if a.state >= stateStopped {
		return ErrActorStopped
	}

	a.needPersistFlag = true
	return nil
}

func (a *actor) onPersist(_ TimerCallbackArgs) {
	a.needPersistFlag = false
	if err := a.ActorBehavior.OnPersist(); err != nil {
		_ = a.NeedPersist()
	}
}

func (a *actor) receiveMessage(ctx context.Context, msg message) error {
	al, err := a.lock(stateStarted, true)
	if err != nil {
		return err
	}
	defer a.unlock(al)

	return a.doReceiveMessage(ctx, msg)
}

func (a *actor) doReceiveMessage(ctx context.Context, msg message) error {
	select {
	case a.chMessageBox <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *actor) resetRecycleTimer() {
	if a.RecycleTime == 0 {
		return
	}

	a.timerManager.startTimer(time.Now().Add(a.RecycleTime).UnixNano(), nil, a.onRecycle)
}

func (a *actor) onRecycle(_ TimerCallbackArgs) {
	a.doStop()
}

// stillAlive 标活，重置回收定时.
func (a *actor) stillAlive() {
	al, err := a.lock(stateStarted, false)
	if err != nil {
		return
	}
	defer a.unlock(al)
	a.resetRecycleTimer()
}

func (a *actor) StartTimer(expireAt int64, args any, callback TimerCallback) TimerID {
	al, err := a.lock(stateStarted, false)
	if err != nil {
		return 0
	}
	defer a.unlock(al)
	return a.timerManager.startTimer(expireAt, args, callback)
}

func (a *actor) StartTimerRepeat(d time.Duration, args any, callback TimerCallback) TimerID {
	al, err := a.lock(stateStarted, false)
	if err != nil {
		return 0
	}
	defer a.unlock(al)
	return a.timerManager.startTimerRepeat(d, args, callback)
}

func (a *actor) StopTimer(tid TimerID) {
	al, err := a.lock(stateStarted, false)
	if err != nil {
		return
	}
	defer a.unlock(al)
	a.timerManager.stopTimer(tid)
}

func (a *actor) loop() {
	for {
		select {
		case msg := <-a.chMessageBox:
			msg.handle(a)
		case nextTime := <-a.timerManager.chanExpiredTime():
			a.timerManager.handleExpiredTimers(a, nextTime)
		}
	}
}
