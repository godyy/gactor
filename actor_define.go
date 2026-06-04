package gactor

import (
	"errors"
	"fmt"
	"sort"
	"time"

	pkgerrors "github.com/pkg/errors"
)

// ActorDefine Actor 定义接口.
type ActorDefine interface {
	// Name Actor 名称.
	Name() string

	// Category Actor 类别.
	Category() uint16

	// init 初始化配置, 验证数据是否有效.
	init() error

	// base 返回基础数据.
	base() *actorDefineBase

	// createActor 创建 Actor.
	createActor(svc *Service, id int64, leaseId string) actorImpl
}

// actorDefineSet Actor 定义集合.
type actorDefineSet struct {
	defineMap      map[uint16]ActorDefine // Actor 定义.
	priorityList   []int                  // 优先级列表.
	priority2Index map[int]int            // 优先级索引.
}

func newActorDefineSet(actorDefines []ActorDefine) *actorDefineSet {
	if len(actorDefines) == 0 {
		panic("gactor: no actor define")
	}
	defineMap := make(map[uint16]ActorDefine, len(actorDefines))
	priorityList := make([]int, 0)
	priorityMap := make(map[int]bool)
	for i, actorDefine := range actorDefines {
		if err := actorDefine.init(); err != nil {
			panic(pkgerrors.WithMessagef(err, "gactor: actor define [%d]", i))
		}
		if _, ok := defineMap[actorDefine.base().category]; ok {
			panic(fmt.Sprintf("gactor: actor define [%d] of category %d already exists", i, actorDefine.base().category))
		}
		defineMap[actorDefine.base().category] = actorDefine
		if !priorityMap[actorDefine.base().priority] {
			priorityList = append(priorityList, actorDefine.base().priority)
			priorityMap[actorDefine.base().priority] = true
		}
	}
	sort.Ints(priorityList)
	priority2Index := make(map[int]int, len(priorityList))
	for i, priority := range priorityList {
		priority2Index[priority] = i
	}
	return &actorDefineSet{
		defineMap:      defineMap,
		priorityList:   priorityList,
		priority2Index: priority2Index,
	}
}

func (s *actorDefineSet) getDefine(category uint16) ActorDefine {
	return s.defineMap[category]
}

func (s *actorDefineSet) getPriorityIndex(priority int) int {
	index, ok := s.priority2Index[priority]
	if !ok {
		panic(fmt.Sprintf("gactor: actor priority %d not exists", priority))
	}
	return index
}

func (s *actorDefineSet) getPriority(index int) int {
	return s.priorityList[index]
}

// actorDefineBase Actor 定义基础数据.
type actorDefineBase struct {
	// Actor 名称.
	name string

	// category 表示 Actor 的类别.
	category uint16

	// priority 表示 Actor 的优先级. 值越小优先级越高.
	priority int

	// messageBoxSize 表示 Actor 的信箱大小.
	messageBoxSize int

	// maxTimerAmount 最大定时器数量.
	// 目前用于控制接收已触发的定时器的队列大小.
	// 默认值 10.
	maxTimerAmount int

	// maxAsyncRPCAmount 最大异步RPC调用数量.
	// 目前用于控制接收已完成异步RPC调用的队列大小.
	// 默认值 10.
	maxAsyncRPCAmount int

	// recycleTime 表示 Actor 的回收时间.
	// 若大于0, Actor 空闲超过该时间后会被系统回收.
	// 否则, 不会被系统回收(适用于静态 Actor).
	recycleTime time.Duration
}

// Name Actor 名称.
func (ad *actorDefineBase) Name() string {
	return ad.name
}

// Category Actor 类别.
func (ad *actorDefineBase) Category() uint16 {
	return ad.category
}

func (ad *actorDefineBase) init() error {
	if ad.name == "" {
		return errors.New("name empty")
	}

	if ad.messageBoxSize <= 0 {
		return errors.New("messageBoxSize must be greater than 0")
	}

	if ad.maxTimerAmount <= 0 {
		ad.maxTimerAmount = 10
	}

	if ad.maxAsyncRPCAmount <= 0 {
		ad.maxAsyncRPCAmount = 10
	}

	return nil
}

// needRecycle 是否需要回收.
func (ad *actorDefineBase) needRecycle() bool {
	return ad.recycleTime > 0
}

// ActorDefineConfig Actor 定义配置.
type ActorDefineConfig struct {
	Name            string                    // Actor 名称.
	Category        uint16                    // Actor 类别.
	Priority        int                       // Actor 优先级.
	MessageBoxSize  int                       // Actor 信箱大小.
	BehaviorCreator func(Actor) ActorBehavior // Actor 行为构造器.
}

// NewActorDefine 构造ActorDefine
func NewActorDefine(config ActorDefineConfig, ops ...func(ActorDefine)) ActorDefine {
	def := &actorDefine{
		actorDefineBase: &actorDefineBase{
			name:           config.Name,
			category:       config.Category,
			priority:       config.Priority,
			messageBoxSize: config.MessageBoxSize,
		},
		behaviorCreator: config.BehaviorCreator,
	}
	for _, op := range ops {
		op(def)
	}
	return def
}

// CActorDefineConfig CActor 定义配置.
type CActorDefineConfig struct {
	Name            string                      // Actor 名称.
	Category        uint16                      // Actor 类别.
	Priority        int                         // Actor 优先级.
	MessageBoxSize  int                         // Actor 信箱大小.
	RecycleTime     time.Duration               // Actor 回收时间.
	BehaviorCreator func(CActor) CActorBehavior // Actor 行为构造器.
}

// NewCActorDefine 构造CActorDefine
func NewCActorDefine(config CActorDefineConfig, ops ...func(ActorDefine)) ActorDefine {
	def := &cactorDefine{
		actorDefineBase: &actorDefineBase{
			name:           config.Name,
			category:       config.Category,
			priority:       config.Priority,
			messageBoxSize: config.MessageBoxSize,
			recycleTime:    config.RecycleTime,
		},
		behaviorCreator: config.BehaviorCreator,
	}
	for _, op := range ops {
		op(def)
	}
	return def
}

// WithMaxTimerAmount 设置最大定时器数量.
func WithMaxTimerAmount(maxTriggeredTimerAmount int) func(ActorDefine) {
	return func(ad ActorDefine) {
		ad.base().maxTimerAmount = maxTriggeredTimerAmount
	}
}

// WithMaxAsyncRPCAmount 设置最大未完成的异步RPC调用数量.
func WithMaxAsyncRPCAmount(maxAsyncRPCAmount int) func(ActorDefine) {
	return func(ad ActorDefine) {
		ad.base().maxAsyncRPCAmount = maxAsyncRPCAmount
	}
}

// WithRecycleTime 设置回收时间.
func WithRecycleTime(recycleTime time.Duration) func(ActorDefine) {
	return func(ad ActorDefine) {
		ad.base().recycleTime = recycleTime
	}
}

// actorDefine Actor 定义.
type actorDefine struct {
	*actorDefineBase
	behaviorCreator func(Actor) ActorBehavior
}

func (ad *actorDefine) init() error {
	if err := ad.actorDefineBase.init(); err != nil {
		return err
	}

	if ad.behaviorCreator == nil {
		return errors.New("behaviorCreator nil")
	}

	return nil
}

func (ad *actorDefine) base() *actorDefineBase {
	return ad.actorDefineBase
}

func (ad *actorDefine) createActor(svc *Service, id int64, leaseId string) actorImpl {
	a := &actor{
		actorCore: newActorCore(ad.actorDefineBase, id, leaseId, svc),
	}
	a.behavior = ad.behaviorCreator(a)
	return a
}

// cactorDefine CActor 定义.
type cactorDefine struct {
	*actorDefineBase
	behaviorCreator func(CActor) CActorBehavior
}

func (ad *cactorDefine) init() error {
	if err := ad.actorDefineBase.init(); err != nil {
		return err
	}

	if ad.actorDefineBase.recycleTime <= 0 {
		return errors.New("recycleTime must > 0")
	}

	if ad.behaviorCreator == nil {
		return errors.New("behaviorCreator nil")
	}

	return nil
}

func (ad *cactorDefine) base() *actorDefineBase {
	return ad.actorDefineBase
}

func (ad *cactorDefine) createActor(svc *Service, id int64, leaseId string) actorImpl {
	a := &cactor{
		actorCore: newActorCore(ad.actorDefineBase, id, leaseId, svc),
	}
	a.behavior = ad.behaviorCreator(a)
	return a
}
