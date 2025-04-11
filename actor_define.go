package gactor

import (
	"errors"
	"fmt"
	"sort"
	"time"

	pkgerrors "github.com/pkg/errors"
)

// ActorDefineImpl Actor 定义实现接口封装.
type ActorDefineImpl interface {
	// init 初始化配置, 验证数据是否有效.
	init() error

	// common 通用部分.
	common() *ActorDefineCommon

	// newActorCore 创建 Actor.
	createActor(svc *Service, id int64) actorImpl
}

// actorDefineSet Actor 定义集合.
type actorDefineSet struct {
	defineMap      map[uint16]ActorDefineImpl // Actor 定义.
	priorityList   []int                      // 优先级列表.
	priority2Index map[int]int                // 优先级索引.
}

func newActorDefineSet(actorDefines []ActorDefineImpl) *actorDefineSet {
	if len(actorDefines) == 0 {
		panic("gactor: no actor define")
	}
	defineMap := make(map[uint16]ActorDefineImpl, len(actorDefines))
	priorityList := make([]int, 0)
	priorityMap := make(map[int]bool)
	for i, actorDefine := range actorDefines {
		if err := actorDefine.init(); err != nil {
			panic(pkgerrors.WithMessagef(err, "gactor: actor define [%d]", i))
		}
		if _, ok := defineMap[actorDefine.common().Category]; ok {
			panic(fmt.Sprintf("gactor: actor define [%d] of category %d already exists", i, actorDefine.common().Category))
		}
		defineMap[actorDefine.common().Category] = actorDefine
		if !priorityMap[actorDefine.common().Priority] {
			priorityList = append(priorityList, actorDefine.common().Priority)
			priorityMap[actorDefine.common().Priority] = true
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

func (s *actorDefineSet) getDefine(category uint16) ActorDefineImpl {
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

// ActorDefineCommon Actor 定义基础数据.
type ActorDefineCommon struct {
	// Actor 名称.
	Name string

	// Category 表示 Actor 的类别.
	Category uint16

	// Priority 表示 Actor 的优先级. 值越小优先级越高.
	Priority int

	// MessageBoxSize 表示 Actor 的信箱大小.
	MessageBoxSize int

	// AsyncRPCCallQueueSize 适配异步 RPC 调用, 设置用于接收已完成 RPC 调用对象
	// 的队列大小. 值大于 0 时, 才可通过 Actor 发起异步 RPC 调用.
	AsyncRPCCallQueueSize int

	// RecycleTime 表示 Actor 的回收时间. Actor 空闲超过该时间后会被系统回收.
	RecycleTime time.Duration

	// Handler Actor 请求处理器.
	Handler HandlerFunc
}

func (ad *ActorDefineCommon) init() error {
	if ad.Name == "" {
		return errors.New("name empty")
	}

	if ad.MessageBoxSize <= 0 {
		return errors.New("messageBoxSize <= 0")
	}

	if ad.Handler == nil {
		return errors.New("requestHandler nil")
	}

	return nil
}

// ActorDefine Actor 定义.
type ActorDefine struct {
	*ActorDefineCommon

	// BehaviorCreator 行为构造器.
	BehaviorCreator ActorBehaviorCreator
}

func (ad *ActorDefine) init() error {
	if ad.ActorDefineCommon == nil {
		return errors.New("core nil")
	}

	if err := ad.ActorDefineCommon.init(); err != nil {
		return err
	}

	if ad.BehaviorCreator == nil {
		return errors.New("behaviorCreator nil")
	}

	return nil
}

func (ad *ActorDefine) common() *ActorDefineCommon {
	return ad.ActorDefineCommon
}

func (ad *ActorDefine) createActor(svc *Service, id int64) actorImpl {
	a := &actor{
		actorCore: newActorCore(ad.ActorDefineCommon, id, svc),
	}
	a.behavior = ad.BehaviorCreator(a)
	return a
}

// CActorDefine 面向客户端的 Actor 定义.
type CActorDefine struct {
	*ActorDefineCommon

	// BehaviorCreator 行为构造器.
	BehaviorCreator CActorBehaviorCreator
}

func (ad *CActorDefine) init() error {
	if ad.ActorDefineCommon == nil {
		return errors.New("core nil")
	}

	if err := ad.ActorDefineCommon.init(); err != nil {
		return err
	}

	if ad.BehaviorCreator == nil {
		return errors.New("behaviorCreator nil")
	}

	return nil
}

func (ad *CActorDefine) common() *ActorDefineCommon {
	return ad.ActorDefineCommon
}

func (ad *CActorDefine) createActor(svc *Service, id int64) actorImpl {
	a := &cActor{
		actorCore: newActorCore(ad.ActorDefineCommon, id, svc),
	}
	a.behavior = ad.BehaviorCreator(a)
	return a
}
