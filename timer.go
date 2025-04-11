package gactor

import (
	"time"

	"github.com/godyy/gutils/container/heap"
)

// TimerID 定时器ID.
type TimerID int64

// None ID是否无意义.
func (tid TimerID) None() bool {
	return tid == 0
}

// SetNone 重置ID.
func (tid *TimerID) SetNone() {
	*tid = 0
}

// TimerCallbackArgs 定时器回调参数.
type TimerCallbackArgs struct {
	TID   TimerID
	Actor Actor
	Args  any
}

// TimerCallback 定时器回调函数.
type TimerCallback func(TimerCallbackArgs)

// timer 定时器.
type timer struct {
	id       TimerID       // ID.
	index    int           // heap index.
	repeat   bool          // 是否重复定时器.
	args     any           // 参数.
	callback TimerCallback // 回调.
	expireAt int64         // 过期时间.
	duration time.Duration // 持续时间.
}

func (t *timer) HeapLess(oth heap.Element) bool {
	if t.expireAt == oth.(*timer).expireAt {
		return t.id < oth.(*timer).id
	}
	return t.expireAt < oth.(*timer).expireAt
}

func (t *timer) SetHeapIndex(index int) {
	t.index = index
}

func (t *timer) HeapIndex() int {
	return t.index
}

// timerManager 定时器管理器.
type timerManager struct {
	timerIdIncr int64              // timer id 自增键.
	timerMap    map[TimerID]*timer // timer map.
	timerHeap   *heap.Heap[*timer] // time 最小堆队列.
	sysTimer    *time.Timer        // 系统定时器.
}

func newTimerManager() *timerManager {
	tm := &timerManager{
		timerIdIncr: 0,
		timerMap:    make(map[TimerID]*timer),
		timerHeap:   heap.NewHeap[*timer](),
		sysTimer:    time.NewTimer(0),
	}

	tm.stopSysTimer()

	return &timerManager{}

}

// genTimerId 生成 TimerID.
func (tm *timerManager) genTimerId() TimerID {
	tm.timerIdIncr++
	return TimerID(tm.timerIdIncr)
}

// addTimer 添加定时器.
func (tm *timerManager) addTimer(t *timer) {
	tm.timerMap[t.id] = t
	tm.timerHeap.Push(t)
}

// delTimer 删除定时器.
func (tm *timerManager) delTimer(t *timer) {
	tm.timerHeap.Remove(t.index)
	delete(tm.timerMap, t.id)
}

// topTimer 获取堆顶定时器.
func (tm *timerManager) topTimer() *timer {
	if tm.timerHeap.Len() == 0 {
		return nil
	}
	return tm.timerHeap.Top()
}

// resetSysTimer 重置系统定时器.
func (tm *timerManager) resetSysTimer(expireAt int64) {
	tm.stopSysTimer()
	tm.sysTimer.Reset(time.Duration(expireAt - time.Now().UnixNano()))
}

// stopSysTimer 停止系统定时器.
func (tm *timerManager) stopSysTimer() {
	if !tm.sysTimer.Stop() {
		select {
		case <-tm.sysTimer.C:
			break
		default:
			break
		}
	}
}

// createTimer 创建定时器.
func (tm *timerManager) createTimer(repeat bool, expiretAt int64, d time.Duration, args any, callback TimerCallback) TimerID {
	t := &timer{
		index:    -1,
		id:       tm.genTimerId(),
		args:     args,
		duration: d,
		expireAt: expiretAt,
		repeat:   repeat,
		callback: callback,
	}
	tm.addTimer(t)
	if t == tm.timerHeap.Top() {
		tm.resetSysTimer(t.expireAt)
	}
	return t.id
}

// startTimer 启动定时器.
func (tm *timerManager) startTimer(expireAt int64, args any, callback TimerCallback) TimerID {
	return tm.createTimer(false, expireAt, 0, args, callback)
}

// startTimerRepeat 启动重复定时器.
func (tm *timerManager) startTimerRepeat(d time.Duration, args any, callback TimerCallback) TimerID {
	return tm.createTimer(true, time.Now().UnixNano()+int64(d), d, args, callback)
}

// stopTimer 停止定时器.
func (tm *timerManager) stopTimer(id TimerID) {
	t, ok := tm.timerMap[id]
	if !ok {
		return
	}
	top := t == tm.timerHeap.Top()
	tm.delTimer(t)
	if top {
		if tm.timerHeap.Len() > 0 {
			tm.resetSysTimer(tm.timerHeap.Top().expireAt)
		} else {
			tm.stopSysTimer()
		}
	}
}

// stopAllTimer 停止所有定时器.
func (tm *timerManager) stopAllTimer() {
	tm.timerIdIncr = 0
	tm.timerMap = make(map[TimerID]*timer)
	tm.timerHeap.Init()
	tm.stopSysTimer()
}

// chanExpiredTime 返回用于读取已过期时间的 chan.
func (tm *timerManager) chanExpiredTime() <-chan time.Time {
	return tm.sysTimer.C
}

// handleExpiredTimers 处理已过期的定时器.
func (tm *timerManager) handleExpiredTimers(a *actor, now time.Time) {
	var (
		ts = now.UnixNano()
		t  *timer
	)

	for {
		// 锁定索引状态, 判断堆顶定时器是否过期，如过期，移除定时.
		al, err := a.lock(stateStarted, false)
		if err != nil {
			break
		}

		t = tm.topTimer()
		if t == nil {
			a.unlock(al)
			break
		}

		if ts < t.expireAt {
			a.unlock(al)
			break
		}

		tm.delTimer(t)
		// 解除状态锁定，避免执行回调时阻塞其它状态锁定需求.
		a.unlock(al)

		// 执行定时器回调.
		t.callback(TimerCallbackArgs{
			TID:   t.id,
			Actor: a,
			Args:  t.args,
		})

		// 若过期的为重复定时器，则重新添加到堆中.
		if t.repeat {
			al, err := a.lock(stateStarted, false)
			if err != nil {
				break
			}
			t.expireAt = t.expireAt + int64(t.duration)
			tm.addTimer(t)
			a.unlock(al)
		}

		t = nil
	}

	// 若仍有定时器未过期，重置系统定时器.
	if t != nil {
		if al, err := a.lock(stateStarted, false); err == nil {
			tm.resetSysTimer(t.expireAt)
			a.unlock(al)
		}
	}
}
