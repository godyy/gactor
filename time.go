package gactor

import (
	"errors"
	"sync"
	"time"

	"github.com/godyy/gutils/container/heap"
)

// TimeSystem 时间系统.
type TimeSystem interface {
	// Now 返回当前时间.
	Now() time.Time

	// Until 返回 t 距离当前时间的时间差.
	Until(t time.Time) time.Duration
}

// defTimeSystem 默认时间系统.
type defTimeSystem struct{}

func (d *defTimeSystem) Now() time.Time {
	return time.Now()
}

func (d *defTimeSystem) Until(t time.Time) time.Duration {
	return time.Until(t)
}

// DefTimeSystem 返回默认时间系统.
func DefTimeSystem() TimeSystem {
	return &defTimeSystem{}
}

// ErrStopTimer 作为 TimerCallback 的返回值, 用于终止重复定时器.
var ErrStopTimer = errors.New("gactor: stop timer")

// TimerID 定时器ID.
type TimerID int64

// None ID是否无意义.
func (tid *TimerID) None() bool {
	return *tid == 0
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
// 如果是重复定时器(repeat=true), 返回 ErrStopTimer 终止定时器.
type TimerCallback func(*TimerCallbackArgs) error

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

var poolOfTimer = &sync.Pool{New: func() interface{} {
	return &timer{}
}}

func (t *timer) HeapLess(oth *timer) bool {
	if t.expireAt == oth.expireAt {
		return t.id < oth.id
	}
	return t.expireAt < oth.expireAt
}

func (t *timer) SetHeapIndex(index int) {
	t.index = index
}

func (t *timer) HeapIndex() int {
	return t.index
}

func (t *timer) release() {
	t.id = 0
	t.index = -1
	t.args = nil
	t.callback = nil
	t.duration = 0
	t.repeat = false
	poolOfTimer.Put(t)
}

// timerManager 定时器管理器.
type timerManager struct {
	timeSys     TimeSystem         // 时间系统.
	timerIdIncr int64              // timer id 自增键.
	timerMap    map[TimerID]*timer // timer map.
	timerHeap   *heap.Heap[*timer] // time 最小堆队列.
	ticker      *time.Ticker       // ticker.
}

func newTimerManager(timeSys TimeSystem) *timerManager {
	tm := &timerManager{
		timeSys:     timeSys,
		timerIdIncr: 0,
		timerMap:    make(map[TimerID]*timer),
		timerHeap:   heap.NewHeap[*timer](),
	}

	return tm
}

// start 启动.
func (tm *timerManager) start() {
	tm.ticker = time.NewTicker(100 * time.Millisecond)
}

// stop 停止.
func (tm *timerManager) stop() {
	tm.ticker.Stop()
	select {
	case <-tm.ticker.C:
	default:
	}
	tm.ticker = nil

	tm.stopAllTimer()
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

// createTimer 创建定时器.
func (tm *timerManager) createTimer(repeat bool, d time.Duration, args any, callback TimerCallback) TimerID {
	t := poolOfTimer.Get().(*timer)
	t.index = -1
	t.id = tm.genTimerId()
	t.args = args
	t.duration = d
	t.expireAt = tm.timeSys.Now().Add(d).UnixNano()
	t.repeat = repeat
	t.callback = callback
	tm.addTimer(t)
	return t.id
}

// startTimer 启动定时器.
func (tm *timerManager) startTimer(d time.Duration, args any, callback TimerCallback) TimerID {
	return tm.createTimer(false, d, args, callback)
}

// startTimerRepeat 启动重复定时器.
func (tm *timerManager) startTimerRepeat(d time.Duration, args any, callback TimerCallback) TimerID {
	return tm.createTimer(true, d, args, callback)
}

// stopTimer 停止定时器.
func (tm *timerManager) stopTimer(id TimerID) {
	t, ok := tm.timerMap[id]
	if !ok {
		return
	}
	tm.delTimer(t)
	t.release()
}

// stopAllTimer 停止所有定时器.
func (tm *timerManager) stopAllTimer() {
	tm.timerIdIncr = 0
	tm.timerMap = make(map[TimerID]*timer)
	tm.timerHeap.Init()
}

// chanTick 返回用于读取 tick time 的 chan.
func (tm *timerManager) chanTick() <-chan time.Time {
	return tm.ticker.C
}

// tick tick 逻辑.
func (tm *timerManager) tick(a actorImpl) {
	for {
		t := tm.topTimer()
		if t == nil {
			break
		}

		ts := tm.timeSys.Now().UnixNano()
		if ts < t.expireAt {
			break
		}

		tm.delTimer(t)

		// 执行定时器回调.
		args := TimerCallbackArgs{
			TID:   t.id,
			Actor: a,
			Args:  t.args,
		}
		err := t.callback(&args)

		// 若过期的为重复定时器，则重新添加到堆中.
		if t.repeat && err == nil {
			// 计算下一次的过期时间.
			p := (ts - t.expireAt) / int64(t.duration)
			t.expireAt += (p + 1) * int64(t.duration)
			tm.addTimer(t)
		} else {
			t.release()
		}

		t = nil
	}
}
