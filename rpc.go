package gactor

import (
	"errors"
	"runtime"
	"sync"
	"time"

	"github.com/godyy/gutils/container/heap"
)

// ErrRPCTimeout RPC 超时错误.
var ErrRPCTimeout = errors.New("gactor: rpc timeout")

// RPCResp RPC 响应参数.
type RPCResp struct {
	svc     *Service // Service.
	payload Buffer   // 响应负载数据.
	err     error    // 错误信息.
}

// DecodeReply 解码响应负载数据到 v.
func (resp *RPCResp) DecodeReply(v any) error {
	if resp.err != nil {
		return resp.err
	}
	if err := resp.svc.decodePayload(PacketTypeS2SRpcResp, &resp.payload, v); err == nil {
		return nil
	} else if errors.Is(err, ErrBytesEscape) {
		resp.payload.SetBuf(nil)
		return nil
	} else {
		return err
	}
}

// Err 返回错误信息.
func (resp *RPCResp) Err() error {
	return resp.err
}

func (resp *RPCResp) release() {
	resp.svc.freeBuffer(&resp.payload)
}

// RPCFunc RPC回调.
// RPCCall 对象只能在回调内部访问, 切记不要传递到回调函数外.
type RPCFunc func(*RPCResp)

// rpcCall 内部 RPC 调用实例实现.
type rpcCall struct {
	heapIndex   int      // heap index.
	reqId       uint32   // RPC 调用序列 ID.
	to          ActorUID // 目标 ActorUID.
	expiredAt   int64    // 过期时间.
	respPayload Buffer   // RPC 响应负载数据.
	err         error    // 错误信息.
	cb          RPCFunc  // 回调函数.
}

var poolOfRPCCall = &sync.Pool{New: func() interface{} {
	return &rpcCall{heapIndex: -1}
}}

func (call *rpcCall) HeapLess(oth *rpcCall) bool {
	return call.expiredAt < oth.expiredAt
}

func (call *rpcCall) SetHeapIndex(i int) {
	call.heapIndex = i
}

func (call *rpcCall) HeapIndex() int {
	return call.heapIndex
}

func (call *rpcCall) onResponse(payload *Buffer, err error) {
	if payload != nil {
		call.respPayload = *payload
	}
	call.err = err
}

func (call *rpcCall) release() {
	call.respPayload.SetBuf(nil)
	call.err = nil
	call.cb = nil
	poolOfRPCCall.Put(call)
}

// rpcManager RPC Manager.
type rpcManager struct {
	svc            *Service      // Service.
	completedCalls chan *rpcCall // 已完成等待执行回调的 rpcCall.

	mtx       sync.Mutex           // mtx for following.
	reqIdIncr uint32               // req id 自增键.
	callMap   map[uint32]*rpcCall  // call map.
	callHeap  *heap.Heap[*rpcCall] // call heap, 按照过期时间排序.
	timerId   TimerId              // 定时器ID.
	cTick     chan struct{}        // chan for tick.
	cStopped  chan struct{}        // 已停止信号.
}

func newRPCManager(svc *Service, maxCompletedCalls int) *rpcManager {
	m := &rpcManager{
		svc:            svc,
		completedCalls: make(chan *rpcCall, maxCompletedCalls),
		reqIdIncr:      0,
		callMap:        make(map[uint32]*rpcCall),
		callHeap:       heap.NewHeap[*rpcCall](),
		timerId:        TimerIdNone,
		cTick:          make(chan struct{}, 1),
		cStopped:       make(chan struct{}),
	}
	return m
}

// genReqId 生成 req id.
func (m *rpcManager) genReqId() uint32 {
	m.reqIdIncr++
	return m.reqIdIncr
}

// addCall 添加 Call.
func (m *rpcManager) addCall(call *rpcCall) {
	m.callMap[call.reqId] = call
	m.callHeap.Push(call)
}

// removeCall 移除 Call.
func (m *rpcManager) removeCall(call *rpcCall) {
	m.callHeap.Remove(call.heapIndex)
	delete(m.callMap, call.reqId)
}

// start 启动.
func (m *rpcManager) start() {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	m.startDoneCallWorkers()
}

// stop 停止.
func (m *rpcManager) stop() {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	m.stopTimer()

	for _, call := range m.callMap {
		m.handleCallDone(call, nil, ErrServiceStopped)
	}
	m.callMap = nil
	m.callHeap.Init()

	close(m.cStopped)
}

// startDoneCallWorkers 启动已完成 RPC 调用的回调执行工作器.
func (m *rpcManager) startDoneCallWorkers() {
	workers := runtime.NumCPU()
	for i := 0; i < workers; i++ {
		go func() {
			for {
				select {
				case call := <-m.completedCalls:
					m.invokeCallback(call)
				case <-m.cStopped:
					drain := false
					for !drain {
						select {
						case call := <-m.completedCalls:
							m.invokeCallback(call)
						default:
							drain = true
						}
					}
					return
				}
			}
		}()
	}
}

// invokeCallback 调用 RPC 回调.
func (m *rpcManager) invokeCallback(call *rpcCall) {
	defer func() {
		if x := recover(); x != nil {
			m.svc.getLogger().ErrorFields("invoke rpc callback panic", lfdPanic(x))
		}
	}()

	resp := RPCResp{
		svc:     m.svc,
		payload: call.respPayload,
		err:     call.err,
	}
	call.cb(&resp)
	resp.release()
	call.release()
}

// resetTimer 重置定时器.
func (m *rpcManager) resetTimer(expiredAt int64) {
	if m.timerId != TimerIdNone {
		m.svc.StopTimer(m.timerId)
		m.timerId = TimerIdNone
	}

	d := time.Duration(expiredAt - time.Now().UnixNano())
	if d <= 0 {
		select {
		case m.cTick <- struct{}{}:
		case <-m.cStopped:
		}
		return
	}

	m.timerId = m.svc.StartTimer(d, false, nil, m.onTimer)
}

// stopTimer 停止定时器
func (m *rpcManager) stopTimer() {
	if m.timerId == TimerIdNone {
		return
	}

	m.svc.StopTimer(m.timerId)
	m.timerId = TimerIdNone
}

// onTimer 定时器回调.
func (m *rpcManager) onTimer(args TimerArgs) {
	m.mtx.Lock()
	if args.TID != m.timerId {
		m.mtx.Unlock()
	}
	m.timerId = TimerIdNone
	m.mtx.Unlock()

	select {
	case m.cTick <- struct{}{}:
	case <-m.cStopped:
	}
}

// createCall 创建 Call.
func (m *rpcManager) createCall(to ActorUID, expiredAt time.Time, cb RPCFunc) *rpcCall {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	call := poolOfRPCCall.Get().(*rpcCall)
	call.heapIndex = -1
	call.reqId = m.genReqId()
	call.to = to
	call.expiredAt = expiredAt.UnixNano()
	call.cb = cb

	m.addCall(call)
	if call == m.callHeap.Top() {
		m.resetTimer(call.expiredAt)
	}

	return call
}

// removeCallByReqId 通过 reqId 移除 Call.
// 通常在出现错误需要直接移除 Call 的情况下调用.
func (m *rpcManager) removeCallByReqId(reqId uint32) bool {
	call := m.popCall(reqId)
	if call == nil {
		return false
	}

	call.release()
	return true
}

// popCall 移除 reqId 指定的 Call. 并重置定时器.
func (m *rpcManager) popCall(reqId uint32) *rpcCall {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	call := m.callMap[reqId]
	if call == nil {
		return nil
	}

	top := call == m.callHeap.Top()
	m.removeCall(call)
	if top {
		if m.callHeap.Len() > 0 {
			m.resetTimer(m.callHeap.Top().expiredAt)
		} else {
			m.stopTimer()
		}
	}

	return call
}

// handleCallDone 处理调用完成.
func (m *rpcManager) handleCallDone(call *rpcCall, payload *Buffer, err error) {
	// 更新监控数据.
	if err == nil {
		m.svc.monitorRPCAction(MonitorCARPC)
	} else if errors.Is(err, ErrRPCTimeout) {
		m.svc.monitorRPCAction(MonitorCARPCTimeout)
	} else {
		m.svc.monitorRPCAction(MonitorCAResponseErr)
	}

	call.onResponse(payload, err)
	m.completedCalls <- call
}

// handleResponse 处理 RPC Response. 返回对应的 rpcCall 是否存在.
func (m *rpcManager) handleResponse(reqId uint32, payload *Buffer, err error) bool {
	call := m.popCall(reqId)
	if call == nil {
		return false
	}

	if errors.Is(err, errCodeOK) {
		err = nil
	}

	m.handleCallDone(call, payload, err)

	return true
}

// chanTick 返回用于读取 tick time 的 chan.
func (m *rpcManager) chanTick() <-chan struct{} {
	return m.cTick
}

// tick 处理已过期的 Call.
func (m *rpcManager) tick() {
	for {
		m.mtx.Lock()

		if m.callHeap.Len() == 0 {
			m.mtx.Unlock()
			break
		}

		call := m.callHeap.Top()
		if m.svc.getTimeSystem().Now().UnixNano() < call.expiredAt {
			m.resetTimer(call.expiredAt)
			m.mtx.Unlock()
			break
		}

		m.removeCall(call)

		m.mtx.Unlock()

		m.handleCallDone(call, nil, ErrRPCTimeout)
	}
}
