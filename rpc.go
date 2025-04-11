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

// RPCCall RPC 调用实例.
type RPCCall interface {
	// DecodePayload 解码 RPC 响应到 v.
	DecodePayload(v any) error

	// Err 返回错误信息.
	// 如果调用超时, 返回 ErrRPCTimeout.
	// 如果服务停机, 返回 ErrServiceStopped.
	Err() error

	// release 释放.
	release()
}

// RPCCallback RPC回调.
// RPCCall 对象只能在回调内部访问, 切记不要传递到回调函数外.
type RPCCallback func(RPCCall)

// rpcCall 内部 RPC 调用实例实现.
type rpcCall struct {
	svc         *Service    // Service.
	heapIndex   int         // heap index.
	reqId       uint64      // RPC 调用序列 ID.
	to          ActorUID    // 目标 ActorUID.
	expiredAt   int64       // 过期时间.
	respPayload Packet      // RPC 响应负载数据.
	err         error       // 错误信息.
	callback    RPCCallback // 回调函数.
}

func (call *rpcCall) HeapLess(oth *rpcCall) bool {
	return call.expiredAt < oth.expiredAt
}

func (call *rpcCall) SetHeapIndex(i int) {
	call.heapIndex = i
}

func (call *rpcCall) HeapIndex() int {
	return call.heapIndex
}

func (call *rpcCall) DecodePayload(v any) error {
	if call.err != nil {
		return call.err
	}
	if err := call.svc.decodePayload(PacketTypeS2SRpcResp, call.respPayload, v); err == nil {
		return nil
	} else if errors.Is(err, ErrPacketEscape) {
		call.respPayload = nil
		return nil
	} else {
		return err
	}
}

func (call *rpcCall) Err() error {
	return call.err
}

func (call *rpcCall) release() {
	if call.respPayload != nil {
		call.svc.putPacket(call.respPayload)
		call.respPayload = nil
	}
	call.err = nil
	call.callback = nil
	call.svc = nil
	poolOfRPCCall.Put(call)
}

func (call *rpcCall) onResponse(payload Packet, err error) {
	call.respPayload = payload
	call.err = err
}

func (call *rpcCall) invokeCallback() {
	call.callback(call)
}

var poolOfRPCCall = &sync.Pool{New: func() interface{} {
	return &rpcCall{heapIndex: -1}
}}

// rpcManager RPC Manager.
type rpcManager struct {
	timeSys    TimeSystem    // 时间系统.
	chCallback chan *rpcCall // 已完成等待执行回调的 rpcCall.
	ticker     *time.Ticker  // ticker.

	mtx       sync.Mutex           // mtx for following.
	reqIdIncr uint64               // req id 自增键.
	callMap   map[uint64]*rpcCall  // call map.
	callHeap  *heap.Heap[*rpcCall] // call heap, 按照过期时间排序.
}

func newRPCManager(timeSys TimeSystem, callbackChanSize int) *rpcManager {
	m := &rpcManager{
		timeSys:    timeSys,
		chCallback: make(chan *rpcCall, callbackChanSize),
		reqIdIncr:  0,
		callMap:    make(map[uint64]*rpcCall),
		callHeap:   heap.NewHeap[*rpcCall](),
	}
	return m
}

// start 启动.
func (m *rpcManager) start() {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	m.startCallbackWorkers()
	m.ticker = time.NewTicker(100 * time.Millisecond)
}

// stop 停止.
func (m *rpcManager) stop() {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	close(m.chCallback)

	for _, call := range m.callMap {
		m.invokeCallback(call, nil, ErrServiceStopped)
	}
	m.callMap = nil
	m.callHeap.Init()
}

// startCallbackWorkers 启动回调工作器.
func (m *rpcManager) startCallbackWorkers() {
	workers := runtime.NumCPU()
	for i := 0; i < workers; i++ {
		go func() {
			for call := range m.chCallback {
				call.invokeCallback()
			}
		}()
	}
}

// genReqId 生成 req id.
func (m *rpcManager) genReqId() uint64 {
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

// createCall 创建 Call.
func (m *rpcManager) createCall(s *Service, to ActorUID, expiredAt time.Time, callback RPCCallback) (*rpcCall, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	call := poolOfRPCCall.Get().(*rpcCall)
	call.svc = s
	call.heapIndex = -1
	call.reqId = m.genReqId()
	call.to = to
	call.expiredAt = expiredAt.UnixNano()
	call.callback = callback

	m.addCall(call)

	return call, nil
}

// removeCallByReqId 通过 reqId 移除 Call.
// 通常在出现错误需要直接移除 Call 的情况下调用.
func (m *rpcManager) removeCallByReqId(reqId uint64) bool {
	m.mtx.Lock()

	call := m.callMap[reqId]
	if call == nil {
		m.mtx.Unlock()
		return false
	}

	m.removeCall(call)

	m.mtx.Unlock()

	call.release()

	return true
}

// invokeCallback 调用回调.
func (m *rpcManager) invokeCallback(call *rpcCall, payload Packet, err error) {
	call.onResponse(payload, err)
	m.chCallback <- call
}

// handleResponse 处理 RPC Response. 返回对应的 rpcCall 是否存在.
func (m *rpcManager) handleResponse(reqId uint64, payload Packet, err error) bool {
	m.mtx.Lock()

	call := m.callMap[reqId]
	if call == nil {
		m.mtx.Unlock()
		return false
	}

	m.removeCall(call)

	m.mtx.Unlock()

	if errors.Is(err, errCodeOK) {
		err = nil
	}

	m.invokeCallback(call, payload, err)

	return true
}

// chanTick 返回用于读取 tick time 的 chan.
func (m *rpcManager) chanTick() <-chan time.Time {
	return m.ticker.C
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
		if m.timeSys.Now().UnixNano() < call.expiredAt {
			m.mtx.Unlock()
			break
		}

		m.removeCall(call)

		m.mtx.Unlock()

		m.invokeCallback(call, nil, ErrRPCTimeout)
	}
}
