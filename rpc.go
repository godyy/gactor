package gactor

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/godyy/gutils/container/heap"
)

// ErrRPCTimeout RPC 超时错误.
var ErrRPCTimeout = errors.New("gactor: rpc timeout")

// ErrRPCCallDuplicate RPC 调用重复错误.
var ErrRPCCallDuplicate = errors.New("gactor: rpc call duplicate")

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
	chCmds         chan rpcCmd   // 指令通道.
	completedCalls chan *rpcCall // 已完成等待执行回调的 rpcCall.

	reqIdIncr uint32               // req id 自增键.
	callMap   map[uint32]*rpcCall  // call map.
	callHeap  *heap.Heap[*rpcCall] // call heap, 按照过期时间排序.
	timerId   TimerId              // 定时器ID.
}

func newRPCManager(svc *Service) *rpcManager {
	maxRPCCallAmount := svc.getCfg().MaxRPCCallAmount
	m := &rpcManager{
		svc:            svc,
		chCmds:         make(chan rpcCmd, maxRPCCallAmount),
		completedCalls: make(chan *rpcCall, maxRPCCallAmount),
		reqIdIncr:      0,
		callMap:        make(map[uint32]*rpcCall),
		callHeap:       heap.NewHeap[*rpcCall](),
		timerId:        TimerIdNone,
	}
	return m
}

// start 启动.
func (m *rpcManager) start() {
	stopWait := m.svc.getStopWait()
	stopWait.W.Add(1)
	go m.run()
	m.startDoneCallWorkers()
}

// stop 停止.
func (m *rpcManager) stop() {
	m.stopTimer()
	for _, call := range m.callMap {
		m.handleCallDone(call, nil, ErrServiceStopped)
	}
	m.callMap = nil
	m.callHeap.Init()
}

// startDoneCallWorkers 启动已完成 RPC 调用的回调执行工作器.
func (m *rpcManager) startDoneCallWorkers() {
	workers := runtime.NumCPU()
	stopWait := m.svc.getStopWait()
	stopWait.W.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer stopWait.W.Done()
			for {
				select {
				case call := <-m.completedCalls:
					m.invokeCallback(call)
				case <-stopWait.C:
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

// run 主循环.
func (m *rpcManager) run() {
	stopWait := m.svc.getStopWait()
	defer stopWait.W.Done()
	for {
		select {
		case c := <-m.chCmds:
			c.exec(m)
			c.release()
		case <-stopWait.C:
			m.stop()
			return
		}
	}
}

// enqueueCmd 入队指令.
func (m *rpcManager) enqueueCmd(c rpcCmd) error {
	chStop := m.svc.getStopWait().C
	select {
	case m.chCmds <- c:
		return nil
	case <-chStop:
		return ErrServiceStopped
	}
}

// genReqId 生成 req id.
func (m *rpcManager) genReqId() uint32 {
	return atomic.AddUint32(&m.reqIdIncr, 1)
}

// add 添加 Call.
func (m *rpcManager) add(call *rpcCall) {
	m.callMap[call.reqId] = call
	m.callHeap.Push(call)
}

// rem 移除 Call.
func (m *rpcManager) rem(call *rpcCall) {
	m.callHeap.Remove(call.heapIndex)
	delete(m.callMap, call.reqId)
}

// addCall 添加新的 Call.
func (m *rpcManager) addCall(call *rpcCall) {
	if _, exists := m.callMap[call.reqId]; exists {
		m.svc.getLogger().ErrorFields("rpc call reqId:%d duplicate", lfdReqId(call.reqId))
		m.handleCallDone(call, nil, ErrRPCCallDuplicate)
		return
	}
	m.add(call)
	if call == m.callHeap.Top() {
		m.resetTimer(call.expiredAt)
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
		m.enqueueCmd(&rpcCmdTick{tid: TimerIdNone})
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
	m.enqueueCmd(&rpcCmdTick{tid: args.TID})
}

// tick 处理已过期的 Call.
func (m *rpcManager) tick() {
	for {
		if m.callHeap.Len() == 0 {
			break
		}

		call := m.callHeap.Top()
		if m.svc.getTimeSystem().Now().UnixNano() < call.expiredAt {
			m.resetTimer(call.expiredAt)
			break
		}

		m.rem(call)
		m.handleCallDone(call, nil, ErrRPCTimeout)
	}
}

// createCall 创建 Call.
func (m *rpcManager) createCall(to ActorUID, expiredAt time.Time, cb RPCFunc) (uint32, error) {
	call := poolOfRPCCall.Get().(*rpcCall)
	call.heapIndex = -1
	call.reqId = m.genReqId()
	call.to = to
	call.expiredAt = expiredAt.UnixNano()
	call.cb = cb

	if err := m.enqueueCmd(&rpcCmdAdd{call: call}); err != nil {
		return 0, err
	}
	return call.reqId, nil
}

// removeCall 通过 reqId 移除 Call.
// 通常在出现错误需要直接移除 Call 的情况下调用.
func (m *rpcManager) removeCall(reqId uint32) {
	m.enqueueCmd(&rpcCmdRem{reqId: reqId})
}

// popCall 移除 reqId 指定的 Call. 并重置定时器.
func (m *rpcManager) popCall(reqId uint32) *rpcCall {
	call := m.callMap[reqId]
	if call == nil {
		return nil
	}

	top := call == m.callHeap.Top()
	m.rem(call)
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
func (m *rpcManager) handleResponse(reqId uint32, payload *Buffer, err error) {
	if errors.Is(err, errCodeOK) {
		err = nil
	}
	m.enqueueCmd(newRPCCmdResp(reqId, payload, err))
}

// rpcCmd RPC 指令.
type rpcCmd interface {
	exec(rm *rpcManager)
	release()
}

// rpcCmdAdd 添加 Call 指令.
type rpcCmdAdd struct {
	call *rpcCall
}

var poolOfRPCCmdAdd = &sync.Pool{
	New: func() interface{} {
		return &rpcCmdAdd{}
	},
}

func newRPCCmdAdd(call *rpcCall) *rpcCmdAdd {
	c := poolOfRPCCmdAdd.Get().(*rpcCmdAdd)
	c.call = call
	return c
}

func (c *rpcCmdAdd) exec(rm *rpcManager) {
	rm.addCall(c.call)
}

func (c *rpcCmdAdd) release() {
	c.call = nil
	poolOfRPCCmdAdd.Put(c)
}

// rpcCmdRem 移除 Call 指令.
type rpcCmdRem struct {
	reqId uint32
}

func (c *rpcCmdRem) exec(rm *rpcManager) {
	call := rm.popCall(c.reqId)
	if call != nil {
		call.release()
	}
}

func (c *rpcCmdRem) release() {}

// rpcCmdResp Response 指令.
type rpcCmdResp struct {
	reqId   uint32
	payload Buffer
	err     error
}

var poolOfRPCCmdResp = &sync.Pool{
	New: func() interface{} {
		return &rpcCmdResp{}
	},
}

func newRPCCmdResp(reqId uint32, payload *Buffer, err error) *rpcCmdResp {
	c := poolOfRPCCmdResp.Get().(*rpcCmdResp)
	c.reqId = reqId
	if payload != nil {
		c.payload = *payload
	}
	c.err = err
	return c
}

func (c *rpcCmdResp) exec(rm *rpcManager) {
	call := rm.popCall(c.reqId)
	if call == nil {
		rm.svc.getLogger().ErrorFields("rpcCall not found", lfdReqId(c.reqId))
		return
	}
	rm.handleCallDone(call, &c.payload, c.err)
}

func (c *rpcCmdResp) release() {
	c.payload.SetBuf(nil)
	c.err = nil
	poolOfRPCCmdResp.Put(c)
}

// rpcCmdTick tick 指令.
type rpcCmdTick struct {
	tid TimerId
}

func (c *rpcCmdTick) exec(rm *rpcManager) {
	if c.tid != rm.timerId {
		return
	}
	rm.timerId = TimerIdNone
	rm.tick()
}

func (c *rpcCmdTick) release() {}
