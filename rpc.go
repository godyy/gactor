package gactor

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/godyy/gutils/container/heap"
)

// errRPCCallDuplicate RPC 调用重复错误.
var errRPCCallDuplicate = errors.New("gactor: rpc call duplicate")

// RPCResp RPC 响应参数.
type RPCResp struct {
	svc     *Service // Service.
	ctx     any      // 上下文.
	payload *Buffer  // 响应负载数据.
	err     error    // 错误信息.
}

// Ctx 返回上下文.
func (resp *RPCResp) Ctx() any {
	return resp.ctx
}

// Err 返回错误信息.
func (resp *RPCResp) Err() error {
	return resp.err
}

// DecodeReply 解码响应负载数据到 v.
func (resp *RPCResp) DecodeReply(v any) error {
	if resp.err != nil {
		return resp.err
	}
	if err := resp.svc.decodePayload(PacketTypeS2SRpcResp, resp.payload, v); err == nil {
		return nil
	} else {
		return err
	}
}

// RPCFunc RPC回调.
// RPCCall 对象只能在回调内部访问, 切记不要传递到回调函数外.
type RPCFunc func(RPCResp)

// rpcCall 内部 RPC 调用实例实现.
type rpcCall struct {
	heapIndex   int      // heap index.
	reqId       uint32   // RPC 调用序列 ID.
	from, to    ActorUID // 来源, 目标 ActorUID.
	expiredAt   int64    // 过期时间.
	cb          RPCFunc  // 回调函数.
	cbctx       any      // 回调上下文.
	respPayload *Buffer  // RPC 响应负载数据.
	err         error    // 错误信息.
}

var poolOfRPCCall = &sync.Pool{New: func() any {
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
	call.respPayload = payload
	call.err = err
}

func (call *rpcCall) callback(rm *rpcManager) {
	call.cb(RPCResp{
		svc:     rm.getSvc(),
		ctx:     call.cbctx,
		payload: call.respPayload,
		err:     call.err,
	})
	call.respPayload = nil
}

func (call *rpcCall) release(rm *rpcManager) {
	if call.respPayload != nil {
		rm.getSvc().freeBuffer(call.respPayload)
		call.respPayload = nil
	}
	call.cbctx = nil
	call.cb = nil
	call.err = nil
	poolOfRPCCall.Put(call)
}

// rpcManager RPC Manager.
type rpcManager struct {
	svc            *Service      // Service.
	chCmds         chan rpcCmd   // 指令通道.
	completedCalls chan *rpcCall // 已完成等待执行回调的 rpcCall.

	reqIdIncr uint32               // req id 自增键.
	enqueueMu sync.RWMutex         // enqueue 屏障.
	stopping  bool                 // 停机标记.
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
		m.handleCallDone(call, nil, errServiceStopped)
	}
	m.callMap = nil
	m.callHeap.Init()
}

func (m *rpcManager) drainCmds() {
	for {
		select {
		case c := <-m.chCmds:
			c.exec(m)
			c.release(m)
		default:
			return
		}
	}
}

// startDoneCallWorkers 启动已完成 RPC 调用的回调执行工作器.
func (m *rpcManager) startDoneCallWorkers() {
	workers := runtime.NumCPU()
	stopWait := m.svc.getStopWait()
	stopWait.W.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer stopWait.W.Done()
			for call := range m.completedCalls {
				m.invokeCallback(call)
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
			c.release(m)
		case <-stopWait.C:
			m.enqueueMu.Lock()
			m.stopping = true
			m.enqueueMu.Unlock()
			m.drainCmds()
			m.stop()
			close(m.completedCalls)
			return
		}
	}
}

// enqueueCmd 入队指令.
func (m *rpcManager) enqueueCmd(c rpcCmd, ignoreBusy bool) error {
	m.enqueueMu.RLock()
	defer m.enqueueMu.RUnlock()
	if m.stopping {
		c.discard(m)
		return errServiceStopped
	}
	chStop := m.svc.getStopWait().C
	if ignoreBusy {
		begin := time.Now()
		select {
		case m.chCmds <- c:
			if cost := time.Since(begin); cost > m.svc.getCfg().QueueWriteTimeAlarmThreshold {
				m.svc.getLogger().Warnf("rpc enqueue slowly, cost:%dms", cost.Milliseconds())
			}
			return nil
		case <-chStop:
			c.discard(m)
			return errServiceStopped
		}
	}
	select {
	case m.chCmds <- c:
		return nil
	case <-chStop:
		c.discard(m)
		return errServiceStopped
	default:
		m.svc.getLogger().Warn("rpc enqueue failed, service busy")
		c.discard(m)
		return ErrServiceBusy
	}
}

// getSvc 获取 Service.
func (m *rpcManager) getSvc() *Service {
	return m.svc
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

// invokeCallback 调用 RPC 回调.
func (m *rpcManager) invokeCallback(call *rpcCall) {
	defer recoverAndLog("invoke rpc callback panic", m.svc.getLogger(), nil)

	call.callback(m)
	call.release(m)
}

// resetTimer 重置定时器.
func (m *rpcManager) resetTimer(expiredAt int64) {
	if m.timerId != TimerIdNone {
		m.svc.StopTimer(m.timerId)
		m.timerId = TimerIdNone
	}

	d := time.Duration(expiredAt - time.Now().UnixNano())
	if d <= 0 {
		d = time.Millisecond * 1
	}
	m.timerId = m.svc.startTimer(d, false, m.onTimer, nil, true)
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
	m.enqueueCmd(&rpcCmdTick{tid: args.TID}, true)
}

// tick 处理已过期的 Call.
func (m *rpcManager) tick() {
	for {
		if m.callHeap.Len() == 0 {
			break
		}

		call := m.callHeap.Top()
		if time.Now().UnixNano() < call.expiredAt {
			m.resetTimer(call.expiredAt)
			break
		}

		m.rem(call)
		m.handleCallDone(call, nil, ErrTimeout)
	}
}

// createCall 创建 Call.
func (m *rpcManager) createCall(from, to ActorUID, expiredAt time.Time, cb RPCFunc, cbctx any) (uint32, error) {
	call := poolOfRPCCall.Get().(*rpcCall)
	call.heapIndex = -1
	call.reqId = m.genReqId()
	call.from = from
	call.to = to
	call.expiredAt = expiredAt.UnixNano()
	call.cbctx = cbctx
	call.cb = cb
	if err := m.enqueueCmd(newRPCCmdAdd(call), false); err != nil {
		return 0, err
	}
	return call.reqId, nil
}

// removeCall 通过 reqId 移除 Call.
// 通常在出现错误需要直接移除 Call 的情况下调用.
func (m *rpcManager) removeCall(reqId uint32, from, to ActorUID) {
	m.enqueueCmd(&rpcCmdRem{reqId: reqId, from: from, to: to}, true)
}

// popCall 移除 reqId 指定的 Call. 并重置定时器.
func (m *rpcManager) popCall(reqId uint32, from, to ActorUID) *rpcCall {
	call := m.callMap[reqId]
	if call == nil || call.from != from || call.to != to {
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
	} else if errors.Is(err, ErrTimeout) {
		m.svc.monitorRPCAction(MonitorCARPCTimeout)
	} else {
		m.svc.monitorRPCAction(MonitorCAResponseErr)
	}

	call.onResponse(payload, err)
	m.completedCalls <- call
}

// handleResponse 处理 RPC Response. 返回对应的 rpcCall 是否存在.
func (m *rpcManager) handleResponse(reqId uint32, from, to ActorUID, payload *Buffer, err error) {
	_ = m.enqueueCmd(newRPCCmdResp(reqId, from, to, payload, err), true)
}

// rpcCmd RPC 指令.
type rpcCmd interface {
	exec(rm *rpcManager)
	release(rm *rpcManager)
	discard(rm *rpcManager)
}

// rpcCmdAdd 添加 Call 指令.
type rpcCmdAdd struct {
	call *rpcCall
}

var poolOfRPCCmdAdd = &sync.Pool{
	New: func() any {
		return &rpcCmdAdd{}
	},
}

func newRPCCmdAdd(call *rpcCall) *rpcCmdAdd {
	c := poolOfRPCCmdAdd.Get().(*rpcCmdAdd)
	c.call = call
	return c
}

func (c *rpcCmdAdd) exec(rm *rpcManager) {
	call := c.call
	if _, exists := rm.callMap[call.reqId]; exists {
		rm.svc.getLogger().ErrorFields("rpc call reqId:%d duplicate", lfdReqId(call.reqId))
		rm.handleCallDone(call, nil, errRPCCallDuplicate)
		return
	}
	rm.add(call)
	if call == rm.callHeap.Top() {
		rm.resetTimer(call.expiredAt)
	}
}

func (c *rpcCmdAdd) release(rm *rpcManager) {
	c.call = nil
	poolOfRPCCmdAdd.Put(c)
}
func (c *rpcCmdAdd) discard(rm *rpcManager) {
	if c.call != nil {
		c.call.release(rm)
		c.call = nil
	}
	c.release(rm)
}

// rpcCmdRem 移除 Call 指令.
type rpcCmdRem struct {
	reqId    uint32
	from, to ActorUID
}

func (c *rpcCmdRem) exec(rm *rpcManager) {
	call := rm.popCall(c.reqId, c.from, c.to)
	if call != nil {
		call.release(rm)
	}
}

func (c *rpcCmdRem) release(rm *rpcManager) {}
func (c *rpcCmdRem) discard(rm *rpcManager) {}

// rpcCmdResp Response 指令.
type rpcCmdResp struct {
	reqId    uint32
	from, to ActorUID
	payload  *Buffer
	err      error
}

var poolOfRPCCmdResp = &sync.Pool{
	New: func() any {
		return &rpcCmdResp{}
	},
}

func newRPCCmdResp(reqId uint32, from, to ActorUID, payload *Buffer, err error) *rpcCmdResp {
	c := poolOfRPCCmdResp.Get().(*rpcCmdResp)
	c.reqId = reqId
	c.from, c.to = from, to
	c.payload = payload
	c.err = err
	return c
}

func (c *rpcCmdResp) exec(rm *rpcManager) {
	call := rm.popCall(c.reqId, c.to, c.from)
	if call == nil {
		rm.svc.getLogger().ErrorFields("[HandleRPCResponse] call not found",
			lfdReqId(c.reqId), rm.svc.lfdActorUID("fromId", c.from), rm.svc.lfdActorUID("toId", c.to))
		return
	}
	rm.handleCallDone(call, c.payload, c.err)
	c.payload = nil
}

func (c *rpcCmdResp) release(rm *rpcManager) {
	if c.payload != nil {
		rm.getSvc().freeBuffer(c.payload)
		c.payload = nil
	}
	c.err = nil
	poolOfRPCCmdResp.Put(c)
}
func (c *rpcCmdResp) discard(rm *rpcManager) { c.release(rm) }

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

func (c *rpcCmdTick) release(rm *rpcManager) {}
func (c *rpcCmdTick) discard(rm *rpcManager) {}
