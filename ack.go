package gactor

import (
	"container/list"
	"sync"
	"time"

	"github.com/godyy/gactor/internal/utils"
	"github.com/godyy/glog"
)

// AckConfig Ack 配置.
type AckConfig struct {
	// MaxPacketAmount 最大数据包数量.
	MaxPacketAmount int64

	// Timeout 超时时间. 若超过该时间未收到 Ack 回复. 将重试发送数据包.
	Timeout time.Duration

	// MaxRetry 最大重试次数.
	MaxRetry uint8

	// TickInterval Tick 时间间隔.
	TickInterval time.Duration
}

func (c *AckConfig) init() {
	if c.MaxPacketAmount <= 0 {
		panic("gactor: AckConfig: MaxPacketAmount must be > 0")
	}

	if c.Timeout <= 0 {
		panic("gactor: AckConfig: Timeout must be > 0")
	}

	if c.MaxRetry <= 0 {
		panic("gactor: AckConfig: MaxRetry must be > 0")
	}

	if c.TickInterval <= 0 {
		panic("gactor: AckConfig: TickInterval must be > 0")
	}
}

// ackPacket 待确认的数据包.
type ackPacket struct {
	pt     PacketType // 数据包类型.
	seq    uint32     // 数据包序号.
	b      []byte     // 数据.
	nodeId string     // 目标节点ID.
}

func genIdOfPacket2Ack(pt PacketType, seq uint32) int64 {
	return int64(pt)<<32 | int64(seq)
}

// packet2Ack 待确认数据包.
type packet2Ack struct {
	ackPacket
	expireAt int64 // 过期时间.
	retry    uint8 // 重试次数.
}

var poolOfPacket2Ack = &sync.Pool{
	New: func() interface{} {
		return &packet2Ack{}
	},
}

// newPacket2Ack 构造 packet2Ack.
func newPacket2Ack(ap ackPacket) *packet2Ack {
	p := poolOfPacket2Ack.Get().(*packet2Ack)
	p.ackPacket = ap
	return p
}

// release 释放数据包.
func (p *packet2Ack) release() {
	p.nodeId = ""
	p.b = nil
	poolOfPacket2Ack.Put(p)
}

// id 返回 packet2Ack 唯一ID.
func (p *packet2Ack) id() int64 {
	return genIdOfPacket2Ack(p.pt, p.seq)
}

// ackOverReason Ack 结束原因.
type ackOverReason int8

const (
	ackOverReasonOK         = ackOverReason(iota) // 成功.
	ackOverReasonRetryLimit                       // 重试限制.
	ackOverReasonRem                              // 被删除.
)

func (r ackOverReason) isFailed() bool {
	return r != ackOverReasonOK && r != ackOverReasonRem
}

// ackHandler Ack 处理器封装, 对接 ackManager.
type ackHandler interface {
	// onAckRetry 通知 Ack 重试.
	onAckRetry(ap ackPacket)

	// onAckOver 通知Ack结束.
	onAckOver(ap ackPacket, reason ackOverReason)

	// getLogger 获取日志工具.
	getLogger() glog.Logger

	// getStopWait 获取停机工具.
	getStopWait() *utils.StopWait
}

// ackManager Ack 管理器.
type ackManager struct {
	cfg     *AckConfig  // 配置.
	handler ackHandler  // handler.
	chCmds  chan ackCmd // 指令队列.

	mtx sync.Mutex              // mutex.
	l   *list.List              // 数据包列表.
	m   map[int64]*list.Element // 数据包唯一ID到数据包映射.
}

func newAckManager(cfg *AckConfig, handler ackHandler) *ackManager {
	cfg.init()
	return &ackManager{
		cfg:     cfg,
		handler: handler,
		chCmds:  make(chan ackCmd, cfg.MaxPacketAmount),
		l:       list.New(),
		m:       make(map[int64]*list.Element),
	}
}

func (m *ackManager) getCfg() *AckConfig {
	return m.cfg
}

// enqueueCmd 入队指令.
func (m *ackManager) enqueueCmd(c ackCmd) {
	chStop := m.handler.getStopWait().C
	select {
	case m.chCmds <- c:
	case <-chStop:
	}
}

// addPacket 添加待确认数据包.
func (m *ackManager) addPacket(ap ackPacket) {
	// 构造数据包对象
	p := newPacket2Ack(ap)

	// 添加指令.
	chStop := m.handler.getStopWait().C
	select {
	case m.chCmds <- newAckCmdAdd(p):
	case <-chStop:
	default:
		m.handler.getLogger().ErrorFields("add packet2Ack but chan is full", lfdPacketType(ap.pt), lfdSeq(ap.seq))
	}
}

// remPacket 移除待确认数据包.
func (m *ackManager) remPacket(pt PacketType, seq uint32) {
	id := genIdOfPacket2Ack(pt, seq)
	m.enqueueCmd(newAckCmdRem(id))
}

// receiveAck 接收到 Ack.
func (m *ackManager) receiveAck(pt PacketType, seq uint32) {
	id := genIdOfPacket2Ack(pt, seq)
	m.enqueueCmd(newAckCmdAck(id))
}

// addPacketInternal 内部添加待确认数据包.
func (m *ackManager) addPacketInternal(p *packet2Ack) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	id := p.id()

	// 检查ID是否存在.
	if _, exists := m.m[id]; exists {
		m.handler.getLogger().ErrorFields("packet2Ack already exists", lfdPacketType(p.pt), lfdSeq(p.seq), lfdId(id))
		return
	}

	// 保存数据包.
	p.expireAt = time.Now().Add(m.cfg.Timeout).UnixNano()
	e := m.l.PushBack(p)
	m.m[id] = e
}

// remPacketInternal 内部移除数据包.
func (m *ackManager) remPacketInternal(id int64) (ap ackPacket, removed bool) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	// 查找数据包.
	e, exists := m.m[id]
	if !exists {
		return
	}

	// 删除数据包.
	delete(m.m, id)
	p := m.l.Remove(e).(*packet2Ack)
	ap = p.ackPacket
	p.release()
	removed = true

	return
}

// start 启动 Ack 管理器.
func (m *ackManager) start() {
	m.handler.getStopWait().W.Add(2)
	go m.processCmds()
	go m.processExpirePackets()
}

// processCmds 处理指令.
func (m *ackManager) processCmds() {
	stopWait := m.handler.getStopWait()
	defer stopWait.W.Done()
	for {
		select {
		case c := <-m.chCmds:
			c.exec(m)
			c.release()
		case <-stopWait.C:
			return
		}
	}
}

// processExpirePackets 处理过期数据包.
func (m *ackManager) processExpirePackets() {
	stopWait := m.handler.getStopWait()
	defer stopWait.W.Done()

	ticker := time.NewTicker(m.cfg.TickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.updateExpirePackets()
		case <-stopWait.C:
			return
		}
	}
}

// updateExpirePackets 更新过期数据包.
func (m *ackManager) updateExpirePackets() {
	for {
		m.mtx.Lock()

		// 若列表为空, 中断.
		if m.l.Len() == 0 {
			m.mtx.Unlock()
			break
		}

		// 检查是否超时.
		e := m.l.Front()
		p := e.Value.(*packet2Ack)
		if p.expireAt > time.Now().UnixNano() {
			m.mtx.Unlock()
			break
		}

		// 若重试达到上限, 移除.
		if p.retry >= m.cfg.MaxRetry {
			m.l.Remove(e)
			delete(m.m, p.id())
			m.mtx.Unlock()
			// 通报ack重试失败.
			m.handler.onAckOver(p.ackPacket, ackOverReasonRetryLimit)
			p.release()
			continue
		}

		// 重试.
		{
			// 提取参数.
			ap := p.ackPacket

			// 更新数据包信息及排位.
			p.expireAt += m.cfg.TickInterval.Nanoseconds()
			p.retry++
			m.l.MoveToBack(e)

			// 通知重试.
			m.mtx.Unlock()
			m.handler.onAckRetry(ap)
		}
	}
}

// ackCmd 指令.
type ackCmd interface {
	// exec 执行指令.
	exec(*ackManager)
	// release 释放指令.
	release()
}

// ackCmdAdd 添加数据包.
type ackCmdAdd struct {
	p *packet2Ack
}

var poolOfAckCmdAdd = &sync.Pool{
	New: func() interface{} {
		return &ackCmdAdd{}
	},
}

func newAckCmdAdd(p *packet2Ack) *ackCmdAdd {
	c := poolOfAckCmdAdd.Get().(*ackCmdAdd)
	c.p = p
	return c
}

func (c *ackCmdAdd) exec(m *ackManager) {
	m.addPacketInternal(c.p)
}

func (c *ackCmdAdd) release() {
	c.p = nil
	poolOfAckCmdAdd.Put(c)
}

// ackCmdAck 确认数据包.
type ackCmdAck struct {
	id int64
}

var poolOfAckCmdAck = &sync.Pool{
	New: func() interface{} {
		return &ackCmdAck{}
	},
}

func newAckCmdAck(id int64) *ackCmdAck {
	c := poolOfAckCmdAck.Get().(*ackCmdAck)
	c.id = id
	return c
}

func (c *ackCmdAck) exec(m *ackManager) {
	ap, removed := m.remPacketInternal(c.id)
	if removed {
		m.handler.onAckOver(ap, ackOverReasonOK)
	} else {
		m.handler.getLogger().ErrorFields("ack packet2Ack not exists", lfdId(c.id))
	}
}

func (c *ackCmdAck) release() {
	c.id = 0
	poolOfAckCmdAck.Put(c)
}

// ackCmdRem 移除数据包.
type ackCmdRem struct {
	id int64
}

func newAckCmdRem(id int64) *ackCmdRem {
	return &ackCmdRem{id: id}
}

func (c *ackCmdRem) exec(m *ackManager) {
	ap, removed := m.remPacketInternal(c.id)
	if removed {
		m.handler.onAckOver(ap, ackOverReasonRem)
	}
}

func (c *ackCmdRem) release() {}
