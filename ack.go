package gactor

import (
	"container/list"
	"fmt"
	"sync"
	"time"
)

// AckConfig Ack 配置.
type AckConfig struct {
	// Timeout 超时时间. 若超过该时间未收到 Ack 回复. 将重试发送数据包.
	Timeout time.Duration

	// MaxRetry 最大重试次数.
	MaxRetry uint8

	// TickInterval Tick 时间间隔.
	TickInterval time.Duration
}

func (c *AckConfig) init() {
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

// packet2Ack 待确认数据包.
type packet2Ack struct {
	id       int64  // 数据包唯一标识.
	nodeId   string // 目标节点ID.
	b        []byte // 数据.
	expireAt int64  // 过期时间.
	retry    uint8  // 重试次数.
}

func genIdOfPacket2Ack(pt PacketType, seq uint32) int64 {
	return int64(pt)<<32 | int64(seq)
}

// newPacket2Ack 构造 packet2Ack.
func newPacket2Ack(id int64, nodeId string, b []byte) *packet2Ack {
	return &packet2Ack{
		id:     id,
		nodeId: nodeId,
		b:      b,
	}
}

// release 释放数据包.
func (p *packet2Ack) release() {
	p.nodeId = ""
	p.b = nil
}

// ackOwner Ack 功能所有者功能封装.
type ackOwner interface {
	// onAckRetry 通知 Ack 重试.
	onAckRetry(nodeId string, pt PacketType, seq uint32, b []byte)

	// onAckFailed 通知 Ack 失败.
	onAckFailed(nodeId string, pt PacketType, seq uint32, b []byte)
}

// ackManager Ack 管理器.
type ackManager struct {
	cfg   *AckConfig // 配置.
	owner ackOwner   // Owner.

	mtx sync.Mutex
	l   *list.List            // 数据包列表.
	m   map[any]*list.Element // 数据包唯一ID到数据包映射.
}

func newAckManager(cfg *AckConfig, owner ackOwner) *ackManager {
	cfg.init()
	return &ackManager{
		cfg:   cfg,
		owner: owner,
		l:     list.New(),
		m:     make(map[any]*list.Element),
	}
}

func (m *ackManager) getCfg() *AckConfig {
	return m.cfg
}

// addPacket 添加数据包.
func (m *ackManager) addPacket(nodeId string, pt PacketType, seq uint32, b []byte) error {
	// 构造数据包对象.
	p := newPacket2Ack(genIdOfPacket2Ack(pt, seq), nodeId, b)

	m.mtx.Lock()
	defer m.mtx.Unlock()

	// 检查ID是否存在.
	if _, exists := m.m[p.id]; exists {
		return fmt.Errorf("packet2Ack pt:%d seq:%d already exists", pt, seq)
	}

	// 设置过期时间.
	p.expireAt = time.Now().Add(m.cfg.Timeout).UnixNano()

	// 保存数据包.
	e := m.l.PushBack(p)
	m.m[p.id] = e

	return nil
}

// remPacket 移除数据包.
func (m *ackManager) remPacket(pt PacketType, seq uint32) (removed bool, b []byte) {
	id := genIdOfPacket2Ack(pt, seq)

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
	b = p.b
	p.release()
	removed = true

	return
}

// tick Tick逻辑.
func (m *ackManager) tick() {
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
			delete(m.m, p.id)
			m.mtx.Unlock()
			// 通报ack重试失败.
			m.owner.onAckFailed(p.nodeId, PacketType(p.id>>32), uint32(p.id), p.b)
			p.release()
			continue
		}

		// 重试.
		{
			// 提取参数.
			nodeId := p.nodeId
			pt := PacketType(p.id >> 32)
			seq := uint32(p.id)
			b := p.b

			// 更新数据包信息及排位.
			p.expireAt += m.cfg.TickInterval.Nanoseconds()
			p.retry++
			m.l.MoveToBack(e)

			// 通知重试.
			m.mtx.Unlock()
			m.owner.onAckRetry(nodeId, pt, seq, b)
		}
	}
}
