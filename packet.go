package gactor

import (
	"sync"
	"unsafe"

	pkgerrors "github.com/pkg/errors"
)

// PacketType 数据包类型.
type PacketType = int8

const (
	PacketTypeUnknown = PacketType(0)

	PacketTypeAck        = PacketType(1) // Ack.
	PacketTypeConnect    = PacketType(2) // 连接.
	PacketTypeDisconnect = PacketType(3) // 连接断开.

	PacketTypeRawReq  = PacketType(11) // C2S 请求.
	PacketTypeRawResp = PacketType(12) // S2C 响应.
	PacketTypeRawPush = PacketType(13) // S2C 推送.

	PacketTypeS2SRpc            = PacketType(21) // S2S RPC调用.
	PacketTypeS2SRpcResp        = PacketType(22) // S2S RPC调用响应.
	PacketTypeS2SCast           = PacketType(23) // S2S 消息投递.
	PacketTypeS2SForward        = PacketType(24) // S2S 透传消息投递.
	PacketTypeS2SMigrateStart   = PacketType(25) // S2S 迁移启动.
	PacketTypeS2SMigrateResult  = PacketType(26) // S2S 迁移结果.
	PacketTypeS2SMigrateForward = PacketType(27) // S2S 迁移转发.
)

const sizeOfPacketType = int(unsafe.Sizeof(PacketType(0)))

// PacketBufAllocator 数据包缓冲区分配器.
type PacketBufAllocator func(ph PacketHead, payloadLen int) (*Buffer, error)

// PacketCodec 数据包编解码器.
type PacketCodec interface {
	BytesManager

	// Encode 编码数据包.
	// allocator 提供分配已 ph 作为数据包包头并附带足以写入
	// payload 数据的缓冲区.
	// allocator 返回的缓冲区已经编码写入 ph 数据。用户自行
	// 编码写入 payload即可.
	//
	// 数据包类型包括:
	// 	PacketTypeRawResp, PacketTypeRawPush
	//	PacketTypeS2SRpc, PacketTypeS2SRpcResp, PacketTypeS2SCast, PacketTypeS2SForward
	Encode(allocator PacketBufAllocator, ph PacketHead, payload any) (*Buffer, error)

	// EncodePayload 编码负载数据.
	// 根据数据包类型 pt 编码 payload 并生成数据切片返回.
	// 数据包类型包括:
	//	PacketTypeRawPush
	//	PacketTypeS2SRpc, PacketTypeS2SCast, PacketTypeS2SForward
	EncodePayload(pt PacketType, payload any) ([]byte, error)

	// DecodePayload 解码负载数据.
	// 根据数据包类型 pt 解码 b 中负载数据并填充入 v 指向的对象中.
	// 数据包类型包括:
	//	PacketTypeRawReq
	//	PacketTypeS2SRpc, PacketTypeS2SRpcResp, PacketTypeS2SCast
	DecodePayload(pt PacketType, b *Buffer, v any) error
}

// PacketHead 定义数据包包头接口.
type PacketHead interface {
	// PacketType 数据包类型.
	PacketType() PacketType

	// 数据包序列号.
	getSeq() uint32

	// 获取长度.
	getLen() int

	// 编码数据包.
	encode(b *Buffer) error

	// 解码数据包.
	decode(b *Buffer) error

	// 释放包头.
	release()
}

// packetHandlerParams 数据包处理器参数.
type packetHandlerParams struct {
	nodeId string     // 来源节点.
	head   PacketHead // 包头.
	buf    *Buffer    // 数据包缓冲区.
}

// packetCreators 数据包创建器.
// 每个数据包类型对应一个创建器, 用于创建数据包包头.
var packetCreators = map[PacketType]func() PacketHead{
	PacketTypeAck:               func() PacketHead { return poolOfAckPacketHead.Get().(*ackPacketHead) },
	PacketTypeConnect:           func() PacketHead { return poolOfConnectPacketHead.Get().(*connectPacketHead) },
	PacketTypeDisconnect:        func() PacketHead { return poolOfDisconnectPacketHead.Get().(*disconnectPacketHead) },
	PacketTypeRawReq:            func() PacketHead { return poolOfRawReqPacketHead.Get().(*rawReqPacketHead) },
	PacketTypeRawResp:           func() PacketHead { return poolOfRawRespPacketHead.Get().(*rawRespPacketHead) },
	PacketTypeRawPush:           func() PacketHead { return poolOfRawPushPacketHead.Get().(*rawPushPacketHead) },
	PacketTypeS2SRpc:            func() PacketHead { return poolOfS2SRpcPacketHead.Get().(*s2sRpcPacketHead) },
	PacketTypeS2SRpcResp:        func() PacketHead { return poolOfS2SRpcRespPacketHead.Get().(*s2sRpcRespPacketHead) },
	PacketTypeS2SCast:           func() PacketHead { return poolOfS2SCastPacketHead.Get().(*s2sCastPacketHead) },
	PacketTypeS2SForward:        func() PacketHead { return poolOfS2SForwardPacketHead.Get().(*s2sForwardPacketHead) },
	PacketTypeS2SMigrateStart:   func() PacketHead { return &s2sMigrateStartPacketHead{} },
	PacketTypeS2SMigrateResult:  func() PacketHead { return &s2sMigrateResultPacketHead{} },
	PacketTypeS2SMigrateForward: func() PacketHead { return &s2sMigrateForwardPacketHead{} },
}

// createPacketByType 根据类型创建数据包包头.
func createPacketByType(pt PacketType) PacketHead {
	creator := packetCreators[pt]
	if creator == nil {
		return nil
	}
	return creator()
}

// ackPacketHead PacketTypeAck 包头.
type ackPacketHead struct {
	ackPt  PacketType // 确认的数据包类型.
	ackSeq uint32     // 确认的数据包序号.
}

var poolOfAckPacketHead = &sync.Pool{
	New: func() any {
		return &ackPacketHead{}
	},
}

func newAckHeadFrom(head PacketHead) *ackPacketHead {
	h := poolOfAckPacketHead.Get().(*ackPacketHead)
	h.ackPt = head.PacketType()
	h.ackSeq = head.getSeq()
	return h
}

const lenAckPacketHead = sizeOfPacketType + 4

func (ph *ackPacketHead) PacketType() PacketType {
	return PacketTypeAck
}

func (ph *ackPacketHead) getSeq() uint32 {
	return ph.ackSeq
}

func (ph *ackPacketHead) getLen() int {
	return lenAckPacketHead
}

func (ph *ackPacketHead) encode(b *Buffer) error {
	if err := b.WritePacketType(ph.ackPt); err != nil {
		return pkgerrors.WithMessage(err, "write ackPt")
	}
	if err := b.WriteUint32(ph.ackSeq); err != nil {
		return pkgerrors.WithMessage(err, "write ackSeq")
	}
	return nil
}

func (ph *ackPacketHead) decode(b *Buffer) (err error) {
	ph.ackPt, err = b.ReadPacketType()
	if err != nil {
		return pkgerrors.WithMessage(err, "read ackPt")
	}
	ph.ackSeq, err = b.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read ackSeq")
	}
	return
}

func (ph *ackPacketHead) release() {
	poolOfAckPacketHead.Put(ph)
}

// connectPacketHead PacketTypeConnect 包头.
type connectPacketHead struct {
	seq uint32  // 序号.
	id  ActorID // Actor ID
	sid uint32  // 会话ID.
}

var poolOfConnectPacketHead = &sync.Pool{
	New: func() any {
		return &connectPacketHead{}
	},
}

func newConnectHead(seq uint32, id ActorID, sid uint32) *connectPacketHead {
	h := poolOfConnectPacketHead.Get().(*connectPacketHead)
	h.seq = seq
	h.id = id
	h.sid = sid
	return h
}

const lenConnectPacketHead = 4 + lenOfActorUID + 4

func (ph *connectPacketHead) PacketType() PacketType {
	return PacketTypeConnect
}

func (ph *connectPacketHead) getSeq() uint32 {
	return ph.seq
}

func (ph *connectPacketHead) getLen() int {
	return lenConnectPacketHead
}

// 编码数据包.
func (ph *connectPacketHead) encode(b *Buffer) error {
	if err := b.WriteUint32(ph.seq); err != nil {
		return pkgerrors.WithMessage(err, "write seq")
	}
	if err := b.WriteInt64(ph.id); err != nil {
		return pkgerrors.WithMessage(err, "write uid")
	}
	if err := b.WriteUint32(ph.sid); err != nil {
		return pkgerrors.WithMessage(err, "write sid")
	}
	return nil
}

// 解码数据包.
func (ph *connectPacketHead) decode(b *Buffer) (err error) {
	ph.seq, err = b.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read seq")
	}
	ph.id, err = b.ReadInt64()
	if err != nil {
		return pkgerrors.WithMessage(err, "read uid")
	}
	ph.sid, err = b.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read sid")
	}
	return nil
}

func (ph *connectPacketHead) release() {
	poolOfConnectPacketHead.Put(ph)
}

// disconnectPacketHead PacketTypeDisconnect 包头.
type disconnectPacketHead struct {
	seq uint32  // 序号.
	id  ActorID // Actor ID.
	sid uint32  // 会话ID.
}

var poolOfDisconnectPacketHead = &sync.Pool{
	New: func() any {
		return &disconnectPacketHead{}
	},
}

func newDisconnectHead(seq uint32, id ActorID, sid uint32) *disconnectPacketHead {
	h := poolOfDisconnectPacketHead.Get().(*disconnectPacketHead)
	h.seq = seq
	h.id = id
	h.sid = sid
	return h
}

const lenS2SDisconnectedPacketHead = 4 + lenOfActorUID + 4

func (ph *disconnectPacketHead) PacketType() PacketType { return PacketTypeDisconnect }

func (ph *disconnectPacketHead) getSeq() uint32 { return ph.seq }

func (ph *disconnectPacketHead) getLen() int {
	return lenS2SDisconnectedPacketHead
}

func (ph *disconnectPacketHead) encode(b *Buffer) error {
	if err := b.WriteUint32(ph.seq); err != nil {
		return pkgerrors.WithMessage(err, "write seq")
	}
	if err := b.WriteInt64(ph.id); err != nil {
		return pkgerrors.WithMessage(err, "write toId")
	}
	if err := b.WriteUint32(ph.sid); err != nil {
		return pkgerrors.WithMessage(err, "write sid")
	}
	return nil
}

func (ph *disconnectPacketHead) decode(b *Buffer) (err error) {
	ph.seq, err = b.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read seq")
	}
	ph.id, err = b.ReadInt64()
	if err != nil {
		return pkgerrors.WithMessage(err, "read toId")
	}
	ph.sid, err = b.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read sid")
	}
	return
}

func (ph *disconnectPacketHead) release() {
	poolOfDisconnectPacketHead.Put(ph)
}

// rawReqPacketHead PacketTypeRawReq 包头.
type rawReqPacketHead struct {
	seq     uint32  // 序号.
	toId    ActorID // 目标 Actor ID.
	sid     uint32  // 会话ID.
	timeout uint32  // 超时.
}

var poolOfRawReqPacketHead = &sync.Pool{
	New: func() any {
		return &rawReqPacketHead{}
	},
}

func newRawReqHead(seq uint32, toId ActorID, sid uint32, timeout uint32) *rawReqPacketHead {
	h := poolOfRawReqPacketHead.Get().(*rawReqPacketHead)
	h.seq = seq
	h.toId = toId
	h.sid = sid
	h.timeout = timeout
	return h
}

const lenRawReqPacketHead = 4 + lenOfActorUID + 4 + 4

func (ph *rawReqPacketHead) PacketType() PacketType { return PacketTypeRawReq }

func (ph *rawReqPacketHead) getSeq() uint32 { return ph.seq }

func (ph *rawReqPacketHead) getLen() int {
	return lenRawReqPacketHead
}

func (ph *rawReqPacketHead) encode(b *Buffer) error {
	if err := b.WriteUint32(ph.seq); err != nil {
		return pkgerrors.WithMessage(err, "write seq")
	}
	if err := b.WriteInt64(ph.toId); err != nil {
		return pkgerrors.WithMessage(err, "write toId")
	}
	if err := b.WriteUint32(ph.sid); err != nil {
		return pkgerrors.WithMessage(err, "write sid")
	}
	if err := b.WriteUint32(ph.timeout); err != nil {
		return pkgerrors.WithMessage(err, "write timeout")
	}
	return nil
}

func (ph *rawReqPacketHead) decode(b *Buffer) (err error) {
	ph.seq, err = b.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read seq")
	}
	ph.toId, err = b.ReadInt64()
	if err != nil {
		return pkgerrors.WithMessage(err, "read toId")
	}
	ph.sid, err = b.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read sid")
	}
	ph.timeout, err = b.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read timeout")
	}
	return
}

func (ph *rawReqPacketHead) release() {
	poolOfRawReqPacketHead.Put(ph)
}

// rawRespPacketHead PacketTypeRawResp 包头.
type rawRespPacketHead struct {
	seq     uint32  // 序号.
	fromId  ActorID // 来自 Actor ID.
	sid     uint32  // 会话ID.
	errCode ErrCode // 错误码.
}

var poolOfRawRespPacketHead = &sync.Pool{
	New: func() any {
		return &rawRespPacketHead{}
	},
}

func newRawRespHead(seq uint32, fromId ActorID, sid uint32, errCode ErrCode) *rawRespPacketHead {
	h := poolOfRawRespPacketHead.Get().(*rawRespPacketHead)
	h.seq = seq
	h.fromId = fromId
	h.sid = sid
	h.errCode = errCode
	return h
}

func newRawRespHeadFromReq(seq uint32, errCode ErrCode, req *rawReqPacketHead) *rawRespPacketHead {
	return &rawRespPacketHead{seq: seq, fromId: req.toId, sid: req.sid, errCode: errCode}
}

const lenRawRespPacketHead = 4 + lenOfActorUID + 4 + sizeOfErrCode

func (ph *rawRespPacketHead) PacketType() PacketType { return PacketTypeRawResp }

func (ph *rawRespPacketHead) getSeq() uint32 { return ph.seq }

func (ph *rawRespPacketHead) getLen() int {
	return lenRawRespPacketHead
}

func (ph *rawRespPacketHead) encode(b *Buffer) error {
	if err := b.WriteUint32(ph.seq); err != nil {
		return pkgerrors.WithMessage(err, "write seq")
	}
	if err := b.WriteInt64(ph.fromId); err != nil {
		return pkgerrors.WithMessage(err, "write fromId")
	}
	if err := b.WriteUint32(ph.sid); err != nil {
		return pkgerrors.WithMessage(err, "write sid")
	}
	if err := b.WriteErrCode(ph.errCode); err != nil {
		return pkgerrors.WithMessage(err, "write errCode")
	}
	return nil
}

func (ph *rawRespPacketHead) decode(b *Buffer) (err error) {
	ph.seq, err = b.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read seq")
	}
	ph.fromId, err = b.ReadInt64()
	if err != nil {
		return pkgerrors.WithMessage(err, "read fromId")
	}
	ph.sid, err = b.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read sid")
	}
	ph.errCode, err = b.ReadErrCode()
	if err != nil {
		return pkgerrors.WithMessage(err, "read errCode")
	}
	return
}

func (ph *rawRespPacketHead) release() {
	poolOfRawRespPacketHead.Put(ph)
}

// rawPushPacketHead PacketTypeRawPush 包头.
type rawPushPacketHead struct {
	seq    uint32  // 序号.
	fromId ActorID // 来自 Actor ID.
	sid    uint32  // 会话ID.
}

var poolOfRawPushPacketHead = &sync.Pool{
	New: func() any {
		return &rawPushPacketHead{}
	},
}

func newRawPushHead(seq uint32, fromId ActorID, sid uint32) *rawPushPacketHead {
	h := poolOfRawPushPacketHead.Get().(*rawPushPacketHead)
	h.seq = seq
	h.fromId = fromId
	h.sid = sid
	return h
}

const lenRawPushPacketHead = 4 + lenOfActorUID + 4

func (ph *rawPushPacketHead) PacketType() PacketType { return PacketTypeRawPush }

func (ph *rawPushPacketHead) getSeq() uint32 { return ph.seq }

func (ph *rawPushPacketHead) getLen() int {
	return lenRawPushPacketHead
}

func (ph *rawPushPacketHead) encode(b *Buffer) error {
	if err := b.WriteUint32(ph.seq); err != nil {
		return pkgerrors.WithMessage(err, "write seq")
	}
	if err := b.WriteInt64(ph.fromId); err != nil {
		return pkgerrors.WithMessage(err, "write fromId")
	}
	if err := b.WriteUint32(ph.sid); err != nil {
		return pkgerrors.WithMessage(err, "write sid")
	}
	return nil
}

func (ph *rawPushPacketHead) decode(b *Buffer) (err error) {
	ph.seq, err = b.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read seq")
	}
	ph.fromId, err = b.ReadInt64()
	if err != nil {
		return pkgerrors.WithMessage(err, "read fromId")
	}
	ph.sid, err = b.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read sid")
	}
	return
}

func (ph *rawPushPacketHead) release() {
	poolOfRawPushPacketHead.Put(ph)
}

// s2sRpcPacketHead PacketTypeS2SRpc 包头.
type s2sRpcPacketHead struct {
	seq     uint32   // 序号.
	reqId   uint32   // 请求ID.
	fromId  ActorUID // 来源 Actor ID.
	toId    ActorUID // 目标 Actor ID.
	timeout uint32   // 超时.
}

var poolOfS2SRpcPacketHead = &sync.Pool{
	New: func() any {
		return &s2sRpcPacketHead{}
	},
}

func newS2SRpcHead(seq uint32, reqId uint32, fromId, toId ActorUID, timeout uint32) *s2sRpcPacketHead {
	h := poolOfS2SRpcPacketHead.Get().(*s2sRpcPacketHead)
	h.seq = seq
	h.reqId = reqId
	h.fromId = fromId
	h.toId = toId
	h.timeout = timeout
	return h
}

const lenS2SRpcPacketHead = 4 + 4 + 2*lenOfActorUID + 4

func (ph *s2sRpcPacketHead) PacketType() PacketType { return PacketTypeS2SRpc }

func (ph *s2sRpcPacketHead) getSeq() uint32 { return ph.seq }

func (ph *s2sRpcPacketHead) getLen() int {
	return lenS2SRpcPacketHead
}

func (ph *s2sRpcPacketHead) encode(b *Buffer) error {
	if err := b.WriteUint32(ph.seq); err != nil {
		return pkgerrors.WithMessage(err, "write seq")
	}
	if err := b.WriteUint32(ph.reqId); err != nil {
		return pkgerrors.WithMessage(err, "write reqId")
	}
	if err := b.WriteActorUID(ph.fromId); err != nil {
		return pkgerrors.WithMessage(err, "write fromId")
	}
	if err := b.WriteActorUID(ph.toId); err != nil {
		return pkgerrors.WithMessage(err, "write toId")
	}
	if err := b.WriteUint32(ph.timeout); err != nil {
		return pkgerrors.WithMessage(err, "write timeout")
	}
	return nil
}

func (ph *s2sRpcPacketHead) decode(b *Buffer) (err error) {
	ph.seq, err = b.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read seq")
	}
	ph.reqId, err = b.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read reqId")
	}
	ph.fromId, err = b.ReadActorUID()
	if err != nil {
		return pkgerrors.WithMessage(err, "read fromId")
	}
	ph.toId, err = b.ReadActorUID()
	if err != nil {
		return pkgerrors.WithMessage(err, "read toId")
	}
	ph.timeout, err = b.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read timeout")
	}
	return
}

func (ph *s2sRpcPacketHead) release() {
	poolOfS2SRpcPacketHead.Put(ph)
}

// s2sRpcRespPacketHead PacketTypeS2SRpcResp 包头.
type s2sRpcRespPacketHead struct {
	seq     uint32   // 序号.
	reqId   uint32   // 请求ID.
	fromId  ActorUID // 来自 Actor ID.
	toId    ActorUID // 目标 Actor ID.
	errCode ErrCode  // 错误码.
}

var poolOfS2SRpcRespPacketHead = &sync.Pool{
	New: func() any {
		return &s2sRpcRespPacketHead{}
	},
}

func newS2SRpcRespHead(seq uint32, reqId uint32, fromId, toId ActorUID, errCode ErrCode) *s2sRpcRespPacketHead {
	h := poolOfS2SRpcRespPacketHead.Get().(*s2sRpcRespPacketHead)
	h.seq = seq
	h.reqId = reqId
	h.fromId = fromId
	h.toId = toId
	h.errCode = errCode
	return h
}

func newS2SRpcRespHeadFromReq(seq uint32, errCode ErrCode, req *s2sRpcPacketHead) *s2sRpcRespPacketHead {
	h := poolOfS2SRpcRespPacketHead.Get().(*s2sRpcRespPacketHead)
	h.seq = seq
	h.reqId = req.reqId
	h.fromId = req.toId
	h.toId = req.fromId
	h.errCode = errCode
	return h
}

const lenS2SRpcRespPacketHead = 4 + 4 + lenOfActorUID*2 + sizeOfErrCode

func (ph *s2sRpcRespPacketHead) PacketType() PacketType { return PacketTypeS2SRpcResp }

func (ph *s2sRpcRespPacketHead) getSeq() uint32 { return ph.seq }

func (ph *s2sRpcRespPacketHead) getLen() int {
	return lenS2SRpcRespPacketHead
}

func (ph *s2sRpcRespPacketHead) encode(b *Buffer) error {
	if err := b.WriteUint32(ph.seq); err != nil {
		return pkgerrors.WithMessage(err, "write seq")
	}
	if err := b.WriteUint32(ph.reqId); err != nil {
		return pkgerrors.WithMessage(err, "write reqId")
	}
	if err := b.WriteActorUID(ph.fromId); err != nil {
		return pkgerrors.WithMessage(err, "write fromId")
	}
	if err := b.WriteActorUID(ph.toId); err != nil {
		return pkgerrors.WithMessage(err, "write toID")
	}
	if err := b.WriteErrCode(ph.errCode); err != nil {
		return pkgerrors.WithMessage(err, "write errCode")
	}
	return nil
}

func (ph *s2sRpcRespPacketHead) decode(b *Buffer) (err error) {
	ph.seq, err = b.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read seq")
	}
	ph.reqId, err = b.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read reqId")
	}
	ph.fromId, err = b.ReadActorUID()
	if err != nil {
		return pkgerrors.WithMessage(err, "read fromId")
	}
	ph.toId, err = b.ReadActorUID()
	if err != nil {
		return pkgerrors.WithMessage(err, "read toId")
	}
	ph.errCode, err = b.ReadErrCode()
	if err != nil {
		return pkgerrors.WithMessage(err, "read errCode")
	}
	return
}

func (ph *s2sRpcRespPacketHead) release() {
	poolOfS2SRpcRespPacketHead.Put(ph)
}

// s2sCastPacketHead PacketTypeS2SCast 包头.
type s2sCastPacketHead struct {
	seq    uint32   // 序号.
	fromId ActorUID // 来源 Actor ID.
	toId   ActorUID // 目标 Actor ID.
}

var poolOfS2SCastPacketHead = &sync.Pool{
	New: func() any {
		return &s2sCastPacketHead{}
	},
}

func newS2SCastHead(seq uint32, fromId, toId ActorUID) *s2sCastPacketHead {
	h := poolOfS2SCastPacketHead.Get().(*s2sCastPacketHead)
	h.seq = seq
	h.fromId = fromId
	h.toId = toId
	return h
}

const lenS2SCastPacketHead = 4 + lenOfActorUID*2

func (ph *s2sCastPacketHead) PacketType() PacketType { return PacketTypeS2SCast }

func (ph *s2sCastPacketHead) getSeq() uint32 { return ph.seq }

func (ph *s2sCastPacketHead) getLen() int {
	return lenS2SCastPacketHead
}

func (ph *s2sCastPacketHead) encode(b *Buffer) error {
	if err := b.WriteUint32(ph.seq); err != nil {
		return pkgerrors.WithMessage(err, "write seq")
	}
	if err := b.WriteActorUID(ph.fromId); err != nil {
		return pkgerrors.WithMessage(err, "write fromId")
	}
	if err := b.WriteActorUID(ph.toId); err != nil {
		return pkgerrors.WithMessage(err, "write toId")
	}
	return nil
}

func (ph *s2sCastPacketHead) decode(b *Buffer) (err error) {
	ph.seq, err = b.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read seq")
	}
	ph.fromId, err = b.ReadActorUID()
	if err != nil {
		return pkgerrors.WithMessage(err, "read fromId")
	}
	ph.toId, err = b.ReadActorUID()
	if err != nil {
		return pkgerrors.WithMessage(err, "read toId")
	}
	return
}

func (ph *s2sCastPacketHead) release() {
	poolOfS2SCastPacketHead.Put(ph)
}

// s2sForwardPacketHead PacketTypeS2SForward 包头.
type s2sForwardPacketHead struct {
	seq    uint32   // 序号.
	fromId ActorUID // 来源 Actor ID.
	toId   ActorUID // 目标 Actor ID.
}

var poolOfS2SForwardPacketHead = &sync.Pool{
	New: func() any {
		return &s2sForwardPacketHead{}
	},
}

func newS2SForwardHead(seq uint32, fromId, toId ActorUID) *s2sForwardPacketHead {
	h := poolOfS2SForwardPacketHead.Get().(*s2sForwardPacketHead)
	h.seq = seq
	h.fromId = fromId
	h.toId = toId
	return h
}

const lenS2SForwardPacketHead = 4 + 2*lenOfActorUID

func (ph *s2sForwardPacketHead) PacketType() PacketType { return PacketTypeS2SForward }

func (ph *s2sForwardPacketHead) getSeq() uint32 { return ph.seq }

func (ph *s2sForwardPacketHead) getLen() int {
	return lenS2SForwardPacketHead
}

func (ph *s2sForwardPacketHead) encode(b *Buffer) error {
	if err := b.WriteUint32(ph.seq); err != nil {
		return pkgerrors.WithMessage(err, "write seq")
	}
	if err := b.WriteActorUID(ph.fromId); err != nil {
		return pkgerrors.WithMessage(err, "write fromId")
	}
	if err := b.WriteActorUID(ph.toId); err != nil {
		return pkgerrors.WithMessage(err, "write toId")
	}
	return nil
}

func (ph *s2sForwardPacketHead) decode(b *Buffer) (err error) {
	ph.seq, err = b.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read seq")
	}
	ph.fromId, err = b.ReadActorUID()
	if err != nil {
		return pkgerrors.WithMessage(err, "read fromId")
	}
	ph.toId, err = b.ReadActorUID()
	if err != nil {
		return pkgerrors.WithMessage(err, "read toId")
	}
	return nil
}

func (ph *s2sForwardPacketHead) release() {
	poolOfS2SForwardPacketHead.Put(ph)
}

// s2sMigrateStartPacketHead PacketTypeS2SMigrateStart 包头.
type s2sMigrateStartPacketHead struct {
	seq           uint32
	uid           ActorUID
	sourceNodeID  string
	sourceLeaseID string
	isCActor      bool
	sessionNodeID string
	sessionSID    uint32
}

func newS2SMigrateStartHead(seq uint32, uid ActorUID, sourceNodeID, sourceLeaseID string, isCActor bool, session ActorSession) *s2sMigrateStartPacketHead {
	return &s2sMigrateStartPacketHead{
		seq:           seq,
		uid:           uid,
		sourceNodeID:  sourceNodeID,
		sourceLeaseID: sourceLeaseID,
		isCActor:      isCActor,
		sessionNodeID: session.NodeId,
		sessionSID:    session.SID,
	}
}

func (ph *s2sMigrateStartPacketHead) PacketType() PacketType { return PacketTypeS2SMigrateStart }

func (ph *s2sMigrateStartPacketHead) getSeq() uint32 { return ph.seq }

func (ph *s2sMigrateStartPacketHead) getLen() int {
	return 4 + lenOfActorUID + 2 + len(ph.sourceNodeID) + 2 + len(ph.sourceLeaseID) + 1 + 2 + len(ph.sessionNodeID) + 4
}

func (ph *s2sMigrateStartPacketHead) encode(b *Buffer) error {
	if err := b.WriteUint32(ph.seq); err != nil {
		return pkgerrors.WithMessage(err, "write seq")
	}
	if err := b.WriteActorUID(ph.uid); err != nil {
		return pkgerrors.WithMessage(err, "write uid")
	}
	if err := b.WriteUint16String(ph.sourceNodeID); err != nil {
		return pkgerrors.WithMessage(err, "write source node id")
	}
	if err := b.WriteUint16String(ph.sourceLeaseID); err != nil {
		return pkgerrors.WithMessage(err, "write source lease id")
	}
	if err := b.WriteInt8(boolToInt8(ph.isCActor)); err != nil {
		return pkgerrors.WithMessage(err, "write is cactor")
	}
	if err := b.WriteUint16String(ph.sessionNodeID); err != nil {
		return pkgerrors.WithMessage(err, "write session node id")
	}
	if err := b.WriteUint32(ph.sessionSID); err != nil {
		return pkgerrors.WithMessage(err, "write session sid")
	}
	return nil
}

func (ph *s2sMigrateStartPacketHead) decode(b *Buffer) (err error) {
	ph.seq, err = b.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read seq")
	}
	ph.uid, err = b.ReadActorUID()
	if err != nil {
		return pkgerrors.WithMessage(err, "read uid")
	}
	ph.sourceNodeID, err = b.ReadUint16String()
	if err != nil {
		return pkgerrors.WithMessage(err, "read source node id")
	}
	ph.sourceLeaseID, err = b.ReadUint16String()
	if err != nil {
		return pkgerrors.WithMessage(err, "read source lease id")
	}
	var c int8
	c, err = b.ReadInt8()
	if err != nil {
		return pkgerrors.WithMessage(err, "read is cactor")
	}
	ph.isCActor = c != 0
	ph.sessionNodeID, err = b.ReadUint16String()
	if err != nil {
		return pkgerrors.WithMessage(err, "read session node id")
	}
	ph.sessionSID, err = b.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read session sid")
	}
	return nil
}

func (ph *s2sMigrateStartPacketHead) release() {}

// s2sMigrateResultPacketHead PacketTypeS2SMigrateResult 包头.
type s2sMigrateResultPacketHead struct {
	seq           uint32
	uid           ActorUID
	sourceLeaseID string
	startSeq      uint32
	errCode       ErrCode
}

func newS2SMigrateResultHead(seq uint32, uid ActorUID, sourceLeaseID string, startSeq uint32, errCode ErrCode) *s2sMigrateResultPacketHead {
	return &s2sMigrateResultPacketHead{
		seq:           seq,
		uid:           uid,
		sourceLeaseID: sourceLeaseID,
		startSeq:      startSeq,
		errCode:       errCode,
	}
}

func (ph *s2sMigrateResultPacketHead) PacketType() PacketType { return PacketTypeS2SMigrateResult }

func (ph *s2sMigrateResultPacketHead) getSeq() uint32 { return ph.seq }

func (ph *s2sMigrateResultPacketHead) getLen() int {
	return 4 + lenOfActorUID + 2 + len(ph.sourceLeaseID) + 4 + sizeOfErrCode
}

func (ph *s2sMigrateResultPacketHead) encode(b *Buffer) error {
	if err := b.WriteUint32(ph.seq); err != nil {
		return pkgerrors.WithMessage(err, "write seq")
	}
	if err := b.WriteActorUID(ph.uid); err != nil {
		return pkgerrors.WithMessage(err, "write uid")
	}
	if err := b.WriteUint16String(ph.sourceLeaseID); err != nil {
		return pkgerrors.WithMessage(err, "write source lease id")
	}
	if err := b.WriteUint32(ph.startSeq); err != nil {
		return pkgerrors.WithMessage(err, "write start seq")
	}
	if err := b.WriteErrCode(ph.errCode); err != nil {
		return pkgerrors.WithMessage(err, "write err code")
	}
	return nil
}

func (ph *s2sMigrateResultPacketHead) decode(b *Buffer) (err error) {
	ph.seq, err = b.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read seq")
	}
	ph.uid, err = b.ReadActorUID()
	if err != nil {
		return pkgerrors.WithMessage(err, "read uid")
	}
	ph.sourceLeaseID, err = b.ReadUint16String()
	if err != nil {
		return pkgerrors.WithMessage(err, "read source lease id")
	}
	ph.startSeq, err = b.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read start seq")
	}
	ph.errCode, err = b.ReadErrCode()
	if err != nil {
		return pkgerrors.WithMessage(err, "read err code")
	}
	return nil
}

func (ph *s2sMigrateResultPacketHead) release() {}

// s2sMigrateForwardPacketHead PacketTypeS2SMigrateForward 包头.
type s2sMigrateForwardPacketHead struct {
	seq             uint32
	uid             ActorUID
	sourceLeaseID   string
	originNodeID    string
	innerPacketType PacketType
}

func newS2SMigrateForwardHead(seq uint32, uid ActorUID, sourceLeaseID, originNodeID string, innerPacketType PacketType) *s2sMigrateForwardPacketHead {
	return &s2sMigrateForwardPacketHead{
		seq:             seq,
		uid:             uid,
		sourceLeaseID:   sourceLeaseID,
		originNodeID:    originNodeID,
		innerPacketType: innerPacketType,
	}
}

func (ph *s2sMigrateForwardPacketHead) PacketType() PacketType { return PacketTypeS2SMigrateForward }

func (ph *s2sMigrateForwardPacketHead) getSeq() uint32 { return ph.seq }

func (ph *s2sMigrateForwardPacketHead) getLen() int {
	return 4 + lenOfActorUID + 2 + len(ph.sourceLeaseID) + 2 + len(ph.originNodeID) + sizeOfPacketType
}

func (ph *s2sMigrateForwardPacketHead) encode(b *Buffer) error {
	if err := b.WriteUint32(ph.seq); err != nil {
		return pkgerrors.WithMessage(err, "write seq")
	}
	if err := b.WriteActorUID(ph.uid); err != nil {
		return pkgerrors.WithMessage(err, "write uid")
	}
	if err := b.WriteUint16String(ph.sourceLeaseID); err != nil {
		return pkgerrors.WithMessage(err, "write source lease id")
	}
	if err := b.WriteUint16String(ph.originNodeID); err != nil {
		return pkgerrors.WithMessage(err, "write origin node id")
	}
	if err := b.WritePacketType(ph.innerPacketType); err != nil {
		return pkgerrors.WithMessage(err, "write inner packet type")
	}
	return nil
}

func (ph *s2sMigrateForwardPacketHead) decode(b *Buffer) (err error) {
	ph.seq, err = b.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read seq")
	}
	ph.uid, err = b.ReadActorUID()
	if err != nil {
		return pkgerrors.WithMessage(err, "read uid")
	}
	ph.sourceLeaseID, err = b.ReadUint16String()
	if err != nil {
		return pkgerrors.WithMessage(err, "read source lease id")
	}
	ph.originNodeID, err = b.ReadUint16String()
	if err != nil {
		return pkgerrors.WithMessage(err, "read origin node id")
	}
	ph.innerPacketType, err = b.ReadPacketType()
	if err != nil {
		return pkgerrors.WithMessage(err, "read inner packet type")
	}
	return nil
}

func (ph *s2sMigrateForwardPacketHead) release() {}

func boolToInt8(v bool) int8 {
	if v {
		return 1
	}
	return 0
}
