package gactor

import (
	"unsafe"

	pkgerrors "github.com/pkg/errors"
)

// PacketType 数据包类型.
type PacketType = int8

const (
	PacketTypeUnknown = PacketType(0)
	PacketTypeAck     = PacketType(1) // Ack.

	PacketTypeRawReq  = PacketType(11) // C2S 请求.
	PacketTypeRawResp = PacketType(12) // S2C 响应.
	PacketTypeRawPush = PacketType(13) // S2C 推送.

	PacketTypeS2SRpc          = PacketType(21) // S2S RPC调用.
	PacketTypeS2SRpcResp      = PacketType(22) // S2S RPC调用响应.
	PacketTypeS2SCast         = PacketType(23) // S2S 消息投递.
	PacketTypeS2SDisconnected = PacketType(24) // S2S 连接断开.
)

const sizeOfPacketType = int(unsafe.Sizeof(PacketType(0)))

// PacketAllocator 数据包分配器.
type PacketAllocator interface {
	// PacketType 返回分配的数据包类型.
	PacketType() PacketType

	// AllocBuf 分配足够容纳长度为 payloadLen 的负载数据的缓冲区.
	// 返回的缓冲区中已经填充好包头信息. 只需往里写入负载数据.
	AllocBuf(b *Buffer, payloadLen int) error
}

// PacketCodec 数据包编解码器.
type PacketCodec interface {
	BytesManager

	// Encode 编码数据包.
	// allocator 提供了获取数据包类型和分配数据包切片的功能.
	// 根据数据包类型编码 payload, 然后调用 allocator 分配
	// 数据包切片, 将编码后的 payload 数据写入数据包切片中.
	// 数据包类型包括:
	// 	PacketTypeRawResp, PacketTypeRawPush
	//	PacketTypeS2SRpc, PacketTypeS2SRpcResp, PacketTypeS2SCast
	Encode(allocator PacketAllocator, payload any) ([]byte, error)

	// EncodePayload 编码负载数据.
	// 根据数据包类型 pt 编码 payload 并生成数据切片返回.
	// 数据包类型包括:
	//	PacketTypeS2SRpc, PacketTypeS2SCast
	EncodePayload(pt PacketType, payload any) ([]byte, error)

	// DecodePayload 解码负载数据.
	// 根据数据包类型 pt 解码 b 中负载数据并填充入 v 指向的对象中.
	// 数据包类型包括:
	//	PacketTypeRawReq
	//	PacketTypeS2SRpc, PacketTypeS2SRpcResp, PacketTypeS2SCast
	//
	// 返回 ErrBytesEscape, 表示 b 中的数据切片被外部劫持, 系统
	// 内部将不再自动回收数据切片.
	DecodePayload(pt PacketType, b *Buffer, v any) error
}

// packetHead 定义数据包包头接口.
type packetHead interface {
	// 数据包类型.
	pt() PacketType

	// 数据包序列号.
	seq() uint32

	// 数据包大小.
	size() int

	// 编码数据包.
	encode(b *Buffer) error

	// 解码数据包.
	decode(b *Buffer) error
}

// ackPacketHead PacketTypeAck 包头.
type ackPacketHead struct {
	ackPt  PacketType // 确认的数据包类型.
	ackSeq uint32     // 确认的数据包序号.
}

const sizeOfAckPacketHead = sizeOfPacketType + 4

func (ph *ackPacketHead) pt() PacketType {
	return PacketTypeAck
}

func (ph *ackPacketHead) seq() uint32 {
	return ph.ackSeq
}

func (ph *ackPacketHead) size() int {
	return sizeOfAckPacketHead
}

func (ph *ackPacketHead) encode(b *Buffer) error {
	if err := b.writePacketType(ph.ackPt); err != nil {
		return pkgerrors.WithMessage(err, "write ackPt")
	}
	if err := b.WriteUint32(ph.ackSeq); err != nil {
		return pkgerrors.WithMessage(err, "write ackSeq")
	}
	return nil
}

func (ph *ackPacketHead) decode(b *Buffer) (err error) {
	ph.ackPt, err = b.readPacketType()
	if err != nil {
		return pkgerrors.WithMessage(err, "read ackPt")
	}
	ph.ackSeq, err = b.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read ackSeq")
	}
	return
}

// rawReqPacketHead PacketTypeRawReq 包头.
type rawReqPacketHead struct {
	seq_    uint32   // 序号.
	toId    ActorUID // 目标 Actor ID.
	sid     uint32   // 会话ID.
	timeout uint32   // 超时.
}

const sizeOfRawReqPacketHead = 4 + sizeOfActorUID + 4 + 4

func (ph *rawReqPacketHead) pt() PacketType { return PacketTypeRawReq }

func (ph *rawReqPacketHead) seq() uint32 { return ph.seq_ }

func (ph *rawReqPacketHead) size() int {
	return sizeOfRawReqPacketHead
}

func (ph *rawReqPacketHead) encode(b *Buffer) error {
	if err := b.WriteUint32(ph.seq_); err != nil {
		return pkgerrors.WithMessage(err, "write seq")
	}
	if err := b.writeActorUID(ph.toId); err != nil {
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
	ph.seq_, err = b.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read seq")
	}
	ph.toId, err = b.readActorUID()
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

// rawRespPacketHead PacketTypeRawResp 包头.
type rawRespPacketHead struct {
	seq_    uint32   // 序号.
	fromId  ActorUID // 来自 Actor ID.
	sid     uint32   // 会话ID.
	errCode errCode  // 错误码.
}

const sizeOfRawRespPacketHead = 4 + sizeOfActorUID + 4 + sizeOfErrCode

func (ph *rawRespPacketHead) pt() PacketType { return PacketTypeRawResp }

func (ph *rawRespPacketHead) seq() uint32 { return ph.seq_ }

func (ph *rawRespPacketHead) size() int {
	return sizeOfRawRespPacketHead
}

func (ph *rawRespPacketHead) encode(b *Buffer) error {
	if err := b.WriteUint32(ph.seq_); err != nil {
		return pkgerrors.WithMessage(err, "write seq")
	}
	if err := b.writeActorUID(ph.fromId); err != nil {
		return pkgerrors.WithMessage(err, "write toId")
	}
	if err := b.WriteUint32(ph.sid); err != nil {
		return pkgerrors.WithMessage(err, "write sid")
	}
	if err := b.writeErrCode(ph.errCode); err != nil {
		return pkgerrors.WithMessage(err, "write errCode")
	}
	return nil
}

func (ph *rawRespPacketHead) decode(b *Buffer) (err error) {
	ph.seq_, err = b.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read seq")
	}
	ph.fromId, err = b.readActorUID()
	if err != nil {
		return pkgerrors.WithMessage(err, "read toId")
	}
	ph.sid, err = b.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read sid")
	}
	ph.errCode, err = b.readErrCode()
	if err != nil {
		return pkgerrors.WithMessage(err, "read errCode")
	}
	return
}

// rawPushPacketHead PacketTypeRawPush 包头.
type rawPushPacketHead struct {
	seq_   uint32   // 序号.
	fromId ActorUID // 来自 Actor ID.
	sid    uint32   // 会话ID.
}

const sizeOfRawPushPacketHead = 4 + sizeOfActorUID + 4

func (ph *rawPushPacketHead) pt() PacketType { return PacketTypeRawPush }

func (ph *rawPushPacketHead) seq() uint32 { return ph.seq_ }

func (ph *rawPushPacketHead) size() int {
	return sizeOfRawPushPacketHead
}

func (ph *rawPushPacketHead) encode(b *Buffer) error {
	if err := b.WriteUint32(ph.seq_); err != nil {
		return pkgerrors.WithMessage(err, "write seq")
	}
	if err := b.writeActorUID(ph.fromId); err != nil {
		return pkgerrors.WithMessage(err, "write toId")
	}
	if err := b.WriteUint32(ph.sid); err != nil {
		return pkgerrors.WithMessage(err, "write sid")
	}
	return nil
}

func (ph *rawPushPacketHead) decode(b *Buffer) (err error) {
	ph.seq_, err = b.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read seq")
	}
	ph.fromId, err = b.readActorUID()
	if err != nil {
		return pkgerrors.WithMessage(err, "read toId")
	}
	ph.sid, err = b.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read sid")
	}
	return
}

// s2sRpcPacketHead PacketTypeS2SRpc 包头.
type s2sRpcPacketHead struct {
	seq_    uint32   // 序号.
	reqId   uint32   // 请求ID.
	toId    ActorUID // 目标 Actor ID.
	timeout uint32   // 超时.
}

const sizeOfS2SRpcPacketHead = 4 + sizeOfActorUID + 4

func (ph *s2sRpcPacketHead) pt() PacketType { return PacketTypeS2SRpc }

func (ph *s2sRpcPacketHead) seq() uint32 { return ph.seq_ }

func (ph *s2sRpcPacketHead) size() int {
	return sizeOfS2SRpcPacketHead
}

func (ph *s2sRpcPacketHead) encode(b *Buffer) error {
	if err := b.WriteUint32(ph.seq_); err != nil {
		return pkgerrors.WithMessage(err, "write seq")
	}
	if err := b.WriteUint32(ph.reqId); err != nil {
		return pkgerrors.WithMessage(err, "write reqId")
	}
	if err := b.writeActorUID(ph.toId); err != nil {
		return pkgerrors.WithMessage(err, "write toId")
	}
	if err := b.WriteUint32(ph.timeout); err != nil {
		return pkgerrors.WithMessage(err, "write timeout")
	}
	return nil
}

func (ph *s2sRpcPacketHead) decode(b *Buffer) (err error) {
	ph.seq_, err = b.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read seq")
	}
	ph.reqId, err = b.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read reqId")
	}
	ph.toId, err = b.readActorUID()
	if err != nil {
		return pkgerrors.WithMessage(err, "read toId")
	}
	ph.timeout, err = b.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read timeout")
	}
	return
}

// s2sRpcRespPacketHead PacketTypeS2SRpcResp 包头.
type s2sRpcRespPacketHead struct {
	seq_    uint32   // 序号.
	reqId   uint32   // 请求ID.
	fromId  ActorUID // 来自 Actor ID.
	errCode errCode  // 错误码.
}

const sizeOfS2SRpcRespPacketHead = 4 + sizeOfActorUID + sizeOfErrCode

func (ph *s2sRpcRespPacketHead) pt() PacketType { return PacketTypeS2SRpcResp }

func (ph *s2sRpcRespPacketHead) seq() uint32 { return ph.seq_ }

func (ph *s2sRpcRespPacketHead) size() int {
	return sizeOfS2SRpcRespPacketHead
}

func (ph *s2sRpcRespPacketHead) encode(b *Buffer) error {
	if err := b.WriteUint32(ph.seq_); err != nil {
		return pkgerrors.WithMessage(err, "write seq")
	}
	if err := b.WriteUint32(ph.reqId); err != nil {
		return pkgerrors.WithMessage(err, "write reqId")
	}
	if err := b.writeActorUID(ph.fromId); err != nil {
		return pkgerrors.WithMessage(err, "write fromId")
	}
	if err := b.writeErrCode(ph.errCode); err != nil {
		return pkgerrors.WithMessage(err, "write errCode")
	}
	return nil
}

func (ph *s2sRpcRespPacketHead) decode(b *Buffer) (err error) {
	ph.seq_, err = b.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read seq")
	}
	ph.reqId, err = b.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read reqId")
	}
	ph.fromId, err = b.readActorUID()
	if err != nil {
		return pkgerrors.WithMessage(err, "read fromId")
	}
	ph.errCode, err = b.readErrCode()
	if err != nil {
		return pkgerrors.WithMessage(err, "read errCode")
	}
	return
}

// s2sCastPacketHead PacketTypeS2SCast 包头.
type s2sCastPacketHead struct {
	seq_ uint32   // 序号.
	toId ActorUID // 目标 Actor ID.
}

const sizeOfS2SCastPacketHead = 4 + sizeOfActorUID

func (ph *s2sCastPacketHead) pt() PacketType { return PacketTypeS2SCast }

func (ph *s2sCastPacketHead) seq() uint32 { return ph.seq_ }

func (ph *s2sCastPacketHead) size() int {
	return sizeOfS2SCastPacketHead
}

func (ph *s2sCastPacketHead) encode(b *Buffer) error {
	if err := b.WriteUint32(ph.seq_); err != nil {
		return pkgerrors.WithMessage(err, "write seq")
	}
	if err := b.writeActorUID(ph.toId); err != nil {
		return pkgerrors.WithMessage(err, "write toId")
	}
	return nil
}

func (ph *s2sCastPacketHead) decode(b *Buffer) (err error) {
	ph.seq_, err = b.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read seq")
	}
	ph.toId, err = b.readActorUID()
	if err != nil {
		return pkgerrors.WithMessage(err, "read toId")
	}
	return
}

// s2sDisconnectedPacketHead PacketTypeS2SDisconnected 包头.
type s2sDisconnectedPacketHead struct {
	seq_ uint32   // 序号.
	uid  ActorUID // Actor ID.
	sid  uint32   // 会话ID.
}

const sizeOfS2SDisconnectedPacketHead = 4 + sizeOfActorUID + 4

func (ph *s2sDisconnectedPacketHead) pt() PacketType { return PacketTypeS2SDisconnected }

func (ph *s2sDisconnectedPacketHead) seq() uint32 { return ph.seq_ }

func (ph *s2sDisconnectedPacketHead) size() int {
	return sizeOfS2SDisconnectedPacketHead
}

func (ph *s2sDisconnectedPacketHead) encode(b *Buffer) error {
	if err := b.WriteUint32(ph.seq_); err != nil {
		return pkgerrors.WithMessage(err, "write seq")
	}
	if err := b.writeActorUID(ph.uid); err != nil {
		return pkgerrors.WithMessage(err, "write toId")
	}
	if err := b.WriteUint32(ph.sid); err != nil {
		return pkgerrors.WithMessage(err, "write sid")
	}
	return nil
}

func (ph *s2sDisconnectedPacketHead) decode(b *Buffer) (err error) {
	ph.seq_, err = b.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read seq")
	}
	ph.uid, err = b.readActorUID()
	if err != nil {
		return pkgerrors.WithMessage(err, "read toId")
	}
	ph.sid, err = b.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read sid")
	}
	return
}

// packetAllocator 数据包分配器.
type packetAllocator struct {
	BytesManager
	ph packetHead
}

func (pa *packetAllocator) PacketType() PacketType {
	return pa.ph.pt()
}

func (pa *packetAllocator) AllocBuf(buf *Buffer, payloadLen int) error {
	// 分配字节切片.
	size := sizeOfPacketType + pa.ph.size() + payloadLen
	b := pa.BytesManager.GetBytes(size)
	buf.SetBuf(b)

	// 编码包头.
	if err := buf.writePacketType(pa.PacketType()); err != nil {
		return err
	}
	if err := pa.ph.encode(buf); err != nil {
		return err
	}
	return nil
}

// encodePacket 编码数据包.
func encodePacket(ph packetHead, payload any, codec PacketCodec) ([]byte, error) {
	pa := packetAllocator{
		BytesManager: codec,
		ph:           ph,
	}
	if payload == nil {
		var buf Buffer
		pa.AllocBuf(&buf, 0)
		return buf.Data(), nil
	}
	return codec.Encode(&pa, payload)
}
