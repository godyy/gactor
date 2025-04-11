package gactor

import (
	"unsafe"

	pkgerrors "github.com/pkg/errors"
)

// PacketType 数据包类型.
type PacketType = int8

const (
	PacketTypeUnknown = PacketType(0)

	PacketTypeRawReq  = PacketType(11) // C2S 请求.
	PacketTypeRawResp = PacketType(12) // S2C 响应.
	PacketTypeRawPush = PacketType(13) // S2C 推送.

	PacketTypeS2SRpc          = PacketType(21) // S2S RPC调用.
	PacketTypeS2SRpcResp      = PacketType(22) // S2S RPC调用响应.
	PacketTypeS2SCast         = PacketType(23) // S2S 消息投递.
	PacketTypeS2SDisconnected = PacketType(24) // S2S 连接断开.
)

const sizeOfPacketType = int(unsafe.Sizeof(PacketType(0)))

// Packet 数据包接口.
type Packet interface {
	WriteBool(bool) error
	WriteByte(byte) error
	WriteInt8(int8) error
	WriteInt16(int16) error
	WriteInt32(int32) error
	WriteInt64(int64) error
	WriteUint8(uint8) error
	WriteUint16(uint16) error
	WriteUint32(uint32) error
	WriteUint64(uint64) error
	WriteString(string) error
	Write([]byte) (int, error)
	ReadBool() (bool, error)
	ReadByte() (byte, error)
	ReadInt8() (int8, error)
	ReadInt16() (int16, error)
	ReadInt32() (int32, error)
	ReadInt64() (int64, error)
	ReadUint8() (uint8, error)
	ReadUint16() (uint16, error)
	ReadUint32() (uint32, error)
	ReadUint64() (uint64, error)
	ReadString() (string, error)
	Read([]byte) (int, error)
	UnreadData() []byte
}

// PacketManager 数据包管理器.
type PacketManager interface {
	// GetPacket 获取容量为 size 的数据包.
	GetPacket(size int) Packet

	// PutPacket 回收数据包.
	PutPacket(p Packet)
}

// PacketAllocator 数据包分配器.
type PacketAllocator interface {
	// PacketType 返回分配的数据包类型.
	PacketType() PacketType

	// Allocate 分配足够容纳长度为 payloadLen 的负载数据的数据包.
	// 返回的数据包已经填充好其它必要信息. 只需往里写入负载数据.
	Allocate(payloadLen int) (Packet, error)
}

// PacketCodec 数据包编解码器.
type PacketCodec interface {
	PacketManager

	// Encode 编码数据包.
	// allocator 提供了获取数据包类型和分配数据包实体的功能.
	// 根据数据包类型编码 payload, 然后调用 allocator 分配
	// 数据包实体, 将编码后的 payload 数据写入数据包中.
	// 数据包类型包括:
	// 	PacketTypeRawResp, PacketTypeRawPush
	//	PacketTypeS2SRpc, PacketTypeS2SRpcResp, PacketTypeS2SCast
	Encode(allocator PacketAllocator, payload any) (Packet, error)

	// EncodePayload 编码负载数据.
	// 根据数据包类型 pt 编码 payload 并生成数据包返回.
	// 数据包类型包括:
	//	PacketTypeS2SRpc, PacketTypeS2SCast
	EncodePayload(pt PacketType, payload any) (Packet, error)

	// DecodePayload 解码负载数据.
	// 根据数据包类型 pt 解码 p 中负载数据并生成负载数据对象.
	// 数据包类型包括:
	//	PacketTypeRawReq
	//	PacketTypeS2SRpc, PacketTypeS2SRpcResp, PacketTypeS2SCast
	//
	// 返回 ErrPacketEscape, 系统内部将不再自动回收数据包 p.
	DecodePayload(pt PacketType, p Packet, v any) error
}

// PacketEncoder 数据包编码器.
// 使用 allocator 分配 Packet, 然后将编码后的 payload 填充进去.
type PacketEncoder func(allocator PacketAllocator, payload any) (Packet, error)

// packetHead 定义数据包包头接口.
type packetHead interface {
	pt() PacketType
	size() int
	encode(Packet) error
	decode(Packet) error
}

// rawReqPacketHead PacketTypeRawReq 包头.
type rawReqPacketHead struct {
	toId    ActorUID
	sid     uint32
	timeout uint32
}

const sizeOfRawReqPacketHead = sizeOfActorUID + 4 + 4

func (ph *rawReqPacketHead) pt() PacketType { return PacketTypeRawReq }

func (ph *rawReqPacketHead) size() int {
	return sizeOfRawReqPacketHead
}

func (ph *rawReqPacketHead) encode(p Packet) error {
	if err := packetIOHelper.writeActorUID(p, ph.toId); err != nil {
		return pkgerrors.WithMessage(err, "write toId")
	}
	if err := p.WriteUint32(ph.sid); err != nil {
		return pkgerrors.WithMessage(err, "write sid")
	}
	if err := p.WriteUint32(ph.timeout); err != nil {
		return pkgerrors.WithMessage(err, "write timeout")
	}
	return nil
}

func (ph *rawReqPacketHead) decode(p Packet) (err error) {
	ph.toId, err = packetIOHelper.readActorUID(p)
	if err != nil {
		return pkgerrors.WithMessage(err, "read toId")
	}
	ph.sid, err = p.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read sid")
	}
	ph.timeout, err = p.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read timeout")
	}
	return
}

// rawRespPacketHead PacketTypeRawResp 包头.
type rawRespPacketHead struct {
	fromId  ActorUID
	sid     uint32
	errCode errCode
}

const sizeOfRawRespPacketHead = sizeOfActorUID + 4 + sizeOfErrCode

func (ph *rawRespPacketHead) pt() PacketType { return PacketTypeRawResp }

func (ph *rawRespPacketHead) size() int {
	return sizeOfRawRespPacketHead
}

func (ph *rawRespPacketHead) encode(p Packet) error {
	if err := packetIOHelper.writeActorUID(p, ph.fromId); err != nil {
		return pkgerrors.WithMessage(err, "write toId")
	}
	if err := p.WriteUint32(ph.sid); err != nil {
		return pkgerrors.WithMessage(err, "write sid")
	}
	if err := packetIOHelper.writeErrCode(p, ph.errCode); err != nil {
		return pkgerrors.WithMessage(err, "write errCode")
	}
	return nil
}

func (ph *rawRespPacketHead) decode(p Packet) (err error) {
	ph.fromId, err = packetIOHelper.readActorUID(p)
	if err != nil {
		return pkgerrors.WithMessage(err, "read toId")
	}
	ph.sid, err = p.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read sid")
	}
	ph.errCode, err = packetIOHelper.readErrCode(p)
	if err != nil {
		return pkgerrors.WithMessage(err, "read errCode")
	}
	return
}

// rawPushPacketHead PacketTypeRawPush 包头.
type rawPushPacketHead struct {
	fromId ActorUID
	sid    uint32
}

const sizeOfRawPushPacketHead = sizeOfActorUID + 4

func (ph *rawPushPacketHead) pt() PacketType { return PacketTypeRawPush }

func (ph *rawPushPacketHead) size() int {
	return sizeOfRawPushPacketHead
}

func (ph *rawPushPacketHead) encode(p Packet) error {
	if err := packetIOHelper.writeActorUID(p, ph.fromId); err != nil {
		return pkgerrors.WithMessage(err, "write toId")
	}
	if err := p.WriteUint32(ph.sid); err != nil {
		return pkgerrors.WithMessage(err, "write sid")
	}
	return nil
}

func (ph *rawPushPacketHead) decode(p Packet) (err error) {
	ph.fromId, err = packetIOHelper.readActorUID(p)
	if err != nil {
		return pkgerrors.WithMessage(err, "read toId")
	}
	ph.sid, err = p.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read sid")
	}
	return
}

// s2sRpcPacketHead PacketTypeS2SRpc 包头.
type s2sRpcPacketHead struct {
	reqId   uint64
	toId    ActorUID
	timeout uint32
}

const sizeOfS2SRpcPacketHead = 8 + sizeOfActorUID + 4

func (ph *s2sRpcPacketHead) pt() PacketType { return PacketTypeS2SRpc }

func (ph *s2sRpcPacketHead) size() int {
	return sizeOfS2SRpcPacketHead
}

func (ph *s2sRpcPacketHead) encode(p Packet) error {
	if err := p.WriteUint64(ph.reqId); err != nil {
		return pkgerrors.WithMessage(err, "write reqId")
	}
	if err := packetIOHelper.writeActorUID(p, ph.toId); err != nil {
		return pkgerrors.WithMessage(err, "write toId")
	}
	if err := p.WriteUint32(ph.timeout); err != nil {
		return pkgerrors.WithMessage(err, "write timeout")
	}
	return nil
}

func (ph *s2sRpcPacketHead) decode(p Packet) (err error) {
	ph.reqId, err = p.ReadUint64()
	if err != nil {
		return pkgerrors.WithMessage(err, "read reqId")
	}
	ph.toId, err = packetIOHelper.readActorUID(p)
	if err != nil {
		return pkgerrors.WithMessage(err, "read toId")
	}
	ph.timeout, err = p.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read timeout")
	}
	return
}

// s2sRpcRespPacketHead PacketTypeS2SRpcResp 包头.
type s2sRpcRespPacketHead struct {
	reqId   uint64
	fromId  ActorUID
	errCode errCode
}

const sizeOfS2SRpcRespPacketHead = 8 + sizeOfActorUID + sizeOfErrCode

func (ph *s2sRpcRespPacketHead) pt() PacketType { return PacketTypeS2SRpcResp }

func (ph *s2sRpcRespPacketHead) size() int {
	return sizeOfS2SRpcRespPacketHead
}

func (ph *s2sRpcRespPacketHead) encode(p Packet) error {
	if err := p.WriteUint64(ph.reqId); err != nil {
		return pkgerrors.WithMessage(err, "write reqId")
	}
	if err := packetIOHelper.writeActorUID(p, ph.fromId); err != nil {
		return pkgerrors.WithMessage(err, "write fromId")
	}
	if err := packetIOHelper.writeErrCode(p, ph.errCode); err != nil {
		return pkgerrors.WithMessage(err, "write errCode")
	}
	return nil
}

func (ph *s2sRpcRespPacketHead) decode(p Packet) (err error) {
	ph.reqId, err = p.ReadUint64()
	if err != nil {
		return pkgerrors.WithMessage(err, "read reqId")
	}
	ph.fromId, err = packetIOHelper.readActorUID(p)
	if err != nil {
		return pkgerrors.WithMessage(err, "read fromId")
	}
	ph.errCode, err = packetIOHelper.readErrCode(p)
	if err != nil {
		return pkgerrors.WithMessage(err, "read errCode")
	}
	return
}

// s2sCastPacketHead PacketTypeS2SCast 包头.
type s2sCastPacketHead struct {
	toId ActorUID
}

const sizeOfS2SCastPacketHead = sizeOfActorUID

func (ph *s2sCastPacketHead) pt() PacketType { return PacketTypeS2SCast }

func (ph *s2sCastPacketHead) size() int {
	return sizeOfS2SCastPacketHead
}

func (ph *s2sCastPacketHead) encode(p Packet) error {
	if err := packetIOHelper.writeActorUID(p, ph.toId); err != nil {
		return pkgerrors.WithMessage(err, "write toId")
	}
	return nil
}

func (ph *s2sCastPacketHead) decode(p Packet) (err error) {
	ph.toId, err = packetIOHelper.readActorUID(p)
	if err != nil {
		return pkgerrors.WithMessage(err, "read toId")
	}
	return
}

// s2sDisconnectedPacketHead PacketTypeS2SDisconnected 包头.
type s2sDisconnectedPacketHead struct {
	uid ActorUID
	sid uint32
}

const sizeOfS2SDisconnectedPacketHead = sizeOfActorUID + 4

func (ph *s2sDisconnectedPacketHead) pt() PacketType { return PacketTypeS2SDisconnected }

func (ph *s2sDisconnectedPacketHead) size() int {
	return sizeOfS2SDisconnectedPacketHead
}

func (ph *s2sDisconnectedPacketHead) encode(p Packet) error {
	if err := packetIOHelper.writeActorUID(p, ph.uid); err != nil {
		return pkgerrors.WithMessage(err, "write toId")
	}
	if err := p.WriteUint32(ph.sid); err != nil {
		return pkgerrors.WithMessage(err, "write sid")
	}
	return nil
}

func (ph *s2sDisconnectedPacketHead) decode(p Packet) (err error) {
	ph.uid, err = packetIOHelper.readActorUID(p)
	if err != nil {
		return pkgerrors.WithMessage(err, "read toId")
	}
	ph.sid, err = p.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read sid")
	}
	return
}

// packetAllocator 数据包分配器.
type packetAllocator struct {
	PacketManager
	ph packetHead
}

func (pa *packetAllocator) PacketType() PacketType {
	return pa.ph.pt()
}

func (pa *packetAllocator) Allocate(payloadLen int) (Packet, error) {
	size := sizeOfPacketType + pa.ph.size() + payloadLen
	p := pa.PacketManager.GetPacket(size)
	if err := packetIOHelper.writePacketType(p, pa.PacketType()); err != nil {
		return nil, err
	}
	if err := pa.ph.encode(p); err != nil {
		return nil, err
	}
	return p, nil
}

// encodePacket 编码数据包.
func encodePacket(ph packetHead, payload any, codec PacketCodec) (Packet, error) {
	pa := packetAllocator{
		PacketManager: codec,
		ph:            ph,
	}
	if payload == nil {
		return pa.Allocate(0)
	}
	return codec.Encode(&pa, payload)
}

// packetIOHelper_ Packet IO 帮助函数集.
type packetIOHelper_ struct{}

var packetIOHelper = packetIOHelper_{}

func (packetIOHelper_) readPacketType(p Packet) (pt PacketType, err error) {
	pt, err = p.ReadInt8()
	if err != nil {
		return PacketTypeUnknown, pkgerrors.WithMessage(err, "read packet type")
	}
	return
}

func (packetIOHelper_) writePacketType(p Packet, pt PacketType) (err error) {
	if err = p.WriteInt8(int8(pt)); err != nil {
		return pkgerrors.WithMessage(err, "write packet type")
	}
	return
}

func (packetIOHelper_) readErrCode(p Packet) (errCode, error) {
	ec, err := p.ReadUint16()
	if err != nil {
		return errCodeOK, pkgerrors.WithMessage(err, "read err code")
	}
	return errCode(ec), nil
}

func (packetIOHelper_) writeErrCode(p Packet, ec errCode) (err error) {
	if err = p.WriteUint16(uint16(ec)); err != nil {
		return pkgerrors.WithMessage(err, "write err code")
	}
	return
}

func (packetIOHelper_) readActorUID(p Packet) (uid ActorUID, err error) {
	uid.Category, err = p.ReadUint16()
	if err != nil {
		return ActorUID{}, pkgerrors.WithMessage(err, "read actor category")
	}
	uid.ID, err = p.ReadInt64()
	if err != nil {
		return ActorUID{}, pkgerrors.WithMessage(err, "read actor id")
	}
	return
}

func (packetIOHelper_) writeActorUID(p Packet, uid ActorUID) (err error) {
	if err = p.WriteUint16(uid.Category); err != nil {
		return pkgerrors.WithMessage(err, "write actor category")
	}
	if err = p.WriteInt64(uid.ID); err != nil {
		return pkgerrors.WithMessage(err, "write actor id")
	}
	return
}
