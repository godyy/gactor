package gactor

import (
	"github.com/godyy/gnet"
	pkgerrors "github.com/pkg/errors"
)

// BytesManager 字节切片管理器.
type BytesManager interface {
	// GetBytes 获取指定容量的字节切片.
	GetBytes(cap int) []byte

	// PutBytes 回收字节切片.
	PutBytes(b []byte)
}

// Buffer 缓冲区.
type Buffer struct {
	gnet.Buffer
}

// readPacketType 读取数据包类型.
func (b *Buffer) readPacketType() (pt PacketType, err error) {
	pt, err = b.ReadInt8()
	if err != nil {
		return PacketTypeUnknown, err
	}
	return
}

// writePacketType 写入数据包类型.
func (b *Buffer) writePacketType(pt PacketType) (err error) {
	if err = b.WriteInt8(int8(pt)); err != nil {
		return err
	}
	return
}

// readErrCode 读取错误码.
func (b *Buffer) readErrCode() (errCode, error) {
	ec, err := b.ReadUint16()
	if err != nil {
		return errCodeOK, err
	}
	return errCode(ec), nil
}

// writeErrCode 写入错误码.
func (b *Buffer) writeErrCode(ec errCode) (err error) {
	if err = b.WriteUint16(uint16(ec)); err != nil {
		return err
	}
	return
}

// readActorUID 读取 ActorUID.
func (b *Buffer) readActorUID() (uid ActorUID, err error) {
	uid.Category, err = b.ReadUint16()
	if err != nil {
		return ActorUID{}, pkgerrors.WithMessage(err, "read actor category")
	}
	uid.ID, err = b.ReadInt64()
	if err != nil {
		return ActorUID{}, pkgerrors.WithMessage(err, "read actor id")
	}
	return
}

// writeActorUID 写入 ActorUID.
func (b *Buffer) writeActorUID(uid ActorUID) (err error) {
	if err = b.WriteUint16(uid.Category); err != nil {
		return pkgerrors.WithMessage(err, "write actor category")
	}
	if err = b.WriteInt64(uid.ID); err != nil {
		return pkgerrors.WithMessage(err, "write actor id")
	}
	return
}
