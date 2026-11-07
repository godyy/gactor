package gactor

import (
	"io"
	"sync"

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

// bufferPool 缓冲区池.
var bufferPool = &sync.Pool{
	New: func() any {
		return &Buffer{}
	},
}

// NewBuffer 创建缓冲区.
func NewBuffer(data []byte) *Buffer {
	b := bufferPool.Get().(*Buffer)
	b.SetBuf(data)
	return b
}

// Release 释放缓冲区.
func (b *Buffer) Release() {
	b.SetBuf(nil)
	bufferPool.Put(b)
}

// CloneAndClear 克隆并清空缓冲区.
func (b *Buffer) CloneAndClear() *Buffer {
	cp := NewBuffer(nil)
	*cp = *b
	b.SetBuf(nil)
	return cp
}

// CloneUnread 克隆未读数据.
func (b *Buffer) CloneUnread() *Buffer {
	if unread := b.UnreadData(); len(unread) > 0 {
		cpUnread := make([]byte, len(unread))
		copy(cpUnread, unread)
		return NewBuffer(cpUnread)
	} else {
		return NewBuffer(nil)
	}
}

// CloneUnreadBytes 克隆未读数据字节切片.
func (b *Buffer) CloneUnreadBytes() []byte {
	if unread := b.UnreadData(); len(unread) > 0 {
		cp := make([]byte, len(unread))
		copy(cp, unread)
		return cp
	}
	return nil
}

// CopyUnread 复制未读数据到 dst缓冲区.
func (b *Buffer) CopyUnread(dst *Buffer) (int, error) {
	if dst == nil {
		return 0, nil
	}
	return dst.Write(b.UnreadData())
}

// ReadPacketType 读取数据包类型.
func (b *Buffer) ReadPacketType() (pt PacketType, err error) {
	pt, err = b.ReadInt8()
	if err != nil {
		return PacketTypeUnknown, err
	}
	return
}

// WritePacketType 写入数据包类型.
func (b *Buffer) WritePacketType(pt PacketType) (err error) {
	if err = b.WriteInt8(int8(pt)); err != nil {
		return err
	}
	return
}

// ReadErrCode 读取错误码.
func (b *Buffer) ReadErrCode() (ErrCode, error) {
	ec, err := b.ReadUint16()
	if err != nil {
		return ErrCodeOK, err
	}
	return ErrCode(ec), nil
}

// WriteErrCode 写入错误码.
func (b *Buffer) WriteErrCode(ec ErrCode) (err error) {
	if err = b.WriteUint16(uint16(ec)); err != nil {
		return err
	}
	return
}

// ReadActorUID 读取 ActorUID.
func (b *Buffer) ReadActorUID() (uid ActorUID, err error) {
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

// WriteActorUID 写入 ActorUID.
func (b *Buffer) WriteActorUID(uid ActorUID) (err error) {
	if err = b.WriteUint16(uint16(uid.Category)); err != nil {
		return pkgerrors.WithMessage(err, "write actor category")
	}
	if err = b.WriteInt64(int64(uid.ID)); err != nil {
		return pkgerrors.WithMessage(err, "write actor id")
	}
	return
}

// ReadUint16String 读取长度前缀字符串.
func (b *Buffer) ReadUint16String() (string, error) {
	n, err := b.ReadUint16()
	if err != nil {
		return "", pkgerrors.WithMessage(err, "read string length")
	}
	if n == 0 {
		return "", nil
	}

	data := make([]byte, n)
	if _, err := io.ReadFull(&b.Buffer, data); err != nil {
		return "", pkgerrors.WithMessage(err, "read string data")
	}
	return string(data), nil
}

// WriteUint16String 写入长度前缀字符串.
func (b *Buffer) WriteUint16String(v string) error {
	if len(v) > 0xffff {
		return pkgerrors.New("string too long")
	}
	if err := b.WriteUint16(uint16(len(v))); err != nil {
		return pkgerrors.WithMessage(err, "write string length")
	}
	if len(v) == 0 {
		return nil
	}
	if _, err := b.Write([]byte(v)); err != nil {
		return pkgerrors.WithMessage(err, "write string data")
	}
	return nil
}
