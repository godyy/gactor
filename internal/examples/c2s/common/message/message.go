package message

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/godyy/gactor"
	"github.com/godyy/gactor/internal/examples/c2s/common"
	"github.com/godyy/gcluster/net"
	pkgerrors "github.com/pkg/errors"
)

type Message interface {
	EncodePacket(pa gactor.PacketAllocator) (gactor.Packet, error)
	Encode() (gactor.Packet, error)
	Decode(p gactor.Packet) error
	DecodePayload(v any) error
}

type ReqMessage struct {
	ReqId   uint32
	MsgId   uint16
	Payload any
}

func (m *ReqMessage) EncodePacket(pa gactor.PacketAllocator) (gactor.Packet, error) {
	payloadBytes, err := json.Marshal(m.Payload)
	if err != nil {
		return nil, pkgerrors.WithMessage(err, "marshal payload")
	}

	p, err := pa.Allocate(4 + 2 + len(payloadBytes))
	if err != nil {
		return nil, pkgerrors.WithMessage(err, "allocate packet")
	}

	if err := p.WriteUint32(m.ReqId); err != nil {
		return nil, pkgerrors.WithMessage(err, "write reqId")
	}

	if err := p.WriteUint16(m.MsgId); err != nil {
		return nil, pkgerrors.WithMessage(err, "write msgId")
	}

	if _, err := p.Write(payloadBytes); err != nil {
		return nil, pkgerrors.WithMessage(err, "write payload")
	}

	return p, nil
}

func (m *ReqMessage) Encode() (gactor.Packet, error) {
	payloadBytes, err := json.Marshal(m.Payload)
	if err != nil {
		return nil, pkgerrors.WithMessage(err, "marshal payload")
	}

	p := net.NewRawPacketWithCap(4 + 2 + len(payloadBytes))

	if err := p.WriteUint32(m.ReqId); err != nil {
		return nil, pkgerrors.WithMessage(err, "write reqId")
	}

	if err := p.WriteUint16(m.MsgId); err != nil {
		return nil, pkgerrors.WithMessage(err, "write msgId")
	}

	if _, err := p.Write(payloadBytes); err != nil {
		return nil, pkgerrors.WithMessage(err, "write payload")
	}

	return p, nil
}

func (m *ReqMessage) Decode(p gactor.Packet) error {
	var err error

	m.ReqId, err = p.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read reqId")
	}

	m.MsgId, err = p.ReadUint16()
	if err != nil {
		return pkgerrors.WithMessage(err, "read msgId")
	}

	m.Payload = p

	return gactor.ErrPacketEscape
}

func (m *ReqMessage) DecodePayload(v any) error {
	return decodePayload(m.Payload.(gactor.Packet), m.MsgId, v)
}

func NewReqMessageWithPayload(reqId uint32, payload any) ReqMessage {
	msgId, ok := msgType2Id[reflect.TypeOf(payload)]
	if !ok {
		panic(fmt.Sprintf("msgId not found of payload type %s", reflect.TypeOf(payload)))
	}
	return ReqMessage{
		ReqId:   reqId,
		MsgId:   msgId,
		Payload: payload,
	}
}

type RespMessage struct {
	ReqId   uint32
	MsgId   uint16
	Payload any
}

func (m *RespMessage) EncodePacket(pa gactor.PacketAllocator) (gactor.Packet, error) {
	payloadBytes, err := json.Marshal(m.Payload)
	if err != nil {
		return nil, pkgerrors.WithMessage(err, "marshal payload")
	}

	p, err := pa.Allocate(4 + 2 + len(payloadBytes))
	if err != nil {
		return nil, pkgerrors.WithMessage(err, "allocate packet")
	}

	if err := p.WriteUint32(m.ReqId); err != nil {
		return nil, pkgerrors.WithMessage(err, "write reqId")
	}

	if err := p.WriteUint16(m.MsgId); err != nil {
		return nil, pkgerrors.WithMessage(err, "write msgId")
	}

	if _, err := p.Write(payloadBytes); err != nil {
		return nil, pkgerrors.WithMessage(err, "write payload")
	}

	return p, nil
}

func (m *RespMessage) Encode() (gactor.Packet, error) {
	payloadBytes, err := json.Marshal(m.Payload)
	if err != nil {
		return nil, pkgerrors.WithMessage(err, "marshal payload")
	}

	p := net.NewRawPacketWithCap(4 + 2 + len(payloadBytes))

	if err := p.WriteUint32(m.ReqId); err != nil {
		return nil, pkgerrors.WithMessage(err, "write reqId")
	}

	if err := p.WriteUint16(m.MsgId); err != nil {
		return nil, pkgerrors.WithMessage(err, "write msgId")
	}

	if _, err := p.Write(payloadBytes); err != nil {
		return nil, pkgerrors.WithMessage(err, "write payload")
	}

	return p, nil
}

func (m *RespMessage) Decode(p gactor.Packet) error {
	var err error

	m.ReqId, err = p.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read reqId")
	}

	m.MsgId, err = p.ReadUint16()
	if err != nil {
		return pkgerrors.WithMessage(err, "read msgId")
	}

	m.Payload = p

	return gactor.ErrPacketEscape
}

func (m *RespMessage) DecodePayload(v any) error {
	return decodePayload(m.Payload.(gactor.Packet), m.MsgId, v)
}

func NewRespMessageWithPayload(reqId uint32, payload any) RespMessage {
	msgId, ok := msgType2Id[reflect.TypeOf(payload)]
	if !ok {
		panic(fmt.Sprintf("msgId not found of payload type %s", reflect.TypeOf(payload)))
	}
	return RespMessage{
		ReqId:   reqId,
		MsgId:   msgId,
		Payload: payload,
	}
}

type PushMessage struct {
	MsgId   uint16
	Payload any
}

func (m *PushMessage) EncodePacket(pa gactor.PacketAllocator) (gactor.Packet, error) {
	payloadBytes, err := json.Marshal(m.Payload)
	if err != nil {
		return nil, pkgerrors.WithMessage(err, "marshal payload")
	}

	p, err := pa.Allocate(2 + len(payloadBytes))
	if err != nil {
		return nil, pkgerrors.WithMessage(err, "allocate packet")
	}

	if err := p.WriteUint16(m.MsgId); err != nil {
		return nil, pkgerrors.WithMessage(err, "write msgId")
	}

	if _, err := p.Write(payloadBytes); err != nil {
		return nil, pkgerrors.WithMessage(err, "write payload")
	}

	return p, nil
}

func (m *PushMessage) Encode() (gactor.Packet, error) {
	payloadBytes, err := json.Marshal(m.Payload)
	if err != nil {
		return nil, pkgerrors.WithMessage(err, "marshal payload")
	}

	p := net.NewRawPacketWithCap(2 + len(payloadBytes))

	if err := p.WriteUint16(m.MsgId); err != nil {
		return nil, pkgerrors.WithMessage(err, "write msgId")
	}

	if _, err := p.Write(payloadBytes); err != nil {
		return nil, pkgerrors.WithMessage(err, "write payload")
	}

	return p, nil
}

func (m *PushMessage) Decode(p gactor.Packet) error {
	var err error

	m.MsgId, err = p.ReadUint16()
	if err != nil {
		return pkgerrors.WithMessage(err, "read msgId")
	}

	m.Payload = p

	return gactor.ErrPacketEscape
}

func (m *PushMessage) DecodePayload(v any) error {
	return decodePayload(m.Payload.(gactor.Packet), m.MsgId, v)
}

func NewPushMessageWithPayload(payload any) PushMessage {
	msgId, ok := msgType2Id[reflect.TypeOf(payload)]
	if !ok {
		panic(fmt.Sprintf("msgId not found of payload type %s", reflect.TypeOf(payload)))
	}

	return PushMessage{
		MsgId:   msgId,
		Payload: payload,
	}
}

type RpcMessage struct {
	MsgId   uint16
	Payload any
}

func (m *RpcMessage) EncodePacket(pa gactor.PacketAllocator) (gactor.Packet, error) {
	payloadBytes, err := json.Marshal(m.Payload)
	if err != nil {
		return nil, pkgerrors.WithMessage(err, "marshal payload")
	}

	p, err := pa.Allocate(2 + len(payloadBytes))
	if err != nil {
		return nil, pkgerrors.WithMessage(err, "allocate packet")
	}

	if err := p.WriteUint16(m.MsgId); err != nil {
		return nil, pkgerrors.WithMessage(err, "write msgId")
	}

	if _, err := p.Write(payloadBytes); err != nil {
		return nil, pkgerrors.WithMessage(err, "write payload")
	}

	return p, nil
}

func (m *RpcMessage) Encode() (gactor.Packet, error) {
	payloadBytes, err := json.Marshal(m.Payload)
	if err != nil {
		return nil, pkgerrors.WithMessage(err, "marshal payload")
	}

	p := net.NewRawPacketWithCap(2 + len(payloadBytes))

	if err := p.WriteUint16(m.MsgId); err != nil {
		return nil, pkgerrors.WithMessage(err, "write msgId")
	}

	if _, err := p.Write(payloadBytes); err != nil {
		return nil, pkgerrors.WithMessage(err, "write payload")
	}

	return p, nil
}

func (m *RpcMessage) Decode(p gactor.Packet) error {
	var err error

	m.MsgId, err = p.ReadUint16()
	if err != nil {
		return pkgerrors.WithMessage(err, "read msgId")
	}

	m.Payload = p
	return gactor.ErrPacketEscape
}

func (m *RpcMessage) DecodePayload(v any) error {
	return decodePayload(m.Payload.(gactor.Packet), m.MsgId, v)
}

func NewRpcMessageWithPayload(payload any) RpcMessage {
	msgId, ok := msgType2Id[reflect.TypeOf(payload)]
	if !ok {
		panic(fmt.Sprintf("msgId not found of payload type %s", reflect.TypeOf(payload)))
	}

	return RpcMessage{
		MsgId:   msgId,
		Payload: payload,
	}
}

type RpcRespMessage struct {
	MsgId   uint16
	Payload any
}

func (m *RpcRespMessage) EncodePacket(pa gactor.PacketAllocator) (gactor.Packet, error) {
	payloadBytes, err := json.Marshal(m.Payload)
	if err != nil {
		return nil, pkgerrors.WithMessage(err, "marshal payload")
	}

	p, err := pa.Allocate(2 + len(payloadBytes))
	if err != nil {
		return nil, pkgerrors.WithMessage(err, "allocate packet")
	}

	if err := p.WriteUint16(m.MsgId); err != nil {
		return nil, pkgerrors.WithMessage(err, "write msgId")
	}

	if _, err := p.Write(payloadBytes); err != nil {
		return nil, pkgerrors.WithMessage(err, "write payload")
	}

	return p, nil
}

func (m *RpcRespMessage) Encode() (gactor.Packet, error) {
	payloadBytes, err := json.Marshal(m.Payload)
	if err != nil {
		return nil, pkgerrors.WithMessage(err, "marshal payload")
	}

	p := net.NewRawPacketWithCap(2 + len(payloadBytes))

	if err := p.WriteUint16(m.MsgId); err != nil {
		return nil, pkgerrors.WithMessage(err, "write msgId")
	}

	if _, err := p.Write(payloadBytes); err != nil {
		return nil, pkgerrors.WithMessage(err, "write payload")
	}

	return p, nil
}

func (m *RpcRespMessage) Decode(p gactor.Packet) error {
	var err error

	m.MsgId, err = p.ReadUint16()
	if err != nil {
		return pkgerrors.WithMessage(err, "read msgId")
	}

	m.Payload = p

	return gactor.ErrPacketEscape
}

func (m *RpcRespMessage) DecodePayload(v any) error {
	return decodePayload(m.Payload.(gactor.Packet), m.MsgId, v)
}

func NewRpcRespMessageWithPayload(payload any) RpcRespMessage {
	msgId, ok := msgType2Id[reflect.TypeOf(payload)]
	if !ok {
		panic(fmt.Sprintf("msgId not found of payload type %s", reflect.TypeOf(payload)))
	}

	return RpcRespMessage{
		MsgId:   msgId,
		Payload: payload,
	}
}

func decodePayload(p gactor.Packet, msgId uint16, v any) error {
	rt, ok := msgId2Type[msgId]
	if !ok {
		return fmt.Errorf("msgId %d not exist", msgId)
	}
	if reflect.TypeOf(v) != rt {
		return fmt.Errorf("v Type not match")
	}

	if err := json.Unmarshal(p.UnreadData(), v); err != nil {
		return pkgerrors.WithMessage(err, "unmarshal payload")
	}

	return nil
}

const (
	MsgIdError         = uint16(9999)
	MsgIdLoginReq      = uint16(1)
	MsgIdLoginResp     = uint16(2)
	MsgIdNotify        = uint16(3)
	MsgIdHeartbeatReq  = uint16(4)
	MsgIdHeartbeatResp = uint16(5)
	MsgIdGetNameReq    = uint16(6)
	MsgIdGetNameResp   = uint16(7)
)

type Error struct {
	Code common.ErrCode `json:"code"`
}

type LoginReq struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type LoginResp struct {
	Err string `json:"err"`
}

type Notify struct {
	Msg string `json:"msg"`
}

type HeartbeatReq struct {
	Ts int64 `json:"ts"`
}

type HeartbeatResp struct {
	Ts int64 `json:"ts"`
}

type GetNameReq struct {
}

type GetNameResp struct {
	Name string `json:"name"`
}

var (
	msgId2Type = map[uint16]reflect.Type{}
	msgType2Id = map[reflect.Type]uint16{}
)

func register(msgId uint16, msg any) {
	t := reflect.TypeOf(msg)
	if t.Kind() != reflect.Pointer {
		panic("msg must be a pointer")
	}

	msgId2Type[msgId] = t
	msgType2Id[t] = msgId
}

func init() {
	register(MsgIdError, &Error{})
	register(MsgIdLoginReq, &LoginReq{})
	register(MsgIdLoginResp, &LoginResp{})
	register(MsgIdNotify, &Notify{})
	register(MsgIdHeartbeatReq, &HeartbeatReq{})
	register(MsgIdHeartbeatResp, &HeartbeatResp{})
	register(MsgIdGetNameReq, &GetNameReq{})
	register(MsgIdGetNameResp, &GetNameResp{})
}
