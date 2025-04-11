package message

import (
	"encoding/json"
	"fmt"
	"reflect"

	pkgerrors "github.com/pkg/errors"

	"github.com/godyy/gactor"
)

type Msg struct {
	ID      int
	Payload any
}

func (m *Msg) DecodePayload(v any) error {
	rt, ok := msgId2Type[m.ID]
	if !ok {
		return fmt.Errorf("msgId %d not exist", m.ID)
	}
	if reflect.TypeOf(v) != rt {
		return fmt.Errorf("v Type not match")
	}

	if err := json.Unmarshal(m.Payload.(gactor.Packet).UnreadData(), v); err != nil {
		return pkgerrors.WithMessage(err, "unmarshal payload")
	}

	return nil
}

func NewMsgWithPayload(payload any) Msg {
	msgId, ok := msgType2Id[reflect.TypeOf(payload)]
	if !ok {
		panic(fmt.Sprintf("msgId not found of payload type %s", reflect.TypeOf(payload)))
	}
	return Msg{
		ID:      msgId,
		Payload: payload,
	}
}

func EncodePacket(pa gactor.PacketAllocator, msg *Msg) (gactor.Packet, error) {
	payloadBytes, err := json.Marshal(msg.Payload)
	if err != nil {
		return nil, pkgerrors.WithMessage(err, "marshal payload")
	}

	p, err := pa.Allocate(4 + len(payloadBytes))
	if err != nil {
		return nil, err
	}

	if err := p.WriteUint32(uint32(msg.ID)); err != nil {
		return nil, pkgerrors.WithMessage(err, "write msgId")
	}

	if _, err := p.Write(payloadBytes); err != nil {
		return nil, pkgerrors.WithMessage(err, "write payload")
	}

	return p, nil
}

func EncodeMsg(pm gactor.PacketManager, msg *Msg) (gactor.Packet, error) {
	payloadBytes, err := json.Marshal(msg.Payload)
	if err != nil {
		return nil, pkgerrors.WithMessage(err, "marshal payload")
	}

	p := pm.GetPacket(4 + len(payloadBytes))
	if err := p.WriteUint32(uint32(msg.ID)); err != nil {
		return nil, pkgerrors.WithMessage(err, "write msgId")
	}

	if _, err := p.Write(payloadBytes); err != nil {
		return nil, pkgerrors.WithMessage(err, "write payload")
	}

	return p, nil
}

func DecodeMsgWithoutDecodePayload(p gactor.Packet, msg *Msg) error {
	msgId, err := p.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read msgId")
	}

	if _, ok := msgId2Type[int(msgId)]; !ok {
		return fmt.Errorf("msgId %d invalid", msgId)
	}

	msg.ID = int(msgId)
	msg.Payload = p
	return gactor.ErrPacketEscape
}

var (
	msgId2Type = map[int]reflect.Type{}
	msgType2Id = map[reflect.Type]int{}
)

func register(msgId int, payload any) {
	rt := reflect.TypeOf(payload)
	if rt.Kind() != reflect.Ptr {
		panic("payload must be a pointer")
	}
	if _, ok := msgId2Type[msgId]; ok {
		panic(fmt.Sprintf("msgId %d type exist", msgId))
	}
	msgId2Type[msgId] = rt
	msgType2Id[rt] = msgId
}

func init() {
	register(MsgIDGetServerNameReq, &GetServerNameReq{})
	register(MsgIDGetServerNameResp, &GetServerNameResp{})
}

const (
	MsgIDGetServerNameReq  = 11
	MsgIDGetServerNameResp = 12
)

type GetServerNameReq struct{}

type GetServerNameResp struct {
	ServerName string `json:"server_name"`
}
