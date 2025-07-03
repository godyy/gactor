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

	buf := m.Payload.(gactor.Buffer)
	if err := json.Unmarshal(buf.UnreadData(), v); err != nil {
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

func EncodePacket(pa gactor.PacketAllocator, msg *Msg) ([]byte, error) {
	payloadBytes, err := json.Marshal(msg.Payload)
	if err != nil {
		return nil, pkgerrors.WithMessage(err, "marshal payload")
	}

	var buf gactor.Buffer
	if err := pa.AllocBuf(&buf, 4+len(payloadBytes)); err != nil {
		return nil, err
	}

	if err := buf.WriteUint32(uint32(msg.ID)); err != nil {
		return nil, pkgerrors.WithMessage(err, "write msgId")
	}

	if _, err := buf.Write(payloadBytes); err != nil {
		return nil, pkgerrors.WithMessage(err, "write payload")
	}

	return buf.Data(), nil
}

func EncodeMsg(bm gactor.BytesManager, msg *Msg) ([]byte, error) {
	payloadBytes, err := json.Marshal(msg.Payload)
	if err != nil {
		return nil, pkgerrors.WithMessage(err, "marshal payload")
	}

	var buf gactor.Buffer
	buf.SetBuf(bm.GetBytes(4 + len(payloadBytes)))

	if err := buf.WriteUint32(uint32(msg.ID)); err != nil {
		return nil, pkgerrors.WithMessage(err, "write msgId")
	}

	if _, err := buf.Write(payloadBytes); err != nil {
		return nil, pkgerrors.WithMessage(err, "write payload")
	}

	return buf.Data(), nil
}

func DecodeMsgWithoutDecodePayload(b *gactor.Buffer, msg *Msg) error {
	msgId, err := b.ReadUint32()
	if err != nil {
		return pkgerrors.WithMessage(err, "read msgId")
	}

	if _, ok := msgId2Type[int(msgId)]; !ok {
		return fmt.Errorf("msgId %d invalid", msgId)
	}

	msg.ID = int(msgId)
	msg.Payload = *b
	return gactor.ErrBytesEscape
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
