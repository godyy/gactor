package gactor

import (
	"errors"
	"sync"

	"go.uber.org/zap/zapcore"
)

// RequestType 请求类型.
type RequestType int8

const (
	_               = RequestType(iota)
	RequestTypeReq  // 对应 PacketTypeRawReq.
	RequestTypeRPC  // 对应 PacketTypeS2SRpc.
	RequestTypeCast // 对应 PacketTypeS2SCast.
)

var requestTypeStrings = [...]string{
	RequestTypeReq:  "req",
	RequestTypeRPC:  "rpc",
	RequestTypeCast: "cast",
}

func (t RequestType) String() string {
	return requestTypeStrings[t]
}

// errSkipHandleRequest 生命跳过处理请求的错误.
var errSkipHandleRequest = errors.New("skip handle request")

// request Request 内部接口封装.
type request interface {
	// requestType 请求类型.
	requestType() RequestType

	// fromActorUID 请求来源Actor ID.
	fromActorUID() ActorUID

	// isTimeout 是否超时.
	isTimeout(nowMs int64) bool

	// decode 解码负载数据到 v 指向的数据结构中.
	// * 不可重入, 只能调用一次.
	decode(ctx *Context, v any) error

	// reply 回复请求. 参数为负载数据实体.
	// * 回复成功后调用无任何效果.
	reply(ctx *Context, payload any) error

	// replyError 回复错误码.
	// * 回复成功后调用无任何效果
	replyError(ctx *Context, ec errCode) error

	// replyDecodeError 回复解码错误.
	// * 回复成功后调用无任何效果.
	replyDecodeError(ctx *Context) error

	// clone 克隆 request.
	clone(ctx *Context) request

	// release 回收.
	release(ctx *Context)

	// beforeHandle 处理请求.
	// 如果返回 errSkipHandleRequest 错误, 则跳过处理请求.
	beforeHandle(ctx *Context) error

	// zap.Field支持
	zapcore.ObjectMarshaler
}

type requestCom struct {
	fromNodeId string // 请求发起节点ID
	replied    bool   // 是否已回复
	payload    Buffer // 负载数据
}

func (req *requestCom) isTimeout(nowMs int64) bool {
	return false
}

func (req *requestCom) decode(ctx *Context, pt PacketType, v any) error {
	if err := ctx.service().decodePayload(pt, &req.payload, v); err == nil {
		return nil
	} else if errors.Is(err, ErrBytesEscape) {
		req.payload.SetBuf(nil)
		return nil
	} else {
		return err
	}
}

func (req *requestCom) clone(ctx *Context) requestCom {
	cp := *req
	if unreadData := req.payload.UnreadData(); len(unreadData) > 0 {
		b := ctx.service().getBytes(len(unreadData))
		b = append(b, unreadData...)
		cp.payload.SetBuf(b)
	}
	return cp
}

func (req *requestCom) release(ctx *Context) {
	ctx.service().freeBuffer(&req.payload)
	req.fromNodeId = ""
	req.replied = false
}

func (req *requestCom) beforeHandle(ctx *Context) error {
	return nil
}

// rawRequest 对应 PacketTypeRawReq.
type rawRequest struct {
	requestCom
	seq         uint32 // 包序
	sid         uint32 // 会话ID
	timeoutAtMs int64  // 超时时刻, 毫秒级
}

var poolOfRawRequest = &sync.Pool{New: func() interface{} {
	return &rawRequest{}
}}

func newRawRequest(fromNodeId string, seq, sid uint32, payload Buffer, timeoutAtMs int64) *rawRequest {
	req := poolOfRawRequest.Get().(*rawRequest)
	req.fromNodeId = fromNodeId
	req.payload = payload
	req.seq = seq
	req.sid = sid
	req.timeoutAtMs = timeoutAtMs
	return req
}

func (req *rawRequest) requestType() RequestType {
	return RequestTypeReq
}

func (req *rawRequest) fromActorUID() ActorUID {
	return ActorUID{}
}

func (req *rawRequest) isTimeout(nowMs int64) bool {
	return nowMs >= req.timeoutAtMs
}

func (req *rawRequest) decode(ctx *Context, v any) error {
	return req.requestCom.decode(ctx, PacketTypeRawReq, v)
}

func (req *rawRequest) reply(ctx *Context, payload any) error {
	if req.replied {
		return nil
	}

	head := rawRespPacketHead{
		seq:     ctx.service().genSeq(),
		fromId:  ctx.actor.core().id,
		sid:     req.sid,
		errCode: errCodeOK,
	}
	if err := ctx.service().sendRemotePacket(ctx, req.fromNodeId, &head, payload); err != nil {
		if errors.Is(err, errCodeEncodePacketFailed) {
			return req.replyError(ctx, errCodeEncodePacketFailed)
		}
		return err
	}

	req.replied = true

	return nil
}

func (req *rawRequest) replyDecodeError(ctx *Context) error {
	return req.replyError(ctx, errCodeDecodePacketFailed)
}

func (req *rawRequest) replyError(ctx *Context, ec errCode) error {
	if req.replied {
		return nil
	}

	head := rawRespPacketHead{
		seq:     ctx.service().genSeq(),
		fromId:  ctx.actor.core().id,
		sid:     req.sid,
		errCode: ec,
	}

	if err := ctx.service().sendRemotePacket(ctx, req.fromNodeId, &head, nil); err != nil {
		return err
	}

	req.replied = true

	return nil
}

func (req *rawRequest) clone(ctx *Context) request {
	cp := poolOfRawRequest.Get().(*rawRequest)
	cp.requestCom = req.requestCom.clone(ctx)
	cp.seq = req.seq
	cp.sid = req.sid
	cp.timeoutAtMs = req.timeoutAtMs
	return cp
}

func (req *rawRequest) release(ctx *Context) {
	req.requestCom.release(ctx)
	poolOfRawRequest.Put(req)
}

func (req *rawRequest) beforeHandle(ctx *Context) error {
	ca, ok := ctx.Actor().(*cActor)
	if !ok {
		return errors.New("not CActor")
	}

	if !ca.session.IsConnected() {
		_ = req.replyError(ctx, errCodeActorNotConnect)
		return errSkipHandleRequest
	}

	reqSession := ActorSession{
		NodeId: req.fromNodeId,
		SID:    req.sid,
	}
	if reqSession != ca.session {
		_ = req.replyError(ctx, errCodeActorOtherConnect)
		return errSkipHandleRequest
	}

	return nil
}

func (req *rawRequest) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("type", req.requestType().String())
	enc.AddString("fromNode", req.fromNodeId)
	enc.AddUint32("seq", req.seq)
	enc.AddUint32("sid", req.sid)
	enc.AddInt64("timeoutAtMs", req.timeoutAtMs)
	return nil
}

// rpcRequest 对应 PacketTypeS2SRpc.
type rpcRequest struct {
	requestCom
	seq         uint32   // 包序
	reqId       uint32   // 请求ID
	fromId      ActorUID // 来源 Actor ID.
	timeoutAtMs int64    // 超时时刻, 毫秒级.
}

var poolOfRpcRequest = &sync.Pool{New: func() interface{} {
	return &rpcRequest{}
}}

func newRPCRequest(remoteNodeId string, seq, reqId uint32, fromId ActorUID, payload Buffer, timeoutAtMs int64) *rpcRequest {
	req := poolOfRpcRequest.Get().(*rpcRequest)
	req.fromNodeId = remoteNodeId
	req.payload = payload
	req.seq = seq
	req.reqId = reqId
	req.fromId = fromId
	req.timeoutAtMs = timeoutAtMs
	return req
}

func (req *rpcRequest) requestType() RequestType {
	return RequestTypeRPC
}

func (req *rpcRequest) fromActorUID() ActorUID {
	return req.fromId
}

func (req *rpcRequest) isTimeout(nowMs int64) bool {
	return nowMs >= req.timeoutAtMs
}

func (req *rpcRequest) decode(ctx *Context, v any) error {
	return req.requestCom.decode(ctx, PacketTypeS2SRpc, v)
}

func (req *rpcRequest) reply(ctx *Context, payload any) error {
	if req.replied {
		return nil
	}

	head := s2sRpcRespPacketHead{
		seq:     ctx.service().genSeq(),
		reqId:   req.reqId,
		fromId:  ctx.actor.ActorUID(),
		toId:    req.fromId,
		errCode: errCodeOK,
	}
	if err := ctx.service().sendPacket(ctx, req.fromNodeId, &head, payload); err != nil {
		if errors.Is(err, errCodeEncodePacketFailed) {
			return req.replyError(ctx, errCodeEncodePacketFailed)
		}
		return err
	}

	req.replied = true

	return nil
}

func (req *rpcRequest) replyDecodeError(ctx *Context) error {
	return req.replyError(ctx, errCodeDecodePacketFailed)
}

func (req *rpcRequest) replyError(ctx *Context, ec errCode) error {
	if req.replied {
		return nil
	}

	head := s2sRpcRespPacketHead{
		seq:     ctx.service().genSeq(),
		reqId:   req.reqId,
		fromId:  ctx.actor.ActorUID(),
		toId:    req.fromId,
		errCode: ec,
	}
	if err := ctx.service().sendPacket(ctx, req.fromNodeId, &head, nil); err != nil {
		return err
	}

	req.replied = true

	return nil
}

func (req *rpcRequest) clone(ctx *Context) request {
	cp := poolOfRpcRequest.Get().(*rpcRequest)
	cp.requestCom = req.requestCom.clone(ctx)
	cp.seq = req.seq
	cp.reqId = req.reqId
	cp.fromId = req.fromId
	cp.timeoutAtMs = req.timeoutAtMs
	return cp
}

func (req *rpcRequest) release(ctx *Context) {
	req.requestCom.release(ctx)
	poolOfRpcRequest.Put(req)
}

func (req *rpcRequest) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("type", req.requestType().String())
	enc.AddString("fromNode", req.fromNodeId)
	enc.AddUint32("seq", req.seq)
	enc.AddUint32("reqId", req.reqId)
	enc.AddObject("fromId", &req.fromId)
	enc.AddInt64("timeoutAtMs", req.timeoutAtMs)
	return nil
}

// castRequest 对应 PacketTypeS2SCast.
type castRequest struct {
	requestCom
	seq    uint32
	fromId ActorUID
}

var poolOfCastRequest = &sync.Pool{New: func() interface{} {
	return &castRequest{}
}}

func newCastRequest(fromNodeId string, seq uint32, fromId ActorUID, payload Buffer) *castRequest {
	req := poolOfCastRequest.Get().(*castRequest)
	req.fromNodeId = fromNodeId
	req.payload = payload
	req.seq = seq
	req.fromId = fromId
	return req
}

func (req *castRequest) requestType() RequestType {
	return RequestTypeCast
}

func (req *castRequest) fromActorUID() ActorUID {
	return req.fromId
}

func (req *castRequest) decode(ctx *Context, v any) error {
	return req.requestCom.decode(ctx, PacketTypeS2SCast, v)
}

func (req *castRequest) reply(ctx *Context, _ any) error {
	return nil
}

func (req *castRequest) replyDecodeError(ctx *Context) error {
	return nil
}

func (req *castRequest) replyError(ctx *Context, _ errCode) error {
	return nil
}

func (req *castRequest) clone(ctx *Context) request {
	cp := poolOfCastRequest.Get().(*castRequest)
	cp.requestCom = req.requestCom.clone(ctx)
	cp.seq = req.seq
	return cp
}

func (req *castRequest) release(ctx *Context) {
	req.requestCom.release(ctx)
	poolOfCastRequest.Put(req)
}

func (req *castRequest) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("type", req.requestType().String())
	enc.AddUint32("seq", req.seq)
	enc.AddObject("fromId", &req.fromId)
	return nil
}
