package gactor

import (
	"errors"
	"sync"
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

// request Request 内部接口封装.
type request interface {
	// requestType 请求类型.
	requestType() RequestType

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
	beforeHandle(ctx *Context) error
}

type requestCom struct {
	replied bool
	payload Packet
}

func (req *requestCom) decode(ctx *Context, pt PacketType, v any) error {
	if err := ctx.service().decodePayload(pt, req.payload, v); err == nil {
		return nil
	} else if errors.Is(err, ErrPacketEscape) {
		req.payload = nil
		return nil
	} else {
		return err
	}
}

func (req *requestCom) clone(ctx *Context) requestCom {
	cp := requestCom{}
	if req.payload != nil {
		if unreadData := req.payload.UnreadData(); len(unreadData) > 0 {
			cp.payload = ctx.service().getPacket(len(unreadData))
			cp.payload.Write(unreadData)
		}
	}
	return cp
}

func (req *requestCom) release(ctx *Context) {
	if req.payload != nil {
		ctx.service().putPacket(req.payload)
		req.payload = nil
	}
	req.replied = false
}

func (req *requestCom) beforeHandle(ctx *Context) error {
	return nil
}

// rawRequest 对应 PacketTypeRawReq.
type rawRequest struct {
	requestCom
	fromNodeId string           // 请求来源节点ID.
	head       rawReqPacketHead // 包头信息.
}

var poolOfRawRequest = &sync.Pool{New: func() interface{} {
	return &rawRequest{}
}}

func newRawRequest(fromNodeId string, head rawReqPacketHead, payload Packet) *rawRequest {
	req := poolOfRawRequest.Get().(*rawRequest)
	req.fromNodeId = fromNodeId
	req.head = head
	req.payload = payload
	return req
}

func (req *rawRequest) requestType() RequestType {
	return RequestTypeReq
}

func (req *rawRequest) decode(ctx *Context, v any) error {
	return req.requestCom.decode(ctx, PacketTypeRawReq, v)
}

func (req *rawRequest) reply(ctx *Context, payload any) error {
	if req.replied {
		return nil
	}

	head := rawRespPacketHead{
		fromId:  req.head.toId,
		sid:     req.head.sid,
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

func (req *rawRequest) replyDecodeError(ctx *Context) error {
	return req.replyError(ctx, errCodeDecodePacketFailed)
}

func (req *rawRequest) replyError(ctx *Context, ec errCode) error {
	if req.replied {
		return nil
	}

	head := rawRespPacketHead{
		fromId:  req.head.toId,
		sid:     req.head.sid,
		errCode: ec,
	}

	if err := ctx.service().sendPacket(ctx, req.fromNodeId, &head, nil); err != nil {
		return err
	}

	req.replied = true

	return nil
}

func (req *rawRequest) clone(ctx *Context) request {
	cp := poolOfRawRequest.Get().(*rawRequest)
	cp.requestCom = req.requestCom.clone(ctx)
	cp.fromNodeId = req.fromNodeId
	cp.head = req.head
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

	ca.updateSession(ctx, ActorSession{
		NodeId: req.fromNodeId,
		SID:    req.head.sid,
	})

	return nil
}

// rpcRequest 对应 PacketTypeS2SRpc.
type rpcRequest struct {
	requestCom
	fromNodeId string           // 请求来源节点ID.
	head       s2sRpcPacketHead // 包头信息.
}

var poolOfRpcRequest = &sync.Pool{New: func() interface{} {
	return &rpcRequest{}
}}

func newRPCRequest(remoteNodeId string, head s2sRpcPacketHead, payload Packet) *rpcRequest {
	req := poolOfRpcRequest.Get().(*rpcRequest)
	req.fromNodeId = remoteNodeId
	req.head = head
	req.payload = payload
	return req
}

func (req *rpcRequest) requestType() RequestType {
	return RequestTypeRPC
}

func (req *rpcRequest) decode(ctx *Context, v any) error {
	return req.requestCom.decode(ctx, PacketTypeS2SRpc, v)
}

func (req *rpcRequest) reply(ctx *Context, payload any) error {
	if req.replied {
		return nil
	}

	head := s2sRpcRespPacketHead{
		reqId:   req.head.reqId,
		fromId:  req.head.toId,
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
		reqId:   req.head.reqId,
		fromId:  req.head.toId,
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
	cp.fromNodeId = req.fromNodeId
	cp.head = req.head
	return cp
}

func (req *rpcRequest) release(ctx *Context) {
	req.requestCom.release(ctx)
	poolOfRpcRequest.Put(req)
}

// castRequest 对应 PacketTypeS2SCast.
type castRequest struct {
	requestCom
	head s2sCastPacketHead // 包头信息.
}

var poolOfCastRequest = &sync.Pool{New: func() interface{} {
	return &castRequest{}
}}

func newCastRequest(head s2sCastPacketHead, payload Packet) *castRequest {
	req := poolOfCastRequest.Get().(*castRequest)
	req.head = head
	req.payload = payload
	return req
}

func (req *castRequest) requestType() RequestType {
	return RequestTypeCast
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
	cp.head = req.head
	return cp
}

func (req *castRequest) release(ctx *Context) {
	req.requestCom.release(ctx)
	poolOfCastRequest.Put(req)
}
