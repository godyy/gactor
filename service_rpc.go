package gactor

import (
	"context"
	"time"
)

// RPCConfig RPC配置.
type RPCConfig struct {
	// DefRPCTimeout 默认 RPC 超时时间. 默认值 5s.
	// 网络传输保留毫秒级精度, 向下取整.
	DefRPCTimeout time.Duration

	// MaxRPCCallAmount 支持的最大并发RPC调用数量.
	// 默认值 1000.
	MaxRPCCallAmount int
}

func (c *RPCConfig) init() {
	if c.DefRPCTimeout <= 0 {
		c.DefRPCTimeout = 5 * time.Second
	}

	if c.MaxRPCCallAmount <= 0 {
		c.MaxRPCCallAmount = 1000
	}
}

// startRpcManager 启动 rpc 管理器.
func (s *Service) startRpcManager() {
	s.rpcManager.start()
}

// doRPC 代理 from, 向 to 请求的目标 Actor 发起 RPC 调用.
// ctx 若未设置dealine, 底层会设置默认的超时时间. ctx 的 deadline 会通过超时传递到
// 远端, 用于简单的超时协同控制.
// async 表示是否采用异步模式. 非异步模式下, ctx 会被传递给请求上下文.
func (s *Service) doRPC(ctx context.Context, from ActorUID, to ActorUID, params any, cb RPCFunc, async bool) error {
	var (
		toNodeId string
		b        []byte
		err      error
	)

	// 获取目标 Actor 所在节点信息.
	toNodeId, err = s.getNodeIdOfActor(to)
	if err != nil {
		s.monitorRPCAction(MonitorCANodeInfoErr)
		return err
	}

	// 检查 ctx 是否已经取消.
	if err := ctx.Err(); err != nil {
		s.monitorRPCActionContextErr(err)
		return err
	}

	// 检查ctx deadline, 若未设置, 设置默认超时.
	deadline, ok := ctx.Deadline()
	if !ok {
		var cancel context.CancelFunc
		deadline = time.Now().Add(s.cfg.DefRPCTimeout)
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}

	// 创建 RPC 调用实例.
	callReqId, err := s.rpcManager.createCall(from, to, deadline, cb)
	if err != nil {
		return err
	}

	// 生成数据包序号.
	seq := s.genSeq()

	if toNodeId != s.nodeId() {
		// 目标节点非本地.
		// 编码数据包并发送到远端节点.
		ph := s2sRpcPacketHead{
			seq:     seq,
			reqId:   callReqId,
			fromId:  from,
			toId:    to,
			timeout: uint32(time.Until(deadline).Milliseconds()),
		}
		if err = s.sendRemotePacket(ctx, toNodeId, &ph, params); err != nil {
			s.monitorRPCActionSend2RemoteErr(err)
		}

	} else {
		// 目标节点为本地.

		// 编码 payload, 创建 rpcRequest 并发送给 Actor.
		if b, err = s.encodePayload(PacketTypeS2SRpc, params); err != nil {
			s.monitorRPCAction(MonitorCASend2LocalErr)
		} else {
			buf := Buffer{}
			buf.SetBuf(b)

			// 根据是否异步，创建请求上下文.
			// 并发送给目标actor.
			var req *Context
			if async {
				req = newContext(s, newRPCRequest(toNodeId, seq, callReqId, from, buf, deadline.UnixMilli()))
			} else {
				req = newContextWithCtx(ctx, s, newRPCRequest(toNodeId, seq, callReqId, from, buf, deadline.UnixMilli()))
			}
			if err = s.send2Actor(ctx, to, req); err != nil {
				req.release()
				s.monitorRPCActionSend2LocalErr(err)
			}
		}
	}

	if err != nil {
		s.rpcManager.removeCall(callReqId, from, to)
	}

	return err
}

// rpcDoneFunc 同步 RPC 回调封装.
type rpcDoneFunc struct {
	done  chan struct{} // 完成信号.
	reply any           // 响应参数.
	err   error         // 错误信息.
}

func (cb *rpcDoneFunc) invoke(resp *RPCResp) {
	defer close(cb.done)

	if err := resp.Err(); err != nil {
		cb.err = err
		return
	}

	if err := resp.DecodeReply(cb.reply); err != nil {
		cb.err = err
		return
	}
}

// rpc 向 to 指向的 Actor 发起 RPC 调用.
func (s *Service) rpc(ctx context.Context, from, to ActorUID, params any, reply any) error {
	// 执行同步RPC 调用.
	doneFunc := rpcDoneFunc{
		done:  make(chan struct{}),
		reply: reply,
	}
	if err := s.doRPC(ctx, from, to, params, doneFunc.invoke, false); err != nil {
		return err
	}

	// 等待调用完成(超时).
	<-doneFunc.done

	return doneFunc.err
}

// RPC 向 to 指向的 Actor 发起 RPC 调用. 若 Service 未启动或停机, 返回错误.
func (s *Service) RPC(ctx context.Context, to ActorUID, params any, reply any) error {
	if err := s.lockState(serviceStateStarted, true); err != nil {
		return err
	}
	defer s.unlockState(true)
	return s.rpc(ctx, ActorUID{}, to, params, reply)
}

// asyncRPC 向 to 指向的 Actor 发起异步 RPC 调用.
func (s *Service) asyncRPC(ctx context.Context, from, to ActorUID, params any, cb RPCFunc) error {
	return s.doRPC(ctx, from, to, params, cb, true)
}

// AsyncRPC 向 to 指向的 Actor 发起异步 RPC 调用. 若 Service 未启动或停机, 返回错误.
func (s *Service) AsyncRPC(ctx context.Context, to ActorUID, params any, cb RPCFunc) error {
	if err := s.lockState(serviceStateStarted, true); err != nil {
		return err
	}
	defer s.unlockState(true)
	return s.asyncRPC(ctx, ActorUID{}, to, params, cb)
}
