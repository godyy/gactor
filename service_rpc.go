package gactor

import (
	"context"
	"errors"
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

// doRPC 代理 from Actor 向 to 指向的 Actor 发起 RPC 调用.
// ctx 用于控制超时. params 表示请求参数. cb 为回调函数.
func (s *Service) doRPC(ctx context.Context, cancel context.CancelFunc, to ActorUID, params any, cb RPCFunc) error {
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

	if err := ctx.Err(); err != nil {
		s.monitorRPCActionContextErr(err)
		return err
	}

	deadline, ok := ctx.Deadline()
	if !ok {
		s.monitorRPCAction(MonitorCAContextErr)
		return errors.New("context deadline not set")
	}

	// 创建 RPC 调用实例.
	callReqId, err := s.rpcManager.createCall(to, deadline, cb)
	if err != nil {
		return err
	}

	// 包头.
	ph := s2sRpcPacketHead{
		seq_:    s.genSeq(),
		reqId:   callReqId,
		toId:    to,
		timeout: uint32(time.Until(deadline).Milliseconds()),
	}

	if toNodeId != s.nodeId() {
		// 目标节点非本地.
		// 编码数据包并发送到远端节点.

		if err = s.sendRemotePacket(ctx, toNodeId, &ph, params); err != nil {
			s.monitorRPCActionSend2RemoteErr(err)
		} else if cancel != nil {
			cancel()
		}

	} else {
		// 目标节点为本地.

		// 编码 payload, 创建 rpcRequest 并发送给 Actor.
		if b, err = s.encodePayload(PacketTypeS2SRpc, params); err != nil {
			s.monitorRPCAction(MonitorCASend2LocalErr)
		} else {
			buf := Buffer{}
			buf.SetBuf(b)
			request := newContext(ctx, cancel, s, newRPCRequest(toNodeId, ph, buf))
			if err = s.send2Actor(ctx, to, request); err != nil {
				request.release()
				s.monitorRPCActionSend2LocalErr(err)
			}
		}
	}

	if err != nil {
		s.rpcManager.removeCall(callReqId)
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
// ctx 用于控制超时. params 表示请求参数, reply 用于接收响应参数.
func (s *Service) rpc(ctx context.Context, to ActorUID, params any, reply any) error {
	var cancel context.CancelFunc

	// 优先检查 ctx 是否done.
	// 如果 ctx 未设置 deadline, 设置默认超时.
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, ok := ctx.Deadline(); !ok {
		ctx, cancel = context.WithTimeout(ctx, s.cfg.DefRPCTimeout)
	}

	// 执行 RPC 调用.
	doneFunc := rpcDoneFunc{
		done:  make(chan struct{}),
		reply: reply,
	}
	if err := s.doRPC(ctx, cancel, to, params, doneFunc.invoke); err != nil {
		if cancel != nil {
			cancel()
		}
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
	return s.rpc(ctx, to, params, reply)
}

// asyncRPC 向 to 指向的 Actor 发起异步 RPC 调用. ctx 用于控制超时时间.
// params 表示请求参数. cb 表示异步回调函数.
func (s *Service) asyncRPC(ctx context.Context, to ActorUID, params any, cb RPCFunc) error {
	var cancel context.CancelFunc

	// 优先检查 ctx 是否done.
	// 如果 ctx 未设置 deadline, 设置默认超时.
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, ok := ctx.Deadline(); !ok {
		ctx, cancel = context.WithTimeout(ctx, s.cfg.DefRPCTimeout)
	}

	// 执行 RPC 调用.
	if err := s.doRPC(ctx, cancel, to, params, cb); err != nil {
		if cancel != nil {
			cancel()
		}
		return err
	}

	return nil
}

// AsyncRPC 向 to 指向的 Actor 发起异步 RPC 调用. 若 Service 未启动或停机, 返回错误.
func (s *Service) AsyncRPC(ctx context.Context, to ActorUID, params any, cb RPCFunc) error {
	if err := s.lockState(serviceStateStarted, true); err != nil {
		return err
	}
	defer s.unlockState(true)
	return s.asyncRPC(ctx, to, params, cb)
}
