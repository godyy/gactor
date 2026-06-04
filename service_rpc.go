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
// deadline 指定超时时间, 若未设置, 采用默认超时时间.
// 若为远程 RPC 调用, deadline 会投递到远端节点作为超时协同参数.
func (s *Service) doRPC(from ActorUID, to ActorUID, params any, cb RPCFunc, deadline time.Time) (reqId uint32, err error) {
	var (
		toNodeId string
		leaseId  string
		b        []byte
	)

	// 检查deadline
	if !deadline.After(time.Now()) {
		return 0, ErrRPCTimeout
	}

	// 检查服务状态
	if err = s.checkNotStopped(); err != nil {
		return 0, err
	}

	// 获取目标 Actor 所在节点信息.
	toNodeId, leaseId, err = s.resolveNodeOfActor(actorNodeModeRouter, to)
	if err != nil {
		s.monitorRPCAction(MonitorCANodeInfoErr)
		return 0, err
	}

	// 再次检查deadline.
	if !deadline.After(time.Now()) {
		return 0, ErrRPCTimeout
	}

	// 创建 RPC 调用实例.
	// 若后续出现任意错误, 移除RPC调用.
	reqId, err = s.rpcManager.createCall(from, to, deadline, cb)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err != nil {
			s.rpcManager.removeCall(reqId, from, to)
		}
	}()

	// 生成数据包序号.
	seq := s.genSeq()

	if toNodeId != s.nodeId() {
		if err = s.checkNotStopped(); err != nil {
			return
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			err = ErrRPCTimeout
			return
		}

		// 目标节点非本地.
		// 编码数据包并发送到远端节点.
		ph := newS2SRpcHead(seq, reqId, from, to, uint32(remaining.Milliseconds()))
		if err = s.sendRemotePacket(toNodeId, &ph, params); err != nil {
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
			req := newContext(s, newRPCRequest(toNodeId, seq, reqId, from, buf, deadline.UnixMilli()))
			if err = s.send2LocalActor(to, req, leaseId); err != nil {
				req.release()
				s.monitorRPCActionSend2LocalErr(err)
			}
		}
	}

	return
}

// rpcDoneFunc 同步 RPC 回调封装.
type rpcDoneFunc struct {
	done    chan struct{} // 完成信号.
	svc     *Service      // 提供解码辅助.
	payload []byte        // payload 拷贝.
	err     error         // 错误信息.
}

func (cb *rpcDoneFunc) invoke(resp *RPCResp) {
	defer close(cb.done)
	if err := resp.Err(); err != nil {
		cb.err = err
		return
	}
	cb.payload = append(cb.payload[:0], resp.payload.UnreadData()...)
}

func (cb *rpcDoneFunc) decode(reply any) error {
	if cb.err != nil {
		return cb.err
	}
	var buf Buffer
	buf.SetBuf(cb.payload)
	if err := cb.svc.decodePayload(PacketTypeS2SRpcResp, &buf, reply); err == nil {
		return nil
	} else if err == ErrBytesEscape {
		return nil
	} else {
		return err
	}
}

// rpcWithDeadline 向 to 指向的 Actor 发起 RPC 调用.
// deadline 指定具体超时时刻.
func (s *Service) rpcWithDeadline(from, to ActorUID, params any, reply any, deadline time.Time) error {
	// 执行同步RPC 调用.
	doneFunc := rpcDoneFunc{done: make(chan struct{}), svc: s}
	if _, err := s.doRPC(from, to, params, doneFunc.invoke, deadline); err != nil {
		return err
	}
	<-doneFunc.done
	return doneFunc.decode(reply)
}

// rpcWithTimeout 向 to 指向的 Actor 发起 RPC 调用.
// timeout 指定超时间隔.
func (s *Service) rpcWithTimeout(from, to ActorUID, params any, reply any, timeout time.Duration) error {
	return s.rpcWithDeadline(from, to, params, reply, time.Now().Add(timeout))
}

// rpc 向 to 指向的 Actor 发起 RPC 调用.
// 超时间隔使用配置的默认值.
func (s *Service) rpc(from, to ActorUID, params any, reply any) error {
	return s.rpcWithDeadline(from, to, params, reply, time.Now().Add(s.cfg.DefRPCTimeout))
}

// rpcWithContext 向 to 指向的 Actor 发起 RPC 调用.
// 超时 deadline 从 ctx 获取，若未设置, 使用默认超时时间.
// 同时监听上下文, 若上下文取消，提前终止调用.
func (s *Service) rpcWithContext(ctx context.Context, from, to ActorUID, params any, reply any) error {
	// 检查上下文是否已取消.
	if err := ctx.Err(); err != nil {
		return err
	}

	// 获取dealine.
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(s.cfg.DefRPCTimeout)
	}

	// 执行同步RPC 调用.
	doneFunc := rpcDoneFunc{done: make(chan struct{}), svc: s}
	reqId, err := s.doRPC(from, to, params, doneFunc.invoke, deadline)
	if err != nil {
		return err
	}

	// 等待调用完成, 同时监听上下文.
	select {
	case <-doneFunc.done:
		return doneFunc.decode(reply)
	case <-ctx.Done():
		s.rpcManager.removeCall(reqId, from, to)
		return ctx.Err()
	}
}

// RPCWithDeadline 向 to 指向的 Actor 发起 RPC 调用.
// deadline 指定具体超时时刻.
func (s *Service) RPCWithDeadline(to ActorUID, params any, reply any, deadline time.Time) error {
	return s.rpcWithDeadline(ActorUID{}, to, params, reply, deadline)
}

// RPCWithTimeout 向 to 指向的 Actor 发起 RPC 调用.
// timeout 指定超时间隔.
func (s *Service) RPCWithTimeout(to ActorUID, params any, reply any, timeout time.Duration) error {
	return s.rpcWithTimeout(ActorUID{}, to, params, reply, timeout)
}

// RPC 向 to 指向的 Actor 发起 RPC 调用.
// 超时间隔使用配置的默认值.
func (s *Service) RPC(to ActorUID, params any, reply any) error {
	return s.rpc(ActorUID{}, to, params, reply)
}

// RPCWithContext 向 to 指向的 Actor 发起 RPC 调用.
// 超时 deadline 从 ctx 获取，若未设置, 使用默认超时时间.
// 同时监听上下文, 若上下文取消，提前终止调用.
func (s *Service) RPCWithContext(ctx context.Context, to ActorUID, params any, reply any) error {
	return s.rpcWithContext(ctx, ActorUID{}, to, params, reply)
}

// asyncRPCWithDeadline 向 to 指向的 Actor 发起异步 RPC 调用.
// deadline 指定具体超时时刻.
func (s *Service) asyncRPCWithDeadline(from, to ActorUID, params any, cb RPCFunc, deadline time.Time) error {
	if _, err := s.doRPC(from, to, params, cb, deadline); err != nil {
		return err
	}
	return nil
}

// asyncRPCWithTimeout 向 to 指向的 Actor 发起异步 RPC 调用.
// timeout 指定超时间隔.
func (s *Service) asyncRPCWithTimeout(from, to ActorUID, params any, cb RPCFunc, timeout time.Duration) error {
	return s.asyncRPCWithDeadline(from, to, params, cb, time.Now().Add(timeout))
}

// asyncRPCWithContext 向 to 指向的 Actor 发起异步 RPC 调用.
// 超时 deadline 从 ctx 获取，若未设置, 使用默认超时时间.
func (s *Service) asyncRPCWithContext(ctx context.Context, from, to ActorUID, params any, cb RPCFunc) error {
	// 检查上下文是否已取消.
	if err := ctx.Err(); err != nil {
		return err
	}

	// 获取dealine.
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(s.cfg.DefRPCTimeout)
	}

	return s.asyncRPCWithDeadline(from, to, params, cb, deadline)
}

// AsyncRPCWithDeadline 向 to 指向的 Actor 发起异步 RPC 调用.
// deadline 指定具体超时时刻.
func (s *Service) AsyncRPCWithDeadline(to ActorUID, params any, cb RPCFunc, deadline time.Time) error {
	return s.asyncRPCWithDeadline(ActorUID{}, to, params, cb, deadline)
}

// AsyncRPCWithTimeout 向 to 指向的 Actor 发起异步 RPC 调用.
// timeout 指定超时间隔.
func (s *Service) AsyncRPCWithTimeout(to ActorUID, params any, cb RPCFunc, timeout time.Duration) error {
	return s.asyncRPCWithTimeout(ActorUID{}, to, params, cb, timeout)
}

// AsyncRPC 向 to 指向的 Actor 发起异步 RPC 调用.
// 超时间隔使用配置的默认值.
func (s *Service) AsyncRPC(to ActorUID, params any, cb RPCFunc) error {
	return s.asyncRPCWithDeadline(ActorUID{}, to, params, cb, time.Now().Add(s.cfg.DefRPCTimeout))
}

// AsyncRPCWithContext 向 to 指向的 Actor 发起异步 RPC 调用.
// 超时 deadline 从 ctx 获取，若未设置, 使用默认超时时间.
func (s *Service) AsyncRPCWithContext(ctx context.Context, to ActorUID, params any, cb RPCFunc) error {
	return s.asyncRPCWithContext(ctx, ActorUID{}, to, params, cb)
}
