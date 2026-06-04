# gactor

`gactor` 是一个基于 Actor 模型的 Go 并发框架，提供 Actor 生命周期管理、RPC/Cast 通信、请求处理链、定时器系统，以及面向客户端和集群节点的通信能力。

## 特性

- 基于 Actor 模型：每个 Actor 拥有独立状态和消息队列，天然避免共享状态加锁。
- 多种通信方式：支持同步 RPC、异步 RPC 和单向投递 `Cast`。
- 处理链机制：基于 `Context` 与 `HandlersChain` 组织中间件和业务处理逻辑。
- 高性能定时器：内置时间轮，支持单次和周期性任务。
- 优先级调度：支持按 Actor 优先级组织调度。
- 可扩展基础设施：支持自定义 `ActorRegistry`、`ActorRouter`、`NetAgent`、`PacketCodec` 和监控器。
- CActor 支持：内置面向客户端连接的 Actor 形态，支持连接生命周期管理和自动回收。

## 核心概念

### Actor

Actor 是框架中的基本执行单元，具备以下特性：

- 唯一标识：`ActorUID` 由 `Category` 和 `ID` 组成。
- 独立状态：Actor 内部状态由自身串行处理，业务侧通常无需加锁。
- 消息信箱：所有请求、回调、定时器都通过邮箱进入 Actor 执行流。
- 生命周期：
  - `OnStart()`：启动初始化
  - `OnStop()`：停机清理

### CActor

`CActor` 面向客户端连接场景，在普通 Actor 的能力之上增加：

- 会话信息：通过 `Session()` 获取客户端会话。
- 连接回调：`OnConnected()` / `OnDisconnected()`
- 主动推送：`PushRawMessage(...)`
- 自动回收：可通过 `RecycleTime` 配置空闲回收时间

### Service

`Service` 是节点级容器，负责：

- Actor 的创建、查找、调度与停机
- 本地/远端消息分发
- RPC 调用管理
- 定时器驱动
- 网络与编解码协同

### Context 与 HandlersChain

框架使用 `Context` 贯穿单次请求处理流程，可配合 `HandlersChain` 组织中间件和业务逻辑。

常用方法：

- `Next()` / `Abort()`：控制处理链执行
- `Set()` / `Get()`：在处理链中传递元数据
- `Decode(v)`：解码请求负载
- `Reply(v)` / `ReplyDecodeError()`：回复请求
- `RPC()` / `AsyncRPC()`：从当前请求上下文继续发起 RPC

说明：

- `Context.RPC()` / `Context.AsyncRPC()` 默认沿用当前请求的 deadline。
- 如需显式指定 deadline、timeout 或外部 `context.Context`，使用 `RPCWithDeadline`、`RPCWithTimeout`、`RPCWithContext` 及对应的异步版本。

### Client

`Client` 用于外部系统与 Actor 节点通信，支持：

- 连接/断开指定 Actor
- 发送请求
- 接收响应与推送
- 接收断开通知

## 项目结构

- `actor.go`：Actor、CActor、ActorBehavior 等公共接口
- `service.go`：`Service`、`ServiceConfig`、`ServiceHandler`
- `service_actor.go`：Actor 管理与分发逻辑
- `service_rpc.go`：同步/异步 RPC 核心实现
- `client.go`：客户端组件与 `ClientHandler`
- `context.go`：请求上下文与处理链控制
- `internal/examples/c2s`：客户端-服务端示例
- `internal/examples/s2s`：服务端-服务端示例

## 快速开始

### 1. 定义 Actor

```go
type MyActor struct{}

func (a *MyActor) OnStart() error { return nil }
func (a *MyActor) OnStop() error  { return nil }

var MyActorDefine = gactor.NewActorDefine(
	gactor.ActorDefineConfig{
		Name:           "my_actor",
		Category:       1,
		Priority:       0,
		MessageBoxSize: 128,
		BehaviorCreator: func(actor gactor.Actor) gactor.ActorBehavior {
			return &MyActor{}
		},
	},
	gactor.WithMaxCompletedAsyncRPCAmount(8),
)

type MyCActor struct {
	actor gactor.CActor
}

func (a *MyCActor) OnStart() error      { return nil }
func (a *MyCActor) OnStop() error       { return nil }
func (a *MyCActor) OnConnected()        {}
func (a *MyCActor) OnDisconnected()     {}

var MyCActorDefine = gactor.NewCActorDefine(
	gactor.CActorDefineConfig{
		Name:           "my_cactor",
		Category:       2,
		Priority:       0,
		MessageBoxSize: 128,
		RecycleTime:    10 * time.Minute,
		BehaviorCreator: func(actor gactor.CActor) gactor.CActorBehavior {
			return &MyCActor{actor: actor}
		},
	},
)
```

### 2. 实现 ServiceHandler

`Service` 依赖 `ServiceHandler` 提供基础设施组件：

```go
type MyServiceHandler struct {
	registry  gactor.ActorRegistry
	router    gactor.ActorRouter
	netAgent  gactor.NetAgent
	codec     gactor.PacketCodec
	timeSys   gactor.TimeSystem
	monitor   gactor.ServiceMonitor
}

func (h *MyServiceHandler) GetActorRegistry() gactor.ActorRegistry { return h.registry }
func (h *MyServiceHandler) GetActorRouter() gactor.ActorRouter     { return h.router }
func (h *MyServiceHandler) GetNetAgent() gactor.NetAgent           { return h.netAgent }
func (h *MyServiceHandler) GetPacketCodec() gactor.PacketCodec     { return h.codec }
func (h *MyServiceHandler) GetTimeSystem() gactor.TimeSystem       { return h.timeSys }
func (h *MyServiceHandler) GetMonitor() gactor.ServiceMonitor      { return h.monitor }
```

### 3. 创建并启动 Service

```go
func BusinessHandler(ctx *gactor.Context) {
	var req MyRequest
	if err := ctx.Decode(&req); err != nil {
		_ = ctx.ReplyDecodeError()
		return
	}

	_ = ctx.Reply(&MyResponse{Message: "ok"})
}

svc := gactor.NewService(&gactor.ServiceConfig{
	NodeId: "node-1",
	ActorConfig: gactor.ActorConfig{
		ActorDefines: []gactor.ActorDefine{MyActorDefine, MyCActorDefine},
		Handler: gactor.NewHandlersChain(
			func(ctx *gactor.Context) {
				start := time.Now()
				ctx.Next()
				fmt.Printf("handled in %v\n", time.Since(start))
			},
			BusinessHandler,
		).Handle,
	},
	RPCConfig: gactor.RPCConfig{
		DefRPCTimeout: 5 * time.Second,
	},
	Handler: myHandler,
})

if err := svc.Start(); err != nil {
	panic(err)
}
```

### 4. Actor 通信

当前版本中，Actor RPC 已拆分为默认超时、显式 timeout/deadline，以及 `context` 适配接口：

```go
var reply MyReply

// 1. 使用默认 RPC 超时
err := actor.RPC(targetUID, &MyRequest{Data: "hello"}, &reply)

// 2. 指定 timeout
err = actor.RPCWithTimeout(targetUID, &MyRequest{Data: "hello"}, &reply, 2*time.Second)

// 3. 使用 context 作为边界适配
err = actor.RPCWithContext(ctx, targetUID, &MyRequest{Data: "hello"}, &reply)

// 4. 异步 RPC
err = actor.AsyncRPCWithTimeout(
	targetUID,
	&MyRequest{Data: "hello"},
	func(a gactor.Actor, resp *gactor.RPCResp) {
		// 在回调中处理响应
	},
	2*time.Second,
)

// 5. 在 Handler 中继续发起 RPC
func Handler(ctx *gactor.Context) {
	var reply MyReply
	if err := ctx.RPC(targetUID, &MyRequest{Data: "hello"}, &reply); err != nil {
		return
	}
}

// 6. 单向投递
err = actor.Cast(targetUID, &MyMessage{Data: "hello"})

// 7. 定时器
tid := actor.StartTimer(time.Second, true, "tick", func(args *gactor.ActorTimerArgs) {
	// 定时任务逻辑
})
actor.StopTimer(tid)
```

### 5. Client 使用

```go
type MyClientHandler struct {
	registry gactor.ActorRegistry
	netAgent gactor.NetAgent
	bytesMgr gactor.BytesManager
}

func (h *MyClientHandler) GetActorRegistry() gactor.ActorRegistry { return h.registry }
func (h *MyClientHandler) GetNetAgent() gactor.NetAgent           { return h.netAgent }
func (h *MyClientHandler) GetBytesManager() gactor.BytesManager   { return h.bytesMgr }
func (h *MyClientHandler) HandleResponse(resp gactor.ClientResponse) {}
func (h *MyClientHandler) HandlePush(push gactor.ClientPush)         {}
func (h *MyClientHandler) HandleDisconnect(id int64, sid uint32)     {}

client := gactor.NewClient(&gactor.ClientConfig{
	NodeId:        "client-1",
	ActorCategory: 1,
	Handler:       myClientHandler,
})

sid := client.GenSessionId()
if err := client.Connect(1001, sid); err != nil {
	panic(err)
}

if err := client.SendRequest(gactor.ClientRequest{
	ID:      1001,
	SID:     sid,
	Timeout: 5 * time.Second,
	Payload: []byte("data"),
}); err != nil {
	panic(err)
}
```

## 示例

- `internal/examples/c2s`：演示客户端与服务端 Actor 通信
- `internal/examples/s2s`：演示服务端节点之间的 Actor 通信

运行示例：

```bash
go run internal/examples/s2s/main.go
```

```bash
go run internal/examples/c2s/server/main.go --client-info <nodeId,addr>
```

```bash
go run internal/examples/c2s/client/main.go --node-id <id> --addr <addr>
```

## 测试

```bash
go test ./...
```
