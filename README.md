# gactor

`gactor` 是一个基于 Actor 模型的 Go 并发框架，提供 Actor 生命周期管理、同步/异步 RPC、消息投递、处理链、时间轮、客户端通信，以及集群中 Actor 的动态迁移能力。

## 特性

- **Actor 模型**：每个 Actor 拥有独立状态与消息邮箱，业务状态通常无需额外加锁。
- **多种通信方式**：支持同步 RPC、异步 RPC、单向 `Cast` 与请求透传 `Forward`。
- **处理链**：通过 `Context` 与 `HandlersChain` 编排中间件和业务处理函数。
- **定时器与异步回调**：内置时间轮，并支持将外部异步结果安全地投递回 Actor。
- **客户端 Actor**：`CActor` 提供连接生命周期、会话和主动推送能力。
- **可插拔基础设施**：可接入自定义 `ActorRegistry`、`ActorRouter`、`NetAgent`、`PacketCodec`、时间系统和监控器。
- **动态迁移**：支持将一个 Service 中当前运行的 Actor 批量迁移到目标节点，并提供收敛、转发与最终清理机制。

## 核心概念

### Actor 与 CActor

`ActorUID` 由 `Category` 和 `ID` 组成，是 Actor 的唯一标识。Actor 的请求、RPC 回调和定时器回调都会回到其邮箱中串行执行。

业务逻辑实现 `ActorBehavior`：

- `OnStart()`：Actor 成功启动后调用。
- `OnStop(ctx)`：Actor 回收时调用。通过 `ctx.Reason` 区分普通回收、Service 停机和迁移完成后的源端回收。

`CActorBehavior` 在此基础上增加：

- `OnConnected()` / `OnDisconnected()`：客户端连接状态变化回调。
- `CActor.Session()`：获取当前会话。
- `CActor.PushRawMessage(payload)`：向客户端主动推送消息。
- `CActor.Disconnect()`：主动断开当前会话。

`ActorDefineConfig` 与 `CActorDefineConfig` 中的 `PriMessageBoxSize` 和 `MessageBoxSize` 都必须大于 0。`RecycleTime` 大于 0 时，空闲 Actor 会被自动回收；CActor 必须设置有效的回收时间。

```go
type UserActor struct {
	gactor.Actor
}

func (a *UserActor) OnStart() error {
	return nil
}

func (a *UserActor) OnStop(ctx gactor.ActorStopContext) error {
	if ctx.Reason == gactor.ActorStopReasonMigrated {
		// 业务状态已由迁移流程转交目标节点；仅释放本地资源。
		return nil
	}
	// 普通回收或 Service 停机时执行持久化、资源释放等逻辑。
	return nil
}

var UserActorDefine = gactor.NewActorDefine(gactor.ActorDefineConfig{
	Name:              "user",
	Category:          1,
	Priority:          0,
	PriMessageBoxSize: 16,
	MessageBoxSize:    128,
	RecycleTime:       10 * time.Minute,
	BehaviorCreator: func(actor gactor.Actor) gactor.ActorBehavior {
		return &UserActor{Actor: actor}
	},
})
```

### Service

`Service` 是一个节点的运行容器，负责 Actor 启动与回收、定时器驱动、本地或远程消息分发，以及 RPC 管理。

创建 `Service` 前需要提供 `ServiceHandler`，为框架注入以下基础设施：

- `ActorRegistry`：Actor 注册、续约、注销和迁移接管。
- `ActorRouter`：未注册 Actor 的节点选择。
- `NetAgent`：向指定节点发送字节数据。
- `PacketCodec`：业务负载编解码，同时也是 `BytesManager`。
- `TimeSystem`：提供当前时间和时间差。
- `ServiceMonitor`：Actor、定时器、RPC 与 Cast 监控；可选实现 `ServiceMigrationMonitor` 接收迁移事件。

`ServiceConfig` 中 `ActorConfig.Handler`、`TimerConfig.TimeWheelLevels` 和 `Handler` 为必填项。RPC 默认超时为 5 秒，Actor 注册 TTL 默认 60 秒。

```go
svc := gactor.NewService(&gactor.ServiceConfig{
	NodeId: "node-1",
	ActorConfig: gactor.ActorConfig{
		ActorDefines: []gactor.ActorDefine{UserActorDefine},
		Handler:      gactor.NewHandlersChain(BusinessHandler).Handle,
	},
	TimerConfig: gactor.TimerConfig{
		TimeWheelLevels: []gtimewheel.LevelConfig{
			{Name: "second", Span: time.Second, Slots: 60},
			{Name: "minute", Span: time.Minute, Slots: 60},
		},
	},
	RPCConfig: gactor.RPCConfig{
		DefRPCTimeout: 5 * time.Second,
	},
	Handler: myServiceHandler,
})

if err := svc.Start(); err != nil {
	panic(err)
}
defer svc.Stop()

// 显式拉起指定 Actor；若已启动，会检查其仍可用。
uid := gactor.ActorUID{Category: 1, ID: 1001}
if err := svc.StartActor(context.Background(), uid); err != nil {
	panic(err)
}
```

### Context 与处理链

每个请求通过 `Context` 进入 `ActorConfig.Handler`。`HandlersChain` 支持在业务处理前后插入中间件：

```go
func BusinessHandler(ctx *gactor.Context) {
	var req Request
	if err := ctx.Decode(&req); err != nil {
		_ = ctx.ReplyDecodeError()
		return
	}

	_ = ctx.Reply(&Response{Message: "ok"})
}

handler := gactor.NewHandlersChain(
	func(ctx *gactor.Context) {
		start := time.Now()
		ctx.Next()
		log.Printf("handled in %s", time.Since(start))
	},
	BusinessHandler,
).Handle
```

常用 `Context` 方法：

- `Actor()`、`RequestType()`、`FromActorUID()`：读取当前请求信息。
- `Set()` / `Get()`：在处理链中传递元数据。
- `Decode(v)`：解码请求负载；同一个 Context 中不可重复调用。
- `Reply(v)` / `ReplyDecodeError()`：回复请求。
- `Next()` / `Abort()`：推进或终止处理链。
- `Clone()`：复制 Context 供异步场景使用。
- `RPC()`、`AsyncRPC()`、`Cast()`：以当前 Actor 发起通信。

`Context` 的 `RPC()` 和 `AsyncRPC()` 会优先复用当前请求 deadline。`AsyncRPC()` 与 `AsyncCall()` 会挂起处理链，并在回调执行完成后继续后续 Handler；回调中不要将 `Context` 保存到其生命周期之外。

## Actor 通信

Actor、Service 和 Context 都提供 RPC 相关接口。默认接口使用 `RPCConfig.DefRPCTimeout`，需要显式控制时使用 `WithTimeout`、`WithDeadline` 或 `WithContext` 版本。

```go
target := gactor.ActorUID{Category: 2, ID: 2001}
var reply Reply

// 同步 RPC。
err := actor.RPC(target, &Request{Data: "hello"}, &reply)
err = actor.RPCWithTimeout(target, &Request{Data: "hello"}, &reply, 2*time.Second)
err = actor.RPCWithContext(ctx, target, &Request{Data: "hello"}, &reply)

// 异步 RPC；回调仍在发起 Actor 的执行流中运行。
err = actor.AsyncRPC(target, &Request{Data: "hello"}, func(a gactor.Actor, resp gactor.RPCResp) {
	if err := resp.DecodeReply(&reply); err != nil {
		return
	}
})

// Handler 内优先使用 Context 版本，以继承当前请求 deadline。
func Handler(ctx *gactor.Context) {
	var reply Reply
	if err := ctx.RPC(target, &Request{Data: "hello"}, &reply); err != nil {
		return
	}
}

// 单向投递、透传当前请求和 Actor 定时器。
_ = actor.Cast(target, &Message{Data: "event"})
_ = actor.Forward(target, &Message{Data: "forward"})
tid := actor.StartTimer(time.Second, true, "tick", func(args gactor.ActorTimerArgs) {
	// 定时回调在 Actor 邮箱中执行。
})
actor.StopTimer(tid)
```

`Actor.AsyncCall` 可将任意外部异步操作的结果安全地带回 Actor。调用方在超时前执行返回的 `ActorAsyncCaller`；未调用时框架会以 `ErrTimeout` 触发回调。

## Client 使用

`Client` 面向框架外部调用方。创建时自动启动；调用 `Stop()` 停止。`ClientHandler` 需要提供注册表、网络代理、字节池以及响应、推送、断开回调。

```go
client := gactor.NewClient(&gactor.ClientConfig{
	NodeId:        "client-1",
	ActorCategory: 1,
	Handler:       myClientHandler,
})
defer client.Stop()

sid := client.GenSessionId()
if err := client.Connect(1001, sid); err != nil {
	panic(err)
}

if err := client.SendRequest(gactor.ClientRequest{
	ID:      1001,
	SID:     sid,
	Timeout: 5 * time.Second, // 未设置时使用 DefRequestTimeout。
	Payload: []byte("data"),
}); err != nil {
	panic(err)
}

_ = client.Disconnect(1001, sid)
```

`ClientResponse.Payload` 与 `ClientPush.Payload` 是需要由 `ClientHandler` 回收的 `Buffer`。`Client.SendRequest` 不接收 `context.Context`，请求超时由 `ClientRequest.Timeout` 控制。

## 动态迁移

Service 可将当前运行中的 Actor 迁移到一个目标节点。迁移以冻结的 Actor 计划为边界；完成接管的源 Actor 在转发阶段仍保留在源节点，将收到的业务消息转发至目标节点。

如需转移业务状态，Actor 可选实现 `ActorMigrationBehavior`：

```go
func (a *UserActor) OnMigrateOut(ctx gactor.ActorMigrationContext) error {
	// 将快照保存到源、目标节点都能访问的存储中。
	// 使用 ctx.SourceLeaseID 作为幂等键：失败重试可能重复调用。
	return saveSnapshot(ctx.SourceLeaseID, a.state)
}

func (a *UserActor) OnMigrateIn(ctx gactor.ActorMigrationContext) error {
	// 在目标节点完成租约接管后，按同一幂等键恢复状态。
	a.state = loadSnapshot(ctx.SourceLeaseID)
	return nil
}
```

`ActorMigrationContext` 提供 Actor UID、源/目标节点、源租约、目标租约、迁移序号和 CActor 会话副本。`OnMigrateOut` 与 `OnMigrateIn` 都必须具备幂等性，避免自动重试或重复迁移包造成重复副作用。

迁移配置位于 `ServiceConfig.Migration`：

- `MigrationMaxConcurrentActors`：同时等待目标端接管结果的 Actor 数，默认 16。
- `MigrationMaxConcurrentForwards`：源节点同时发送的迁移转发请求数，默认 256。
- `TrustedMigrationNodeIDs`：迁移控制包来源白名单。生产环境建议显式配置。
- `RequireTrustedMigrationNodes`：开启后目标节点与控制包来源必须在白名单内。

典型迁移流程如下：

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

if err := svc.StartMigration("node-2", func(snapshot gactor.ServiceMigrationSnapshot) {
	log.Printf("migration converged: %d actors", snapshot.MigratedActorCount)
}); err != nil {
	panic(err)
}

// 等待冻结计划全部进入 migrated 或 stopped 终态。
if err := svc.WaitMigrationConverged(ctx); err != nil {
	panic(err)
}

// 在上层流量切换完成后停止源端业务转发。
if err := svc.FinishMigrationForwarding(); err != nil {
	panic(err)
}

// 停止源端已迁移 Actor，触发 OnStop(ActorStopReasonMigrated)，并清理迁移状态。
if err := svc.FinalizeMigration(ctx); err != nil {
	panic(err)
}
```

通过 `MigrationSnapshot()` 可查询迁移的目标节点、阶段统计、重试信息、转发统计和每个 Actor 的迁移状态。可选的 `ServiceMigrationMonitor` 会收到迁移开始、重试、接管、转发、收敛和最终清理等事件。

## 项目结构

- `actor.go`：Actor、CActor、生命周期和迁移公共接口。
- `actor_define.go`：Actor 定义及邮箱、优先级、回收配置。
- `context.go` / `handler.go`：请求上下文与处理链。
- `service.go` / `service_actor.go`：Service 生命周期、Actor 管理与分发。
- `service_rpc.go`：同步和异步 RPC 实现。
- `service_migration.go`：动态迁移、收敛、转发和最终清理。
- `client.go`：客户端组件与 `ClientHandler`。
- `internal/examples/c2s`：客户端与服务端通信示例。
- `internal/examples/s2s`：服务端集群通信及迁移行为示例。

## 示例与测试

运行 S2S 示例：

```bash
go run internal/examples/s2s/main.go
```

运行 C2S 示例：

```bash
go run internal/examples/c2s/server/main.go --client-info <nodeId,addr>
go run internal/examples/c2s/client/main.go --node-id <id> --addr <addr>
```

执行全部测试：

```bash
go test ./...
```
