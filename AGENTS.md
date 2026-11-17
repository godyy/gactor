# gactor 项目开发指南 (AGENTS)

> 本文件面向在本仓库工作的 AI/开发者，用于快速理解 gactor 的架构、公开接口与编码约定。
> 文档内容以当前源码为准；修改公开接口后必须同步更新本文件与 README。

## 1. 项目概览

gactor 是基于 Actor 模型的 Go 并发框架（module `github.com/godyy/gactor`，Go 1.24）。

设计目标：**让业务以“单线程”方式组织并发状态，同时保留跨节点通信能力。**

- **状态自持、串行执行**：每个 Actor 拥有独立状态与邮箱，发往同一 Actor 的请求、RPC 回调、定时器回调均在其邮箱中串行执行，业务状态通常无需加锁。
- **消息驱动**：Actor 之间不共享内存，只通过 `RPC`（有回复、有超时）、`Cast`（单向）、`Forward`（透传来源）通信；接口屏蔽本地与远端差异。
- **运行时与业务分离**：框架提供邮箱、时间轮、RPC 管理、网络分发等运行时能力；业务只实现 `ActorBehavior`，通过 `ActorDefine.BehaviorCreator` 绑定。
- **可插拔基础设施**：注册表、路由、网络、编解码、时间系统、监控均为接口，由 `ServiceHandler` / `ClientHandler` 注入。
- **动态迁移**：支持把节点上的 Actor 迁移到其它节点，以“冻结迁移计划”为边界，具备幂等与可观测性。

为提升吞吐与降低 GC 压力，框架内部大量使用对象池（`sync.Pool`）、命令队列串行化与非并发安全的组件封装（如 `rpcManager` / `timerManager` / `ackManager`）。

## 2. 目录与文件结构

### 根目录（框架核心）

| 文件 | 说明 |
| --- | --- |
| `actor.go` | `ActorUID`、`Actor`、`CActor`、`ActorBehavior`、`CActorBehavior`、`ActorMigrationBehavior`、停机/迁移上下文与各种回调类型 |
| `actor_define.go` | `ActorDefine`、`ActorDefineConfig`、`CActorDefineConfig`、`NewActorDefine`、`NewCActorDefine`、定义集合与优先级索引 |
| `actor_impl.go` | Actor 运行时实现（`actor`、`cactor`、`actorCore`）、邮箱与消息 `handle` |
| `actor_migration.go` | 源端 Actor 迁移状态机 |
| `context.go` | `Context`、HandlerChain 执行、`ContextSuspender`、Context 版 RPC/AsyncRPC/Cast/AsyncCall/Clone |
| `handler.go` | `HandlerFunc`、`HandlersChain`、`NewHandlersChain` |
| `request.go` | 请求抽象（RPC/Cast/Raw Request 的统一 `request`）与回复 |
| `service.go` | `ServiceHandler`、`ServiceConfig`、`Service` 生命周期（`Start`/`Stop`）与编解码/发送基础设施 |
| `service_actor.go` | `ActorConfig`、Actor 创建/启动/回收/寻址、`Service.StartActor`、`Service.Cast` |
| `service_rpc.go` | `RPCConfig`、`Service.RPC*` / `AsyncRPC*` 与内部 `rpcManager` 入口 |
| `service_forward.go` | `Service.Forward` 与透传实现 |
| `service_timer.go` | `Service.StartTimer` / `StopTimer` |
| `service_packet_handler.go` | `Service.HandlePacket` 及各类数据包处理 |
| `service_migration.go` | `MigrationConfig`、迁移公开 API（`StartMigration*` / `WaitMigrationConverged` / `FinishMigrationForwarding` / `FinalizeMigration` / `MigrationSnapshot`）与迁移状态 |
| `service_monitor.go` | `ServiceMonitor`、`ServiceMigrationMonitor` 与各 `Monitor*Action` 枚举 |
| `service_ack.go` / `ack.go` | `AckConfig` 与 `ackManager`（可靠投递重传） |
| `service_option.go` | `ServiceOption`（`WithServiceLogger` / `WithServiceAckManager`） |
| `rpc.go` | `RPCResp`、`RPCFunc`、`rpcManager` 及 RPC 调用/超时/回调实现 |
| `timer.go` | `TimerId`、`TimerArgs`、`TimerConfig`、`timerManager`（封装 gtimewheel） |
| `packet.go` | `PacketType` 枚举、`PacketHead`、`PacketCodec`、`PacketBufAllocator` 及各类型包头 |
| `net.go` | `NetAgent`、`ErrNetworkBusy` |
| `actor_location.go` | `ActorRegistry`、`ActorRouter` 及其参数/结果类型与错误 |
| `error.go` | `ErrCode` 枚举、`ErrTimeout`、`Err2ErrCode` |
| `bytes.go` | `BytesManager`、`Buffer`、`NewBuffer`、缓冲池 |
| `cbctx.go` | 回调上下文 `CbCtx`（`NewCbCtx` / `CbCtxRelease`） |
| `time.go` | `TimeSystem` 与 `DefTimeSystem` |
| `client.go` | `ClientRequest`/`Response`/`Push`、`ClientHandler`、`ClientConfig`、`Client` |
| `client_option.go` | `ClientOption`（`WithClientLogger` / `WithClientAckManager`） |
| `client_ack.go` / `client_packet_handler.go` | 客户端 Ack 回调与 `Client.HandlePacket` |
| `log.go` / `utils.go` / `message.go` | 日志封装、内部工具与消息抽象 |

### 其它目录

| 目录 | 说明 |
| --- | --- |
| `internal/utils` | 并发工具 `ConcurrentMap` 与停机管理 `StopWait` |
| `internal/examples/c2s` | 客户端-服务端示例（`client` / `server` / `common`） |
| `internal/examples/s2s` | 服务端集群示例，包含 `actors/migration.go`（迁移行为样例） |
| `*_test.go`（根目录） | 单元测试与端到端测试，部分测试内含可直接参考的最小 `ServiceHandler` 实现 |

## 3. 核心概念与接口

### 3.1 ActorUID

`ActorUID` 由 `Category`（`uint16`）与 `ID`（`int64`）组成，是所有通信接口的寻址单位。

```go
uid := gactor.ActorUID{Category: 1, ID: 1001}
uid.String() // "1-1001"
```

`Category` 与 `ActorDefine.Category` 对应；`ID` 为分类内唯一实例 ID。

### 3.2 Actor 运行时接口

`Actor` 是框架提供给业务使用的运行时接口（业务结构体通常内嵌它）：

| 方法 | 说明 |
| --- | --- |
| `Category()` / `ActorUID()` / `Behavior()` | 分类、唯一标识与行为实例 |
| `StartTimer(d, periodic, cb, cbctx)` / `StopTimer(tid)` | 定时器管理 |
| `RPC(to, params, reply)` / `AsyncRPC(to, params, cb, cbctx)` | 默认超时调用 |
| `RPCWithTimeout` / `AsyncRPCWithTimeout` | 显式超时间隔 |
| `RPCWithDeadline` / `AsyncRPCWithDeadline` | 显式超时时刻 |
| `RPCWithContext` / `AsyncRPCWithContext` | 适配外部 `context.Context` |
| `Cast(to, payload)` | 单向投递 |
| `Forward(to, payload)` | 透传当前请求来源 |
| `AsyncCall(f, timeout)` | 把外部异步结果安全带回 Actor 邮箱，返回 `ActorAsyncCaller` |

`CActor` 在 `Actor` 之上增加会话能力：

- `Session() ActorSession`：返回 `NodeId` + `SID`。
- `PushRawMessage(payload) error`：向客户端主动推送。
- `Disconnect()`：主动断开当前会话。

### 3.3 行为接口

业务需实现 `ActorBehavior`（普通 Actor）或 `CActorBehavior`（客户端 Actor）：

```go
type ActorBehavior interface {
    OnStart() error
    OnStop(ctx ActorStopContext) error
}

type CActorBehavior interface {
    ActorBehavior
    OnConnected()
    OnDisconnected()
}
```

`ActorStopContext.Reason`（`ActorStopReason`）区分停机原因：

| 枚举 | 含义 |
| --- | --- |
| `ActorStopReasonNormal` | 普通回收或异常终止 |
| `ActorStopReasonShutdown` | Service 正在停机 |
| `ActorStopReasonMigrated` | 源 Actor 迁移完成后的本地回收（业务状态已转交目标节点） |

需要迁移业务状态时，可选实现 `ActorMigrationBehavior`：

```go
type ActorMigrationBehavior interface {
    OnMigrateOut(ctx ActorMigrationContext) error // 迁移发起前，按 SourceLeaseID 保存快照
    OnMigrateIn(ctx ActorMigrationContext) error  // 目标端租约接管后，按 SourceLeaseID 恢复快照
}
```

`ActorMigrationContext` 提供 `UID`、`SourceNodeID`、`SourceLeaseID`、`TargetNodeID`、`TargetLeaseID`（仅 `OnMigrateIn` 有效）、`StartSeq` 与 CActor 会话副本。
**两个回调都必须幂等**：迁移失败会自动重试，重复迁移包也可能到达。推荐以 `SourceLeaseID` 作为快照读写与外部副作用的幂等键。

### 3.4 ActorDefine

`ActorDefine` 描述 Actor 的元数据与行为工厂，必须在 `Service` 启动前注册。

| 配置字段 | 说明 |
| --- | --- |
| `Name` / `Category` | 名称与分类；`Category` 决定 `ActorUID.Category`，同名分类不可重复 |
| `Priority` / `PriMessageBoxSize` | 优先级与优先级信箱大小；数值越小优先级越高 |
| `MessageBoxSize` | 普通信箱大小 |
| `RecycleTime` | 空闲回收时间；`> 0` 超时自动回收，`0` 表示常驻（**CActor 必须 > 0**） |
| `BehaviorCreator` | 行为工厂，每次创建返回一个新的 Behavior 实例（不能为空） |

约束（`init()` 校验，违反会 panic）：`Name` 非空、`PriMessageBoxSize > 0`、`MessageBoxSize > 0`、`BehaviorCreator != nil`、CActor 的 `RecycleTime > 0`。

- 普通 Actor：`NewActorDefine(ActorDefineConfig{...})`
- 客户端 Actor：`NewCActorDefine(CActorDefineConfig{...})`

### 3.5 Service 与配置

`Service` 是一个节点的运行容器，负责 Actor 生命周期、定时器驱动、消息分发与 RPC 管理。通过 `NewService(cfg, options...)` 创建，`Start()` / `Stop()` 控制生命周期。

`ServiceHandler`（`ServiceConfig.Handler`，required）需要提供六类基础设施：

| 接口 | 职责 |
| --- | --- |
| `ActorRegistry` | Actor 注册、续约、注销与迁移接管（`MakeLeaseID`/`RegisterActor`/`TakeOverActor`/`UnregisterActor`/`KeepActorAlive`/`GetActorLocation`） |
| `ActorRouter` | 未注册 Actor 的节点选择（`PickActorNode`） |
| `NetAgent` | 向指定节点发送字节（`Send2Node`） |
| `PacketCodec` | 业务负载编解码，同时实现 `BytesManager` |
| `TimeSystem` | `Now()` / `Until(t)`；通常返回 `DefTimeSystem` |
| `ServiceMonitor` | Actor/定时器/RPC/Cast 计数上报；可返回 `nil` |

`ServiceConfig` 主要字段与默认值：

| 字段 | 说明 | 默认值 |
| --- | --- | --- |
| `NodeId` | 节点 ID | required |
| `ActorConfig` | 见下 | required |
| `TimerConfig` | 见下 | required |
| `RPCConfig` | `DefRPCTimeout` / `MaxRPCCallAmount` | `5s` / `1000` |
| `Handler` | `ServiceHandler` | required |
| `MaxRTT` | 最大网络延迟（ms），用于衰减网络 RTT 引起的超时 | `50` |
| `QueueWriteTimeAlarmThreshold` | 队列写入耗时告警阈值 | `50ms` |
| `Migration` | `MigrationConfig`，见 §4.4 | 各并发上限 `256` |

`ActorConfig`：`ActorDefines`（required）、`ClientActorCategory`（>0 时必须已在 ActorDefines 中定义）、`RegistryTTL`（默认 `60`s）、`KeepAliveInterval`（默认 `RegistryTTL/3`，必须 `< RegistryTTL`）、`Handler`（`HandlerFunc`，required）。

`TimerConfig`：`TimeWheelLevels`（`[]gtimewheel.LevelConfig`，required）、`MaxTimerAmount`（默认 `1000`）。

`ServiceOption`：`WithServiceLogger(logger)`、`WithServiceAckManager(cfg)`。

### 3.6 Context 与 HandlersChain

每个请求（RPC / Cast / Raw Request）都通过 `*Context` 进入 `ActorConfig.Handler`。`HandlersChain` 支持中间件（在业务前后插入逻辑）：

```go
handler := gactor.NewHandlersChain(middleware, businessHandler).Handle
```

`Context` 常用方法：

| 方法 | 说明 |
| --- | --- |
| `Actor()` / `RequestType()` / `FromActorUID()` | 读取当前请求信息 |
| `Set()` / `Get()` | 经内嵌 `kv.M` 在链路中传递元数据 |
| `Decode(v)` | 解码请求负载；**同一 Context 中不可重复调用** |
| `Reply(v)` / `ReplyDecodeError()` | 回复请求 |
| `Next()` / `Abort()` | 推进 / 终止处理链 |
| `Clone()` | 复制 Context 供异步场景使用 |
| `RPC()` / `AsyncRPC()` / `Cast()` | 以当前 Actor 发起通信 |
| `AsyncCall(f, timeout)` | 异步调用并挂起处理链 |

要点：

- `Context.RPC()` 优先复用当前请求 deadline；`AsyncRPC()` 会**挂起**处理链，回调中通过 `resumeAndNext()` 自动继续后续 Handler。
- 异步回调中不要将 `Context` 保存到其生命周期之外；需要跨生命周期时先 `Clone()`。
- `Context` 同时实现 `context.Context`（`Deadline()` 返回请求 deadline，`Done()`/`Err()`/`Value()` 为空实现）。

### 3.7 网络与扩展点

- `NetAgent.Send2Node(nodeId, b)`：底层发送，网络繁忙返回 `ErrNetworkBusy`。
- `PacketCodec`：`Encode(alloc, ph, payload)`、`EncodePayload(pt, payload)`、`DecodePayload(pt, b, v)`；同时提供 `GetBytes` / `PutBytes`。
- `ActorRegistry`：注册/续约/注销/接管/查询；`TakeOverActor` 必须基于 `SourceLeaseId` 原子接管，不匹配返回 `ErrLeaseMismatch`。
- `ActorRouter.PickActorNode(uid)`：无可用节点返回 `ErrNoAvailableNode`。
- 网络层收到字节后回调业务，业务转交 `Service.HandlePacket(nodeId, b)`。

### 3.8 客户端 Client

`Client` 代表框架外部调用方，连接服务端 `CActor`，创建时自动启动，`Stop()` 停机（阻塞）。`ClientHandler` 需提供 `GetActorRegistry`、`GetNetAgent`、`GetBytesManager` 及 `HandleResponse`、`HandlePush`、`HandleDisconnect`。

`ClientConfig`：`NodeId`（required）、`ActorCategory`（>0，required）、`DefCtxTimeout`（默认 `5s`）、`DefRequestTimeout`（默认 `5s`）、`Handler`（required）。

```go
sid := client.GenSessionId()
_ = client.Connect(id, sid)                          // 连接目标 Actor
_ = client.SendRequest(gactor.ClientRequest{         // 超时由 req.Timeout 控制
    ID: id, SID: sid, Timeout: 5 * time.Second, Payload: data,
})
_ = client.Disconnect(id, sid)
```

`ClientResponse.Payload` / `ClientPush.Payload` 为 `*Buffer`，由 `ClientHandler` 负责回收。

### 3.9 其它设施

- **Ack 可靠投递**：`AckConfig{MaxPacketAmount, Timeout, MaxRetry, TickInterval, QueueWriteTimeAlarmThreshold}`，通过 `WithServiceAckManager` / `WithClientAckManager` 启用；未启用时不做确认。
- **CbCtx**：`NewCbCtx(pairs...)` / `CbCtxRelease(km)`，随异步 RPC、定时器携带键值，回调中通过 `RPCResp.Ctx()` / `ActorTimerArgs.Ctx` 取回。
- **Buffer / BytesManager**：`Buffer` 来自缓冲池，使用完必须 `Release()` 归还；`Service` / `Client` 内部通过 `PacketCodec` 或 `BytesManager` 的 `GetBytes`/`PutBytes` 复用底层字节。
- **错误码**：`ErrCode` 覆盖编解码、服务状态、Actor 状态与迁移状态；`Err2ErrCode(err)` 将 error 归一为错误码。

## 4. 关键流程

### 4.1 消息处理

`Service.HandlePacket(nodeId, b)` → **解码包头 (PacketHead)** → **创建 request 并绑定 Context** → **投递到目标 Actor 邮箱** → **执行 HandlersChain** → **业务逻辑** → **Reply/Cast 编码** → **发送 (Send2Node)**

### 4.2 RPC 调用

`Actor/Context/Service RPC` → 由 `ActorRegistry` 解析目标节点（未注册则 `ActorRouter` 选节点）→ `rpcManager` 创建 `rpcCall`（堆按过期时间排序）→ 本地投递或远端发送 → 目标 Actor 回复 → `rpcManager` 触发回调或补全响应 → 超时则以 `ErrTimeout` 完成回调。

### 4.3 定时器

`Actor.StartTimer` / `Service.StartTimer` → `timerManager` 串行入队 → 时间轮 `AddTimer` → 周期推进 `Tick` → 到期回调入队并由 worker 执行 → 回调在调用方 Actor 邮箱（或 Service 侧）串行执行；周期定时器自动重排。

### 4.4 动态迁移

`Service` 可把本节点 Actor 迁移到目标节点，以**冻结的迁移计划**为边界（开始前用规划函数为每个 Actor 确定目标，后续重试与转发复用该结果）。

三阶段：

1. **接管**：源 Actor 触发 `OnMigrateOut` 保存快照 → 目标节点拉起 Actor 执行 `OnMigrateIn` 恢复状态并接管租约。
2. **转发**（`migrated` 态）：源 Actor 仍驻留源节点，把后续业务消息（请求/Cast/连接/断连）转发到目标节点，避免流量切换期丢消息。
3. **清理**：流量切换完成后结束转发并停止源端 Actor，触发 `OnStop(ActorStopReasonMigrated)`。

公开 API：

```go
_ = svc.StartMigration("node-2", cb)              // 单目标
_ = svc.StartMigrationWithPlanner(planner, cb)    // 多目标（扩容分散）
_ = svc.WaitMigrationConverged(ctx)               // 等待冻结计划收敛
_ = svc.FinishMigrationForwarding()               // 结束源端转发
_ = svc.FinalizeMigration(ctx)                    // 停止源端已迁移 Actor 并清理迁移状态
snapshot := svc.MigrationSnapshot()               // 观测迁移进度
```

`MigrationConfig`（默认并发上限均为 `256`）：`MaxConcurrentActors`（源端等待结果并发）、`MaxConcurrentInboundActors`（目标端接管并发）、`MaxConcurrentForwards`（源端转发并发）。

`ActorMigrationPhase`：`pending` / `draining` / `waiting_for_slot` / `migrating_out` / `retrying` / `migrated` / `stopped`（后两者为终态）。
`ServiceMigrationSnapshot` 中单目标计划读 `TargetNodeID`；多目标计划该字段为空，应遍历 `Actors` 中每个 Actor 的 `TargetNodeID`。

## 5. 开发指南

### 5.1 新增 Actor

1. 定义行为结构体，嵌入 `gactor.Actor`（客户端 Actor 嵌入 `gactor.CActor`），实现 `OnStart` / `OnStop`（客户端再加 `OnConnected` / `OnDisconnected`；需迁移则实现 `OnMigrateOut` / `OnMigrateIn`）。
2. 用 `NewActorDefine` / `NewCActorDefine` 创建定义并配置 `BehaviorCreator`。
3. 在 `ServiceConfig.ActorConfig.ActorDefines` 中注册定义。
4. 编写 Handler，处理 `Context` 中的请求，通过 RPC / Cast / Forward 与其它 Actor 交互。

### 5.2 实现 ServiceHandler / ClientHandler

参考 [README.md](/Users/godyy/work/go/godyy/gactor/README.md) §2 的最小可编译样板与 [internal/examples/s2s](/Users/godyy/work/go/godyy/gactor/internal/examples/s2s)（完整实现）。

### 5.3 启动与停机

- `svc.Start()` 启动；`svc.Stop()` **阻塞**直到所有内部协程退出，属一次性生命周期（`init → started → stopping → stopped`，停机后不可再 `Start`）。
- 停机时按优先级分层停机 Actor；未处理的普通消息以 `ErrCodeServiceStopped` 回复，已挂起的 `AsyncRPC`/`AsyncCall` 以停机错误完成。
- **停机不等同于迁移收敛**：停机时等待迁移收敛的调用方会被唤醒并返回停机错误。
- `Client.Stop()` 同样阻塞；停机后相关接口返回 `ErrClientStopped`。

## 6. 接口使用约定与常见陷阱

- **RPC 接口选择**：常规用 `RPC(...)` / `AsyncRPC(...)`；需显式超时用 `WithTimeout` / `WithDeadline`；仅在对接外部 `context.Context` 时用 `WithContext`。
- **Handler 中优先用 `Context.RPC()` / `Context.AsyncRPC()`**：可继承当前请求 deadline 并支持挂起处理链。不要在业务代码中把 `Context` 当作普通 `context.Context` 传给 `actor.RPC ctx` 形式（旧风格已废弃，见 §6 末）。
- **`Context.Decode` 不可重入**：异步复用 Payload 前先 `Clone()`。
- **Buffer 回收**：`ClientResponse.Payload` / `ClientPush.Payload` / `RPCResp` 内部缓冲由框架回收；业务侧从 `PacketCodec` / `BytesManager` 取用的 `Buffer` 必须 `Release()`。
- **CbCtx 归还**：使用 `NewCbCtx` 后应在回调结束处 `CbCtxRelease`（框架对内部的 `Context.AsyncRPC` 已自动处理）。
- **迁移幂等**：`OnMigrateOut` / `OnMigrateIn` 必须可重复调用而不产生重复副作用。
- **构造 error**：需要嵌套上下文错误时使用 `pkgerrors`（`github.com/pkg/errors`）的 `WithMessage` / `WithMessagef` / `Wrap` 系列，而不是直接 `fmt.Errorf`（示例见 `packet.go`、`service.go`）。
- **注释**：新增/修改代码应补充简洁明了的中文注释，说明意图与关键约束，而非逐行翻译。
- **废弃写法**：文档与代码中都不应再出现 `actor.RPC(ctx, ...)`、`actor.AsyncRPC(ctx, ...)`、`actor.Cast(ctx, ...)`、`client.SendRequest(ctx, ...)`。

## 7. 示例与测试

```bash
# S2S 示例（两个节点演示集群通信与迁移）
go run internal/examples/s2s/main.go

# C2S 示例
go run internal/examples/c2s/server/main.go --client-info <nodeId,addr>
go run internal/examples/c2s/client/main.go --node-id <id> --addr <addr>

# 全部测试
go test ./...
```
