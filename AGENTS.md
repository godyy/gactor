# gactor 项目开发指南 (AGENTS)

## 1. 项目概览
gactor 是一个基于 Actor 模型的 Go 语言高性能并发框架。它旨在简化并发编程，提供无锁的 Actor 状态管理、高效的消息传递机制以及灵活的扩展能力。

**核心特性：**
- **Actor 模型**：每个 Actor 拥有独立状态和消息队列，无需加锁。
- **通信机制**：支持同步/异步 RPC、单向消息（Cast）、以及客户端-服务端（C2S）和服务端-服务端（S2S）通信。
- **中间件机制**：基于 `Context` 和 `HandlersChain` 的请求处理管道，易于扩展。
- **时间轮定时器**：内置高性能时间轮，支持单次和周期性任务。
- **高扩展性**：支持自定义注册表（Registry）、路由（Router）、网络代理（NetAgent）和编解码器（PacketCodec）。

## 2. 目录结构说明

| 目录/文件 | 说明 |
| --- | --- |
| `/` (根目录) | 包含框架核心接口与实现 (Actor, Service, RPC, Packet, Context, Client) |
| `internal/examples/c2s` | 客户端-服务端 (C2S) 通信示例，包含 common, client, server |
| `internal/examples/s2s` | 服务端集群 (S2S) 通信示例 |
| `internal/utils` | 并发工具 (ConcurrentMap) 与停机管理 (StopWait) |
| `actor.go` | Actor 接口 (Runtime) 与 Behavior 接口 (User Logic) 定义 |
| `service.go` | Service 主体，负责 Actor 管理与消息分发 |
| `rpc.go` | RPC 调用相关的接口定义 |
| `packet.go` | 网络包协议定义与编解码接口 |

## 3. 关键组件与架构

### 3.1 Actor 体系
gactor 将 Actor 分为 **底层运行时 (Runtime)** 和 **用户行为 (Behavior)** 两层：

- **Actor (Runtime Interface)**: 框架提供的操作接口。
  - 提供 `RPC`, `AsyncRPC`, `Cast` 等通信方法。
  - 提供 `StartTimer`, `StopTimer` 等定时器管理方法。
  - 每个 Actor 实例都有唯一的 `ActorUID`。
- **ActorBehavior (User Logic)**: 用户需实现的业务逻辑接口。
  - `OnStart()`: Actor 启动初始化。
  - `OnStop()`: Actor 销毁资源清理。
- **CActorBehavior**: 面向客户端的 Actor 行为接口，继承自 `ActorBehavior`。
  - 增加 `OnConnected()`, `OnDisconnected()` 回调。
- **ActorDefine**: 定义 Actor 的元数据与配置。
  - 包含名称、类别 (Category)、优先级、邮箱大小、自动回收时间等。
  - 通过 `BehaviorCreator` 工厂函数将用户定义的 Behavior 绑定到框架 Actor。

### 3.2 Service (服务端容器)
Service 是整个节点的核心容器，负责：
- **生命周期管理**: 启动、停止 Actor，管理 Actor 注册表。
- **消息分发**: 接收网络包，解码并路由到目标 Actor。
- **时间轮**: 管理所有 Actor 的定时任务。
- **RPC 管理**: 处理发出的 RPC 请求及其回调。

### 3.3 Context 与 HandlerChain
请求处理采用类似 Web 框架的中间件模式：
- **Context**: 贯穿请求处理全流程，用于传递参数、解码请求、发送响应。
- **HandlerChain**: 允许在处理具体业务逻辑前/后插入通用逻辑 (如鉴权、日志、监控)。

### 3.4 网络与扩展
框架抽象了网络层，允许接入不同的网络库 (如 gnet, net, websocket 等)：
- **NetAgent**: 网络代理接口，负责底层的 Send/Receive。
- **PacketCodec**: 协议编解码接口，允许自定义包格式。
- **ActorRegistry/Router**: 负责 Actor 的定位与寻址 (本地或远程)。

## 4. 核心流程分析

### 4.1 消息处理流程
`Service.HandlePacket` -> **解码 (Decode)** -> **创建 Request** -> **绑定 Context** -> **执行 HandlerChain** -> **业务逻辑** -> **编码响应 (Reply)** -> **发送 (Send)**

### 4.2 定时器流程
`Actor.StartTimer` -> **Service 时间轮注册** -> **时间到期触发** -> **回调 Actor 消息队列** -> **执行用户定义的回调函数**

## 5. 开发与扩展指南

### 5.1 如何新增 Actor
1. **定义 Actor Behavior**: 定义结构体并实现 `ActorBehavior` (普通 Actor) 或 `CActorBehavior` (客户端 Actor) 接口。
2. **创建 ActorDefine**: 使用 `gactor.NewActorDefine` 或 `gactor.NewCActorDefine` 创建配置。
   - 配置 `BehaviorCreator` 工厂函数，返回新的 Behavior 实例。
   - 设置 Category, Priority, MessageBoxSize 等参数。
3. **注册 Actor**: 在 `Service` 启动前通过 `AddActorDefine` 注册 Actor 定义。
4. **编写 Handler**: 实现消息处理逻辑，通过 RPC 或 Cast 与 Actor 交互。

### 5.2 扩展点实现
若需集成到现有系统，通常需要实现以下接口：
- **ServiceHandler**: 提供 Registry, Router, NetAgent, PacketCodec 等组件。
- **ClientHandler**: 客户端侧的 Registry, NetAgent 实现。

### 5.3 注意事项
- **线程安全**: Actor 内部状态是线程安全的，但在回调中访问外部共享资源需注意。
- **Context 使用**: `request.Decode` 不可重入。若需异步使用 Payload，请调用 `Context.Clone()`。
- **资源回收**: 注意配置 `RecycleTime` 以自动回收空闲的 CActor。

## 6. 示例运行

### 6.1 C2S (Client-Server) 示例
**服务端**:
```bash
go run internal/examples/c2s/server/main.go --client-info <nodeId,addr>
```
**客户端**:
```bash
go run internal/examples/c2s/client/main.go --node-id <id> --addr <addr>
```

### 6.2 S2S (Server-Server) 示例
```bash
go run internal/examples/s2s/main.go
```
该示例会启动两个节点演示集群间通信。

## 7. 测试
项目包含单元测试，可通过标准 Go 命令运行：
```bash
go test ./...
```
