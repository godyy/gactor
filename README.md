# gactor

gactor是一个基于Actor模型的Go语言并发框架，提供了高性能、可扩展的并发编程解决方案。

## 特性

- **基于Actor模型**：每个Actor拥有独立状态和消息队列，天然无锁。
- **通信机制**：支持同步/异步RPC调用，以及单向消息投递（Cast）。
- **请求处理管道**：基于 `Context` 和 `HandlersChain` 的中间件机制，灵活处理请求。
- **定时器系统**：内置高性能时间轮，支持单次和周期性定时任务。
- **优先级管理**：支持Actor优先级调度，高优先级Actor优先处理。
- **网络通信**：内置网络代理接口，支持集群间Actor通信。
- **可扩展性**：支持自定义Actor注册表（Registry）、路由（Router）和编解码器（PacketCodec）。

## 核心概念

### Actor

Actor是gactor框架中的基本执行单元，每个Actor都有：

- **唯一标识**：`ActorUID`，由 `Category`（类别）和 `ID`（实例ID）组成。
- **独立状态**：Actor内部状态线程安全。
- **消息信箱**：缓冲待处理的消息。
- **生命周期**：
    - `OnStart`: Actor启动时初始化。
    - `OnStop`: Actor销毁时清理资源。

### CActor (Client Actor)

CActor是专门面向客户端连接的Actor，除了具备普通Actor特性外，还负责：

- **连接管理**：维护客户端连接状态 (`OnConnected`, `OnDisconnected`)。
- **消息推送**：支持向客户端推送消息。
- **自动回收**：支持配置空闲回收时间 (`RecycleTime`)。

### Service

Service是服务端容器，负责：
- 管理Actor生命周期（创建、调度、回收）。
- 消息分发与路由。
- 管理时间轮和定时器。
- 提供网络通信能力。

### Context 与 中间件

gactor 使用 `Context` 对象贯穿整个请求处理流程。类似于 Web 框架，你可以通过 `HandlersChain` 组装多个 Handler，实现鉴权、日志、监控等中间件逻辑。

`Context` 提供的方法：
- `Next()` / `Abort()`: 控制处理链的执行。
- `Set(k, v)` / `Get(k)`: 在处理链中传递元数据。
- `Decode(v)`: 解码请求负载。
- `Reply(v)`: 回复 RPC 请求。

### Client

Client是客户端组件，用于与服务端Actor交互，支持：
- **发送请求**：向指定 Actor 发送 RPC 请求。
- **接收响应**：处理 RPC 响应数据。
- **接收推送**：处理服务端主动推送的消息。
- **断开通知**：通知服务端断开连接。

## 快速开始

### 1. 定义 Actor

```go
// 定义Actor行为
type MyActor struct {
    // 可以在此保存Actor状态
}

func (a *MyActor) OnStart() error {
    return nil
}

func (a *MyActor) OnStop() error {
    return nil
}

// 定义Actor配置
var MyActorDefine = gactor.NewActorDefine(
    gactor.ActorDefineConfig{
        Name:           "MyActor",
        Category:       1,
        Priority:       0,
        MessageBoxSize: 100,
        BehaviorCreator: func(actor gactor.Actor) gactor.ActorBehavior {
            return &MyActor{}
        },
    },
    // 高级配置选项
    gactor.WithMaxCompletedAsyncRPCAmount(10), // 最大并发异步RPC回调数
    gactor.WithMaxTriggeredTimerAmount(10),    // 最大并发定时器触发数
)

// 定义CActor配置 (支持自动回收)
var MyCActorDefine = gactor.NewCActorDefine(
    gactor.CActorDefineConfig{
        Name:           "MyCActor",
        Category:       2,
        RecycleTime:    10 * time.Minute, // 空闲10分钟后自动回收
        BehaviorCreator: func(actor gactor.CActor) gactor.CActorBehavior {
            return &MyCActor{actor: actor}
        },
    },
)
```

### 2. 实现 ServiceHandler

你需要实现 `ServiceHandler` 接口来提供基础设施组件。

```go
type MyServiceHandler struct {
    registry    gactor.ActorRegistry // Actor注册表 (如 Redis/Etcd)
    router      gactor.ActorRouter   // Actor路由策略
    netAgent    gactor.NetAgent      // 网络通信代理
    packetCodec gactor.PacketCodec   // 数据包编解码器
    timeSystem  gactor.TimeSystem    // 时间系统
    monitor     gactor.ServiceMonitor // 监控指标收集 (可选)
}

// ... 实现接口方法 GetActorRegistry, GetActorRouter 等 ...
```

### 3. 创建并启动 Service

```go
// 业务逻辑 Handler
func BusinessHandler(ctx *gactor.Context) {
    switch ctx.RequestType() {
    case gactor.RequestTypeRPC:
        var req MyReq
        if err := ctx.Decode(&req); err != nil {
            ctx.ReplyDecodeError()
            return
        }
        // 处理请求...
        ctx.Reply(&MyResp{Data: "ok"})
    }
}

// 中间件示例：日志记录
func LoggingMiddleware(ctx *gactor.Context) {
    start := time.Now()
    ctx.Next() // 执行下一个 Handler
    fmt.Printf("Request processed in %v\n", time.Since(start))
}

// 创建 Service 配置
config := &gactor.ServiceConfig{
    NodeId: "node-1",
    // Actor 相关配置
    ActorConfig: gactor.ActorConfig{
        ActorDefines: []gactor.ActorDefine{MyActorDefine},
        // 组装 Handler Chain
        Handler: gactor.NewHandlersChain(
            LoggingMiddleware,
            BusinessHandler,
        ).Handle,
    },
    // 其他高级配置
    MaxRTT:        50,               // 网络延迟补偿
    DefCtxTimeout: 5 * time.Second,  // 默认超时
    Handler:       NewMyServiceHandler(),
}

// 启动 Service
service := gactor.NewService(config)
if err := service.Start(); err != nil {
    panic(err)
}
```

### 4. Actor 通信

```go
// 1. 同步 RPC
var reply MyReply
err := actor.RPC(ctx, targetUID, &MyRequest{Data: "hello"}, &reply)

// 2. 异步 RPC
err := actor.AsyncRPC(ctx, targetUID, &MyRequest{Data: "hello"}, func(a gactor.Actor, resp *gactor.RPCResp) {
    // 在回调中处理响应
})

// 3. 消息投递 (Cast - 无需响应)
err := actor.Cast(ctx, targetUID, &MyMessage{Data: "hello"})

// 4. 定时器
actor.StartTimer(time.Second, true, "tick", func(args *gactor.ActorTimerArgs) {
    // 定时任务逻辑
})
```

### 5. Client 使用

Client 用于外部系统连接到 Actor 集群。

```go
// 1. 实现 ClientHandler
type MyClientHandler struct {}
func (h *MyClientHandler) HandleResponse(resp gactor.ClientResponse) { 
    // 处理 RPC 响应
}
func (h *MyClientHandler) HandlePush(push gactor.ClientPush) { 
    // 处理服务端推送消息
}
func (h *MyClientHandler) HandleDisconnect(id int64, sid uint32) {
    // 处理断开连接通知
}
// ... 实现 GetNetAgent 等其他方法 ...

// 2. 创建 Client
client := gactor.NewClient(&gactor.ClientConfig{
    NodeId:        "client-1",
    ActorCategory: 1, // 目标Actor分类
    Handler:       &MyClientHandler{},
})

// 3. 发送请求
err := client.SendRequest(ctx, gactor.ClientRequest{
    ID:      targetActorID,
    Payload: []byte("data"),
    Timeout: 5 * time.Second,
})
```
