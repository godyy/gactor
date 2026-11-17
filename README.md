# gactor

`gactor` 是一个基于 Actor 模型的 Go 并发框架，提供 Actor 生命周期管理、同步/异步 RPC、消息投递、处理链、时间轮、客户端通信，以及集群中 Actor 的动态迁移能力。

## 设计理念

`gactor` 的核心目标是：**让业务以“单线程”的方式组织并发状态，同时保持跨节点通信的能力。**

- **状态自持，串行执行**：每个 Actor 拥有独立的状态与邮箱。发往同一个 Actor 的请求、RPC 回调、定时器回调都在其邮箱中**串行**执行，因此业务状态通常无需额外加锁，也避免了数据竞争。
- **消息驱动，通信即接口**：Actor 之间不共享内存，只能通过 `RPC`（有回复、有超时）、`Cast`（单向、无回复）和 `Forward`（透传请求来源）传递消息。通信接口天然屏蔽了本地与远端差异。
- **运行时与业务分离**：框架提供运行时能力（信箱、时间轮、RPC 管理、网络分发），业务只需实现 `ActorBehavior`（`OnStart` / `OnStop`）。二者通过 `ActorDefine.BehaviorCreator` 绑定。
- **可插拔的基础设施**：注册表、路由、网络、编解码、时间系统、监控全部抽象为接口，由使用方通过 `ServiceHandler` / `ClientHandler` 注入，便于对接任意网络库与存储。
- **迁移以“冻结计划”为边界**：动态迁移在开始前冻结本轮要迁移的 Actor 及其目标节点，后续重试与转发都复用该计划，从而获得可收敛、可观测、可幂等的迁移过程。

## 核心概念

### ActorUID

`ActorUID` 由 `Category`（分类）和 `ID`（分类内唯一实例 ID）组成，是 Actor 的唯一标识，也是所有通信接口的寻址单位。

```go
uid := gactor.ActorUID{Category: 1, ID: 1001}
fmt.Println(uid.String()) // "1-1001"
```

### Actor 与 CActor

业务逻辑实现 `ActorBehavior`：

- `OnStart() error`：Actor 成功启动后调用，用于初始化状态。
- `OnStop(ctx ActorStopContext) error`：Actor 回收时调用。通过 `ctx.Reason` 区分停机原因：

| `ActorStopReason` | 含义 |
| --- | --- |
| `ActorStopReasonNormal` | 普通回收或异常终止 |
| `ActorStopReasonShutdown` | Service 正在停机 |
| `ActorStopReasonMigrated` | 源 Actor 已完成迁移后的本地回收（业务状态已转交目标节点） |

`CActorBehavior` 在 `ActorBehavior` 基础上增加客户端连接生命周期回调：

- `OnConnected()` / `OnDisconnected()`：连接建立/断开时调用。

对应的运行时接口分别是 `Actor` 与 `CActor`，`CActor` 额外提供：

- `CActor.Session() ActorSession`：获取当前会话（`NodeId` + `SID`）。
- `CActor.PushRawMessage(payload any) error`：向客户端主动推送消息。
- `CActor.Disconnect()`：主动断开当前会话。

具体如何声明与注册 Actor，见下文《1. 定义 Actor》。

### Service

`Service` 是一个节点的运行容器，负责 Actor 启动与回收、定时器驱动、本地/远程消息分发以及 RPC 管理。创建 `Service` 时必须提供 `ServiceHandler`，由业务侧注入注册表、路由、网络、编解码、时间系统与监控等基础设施（详见《2. 启动 Service 与 Client》）。

### Context 与处理链

每个请求（RPC / Cast / Raw Request）都通过 `Context` 进入 `ActorConfig.Handler`。`HandlersChain` 支持在业务处理前后插入中间件，类似 Web 框架：

```go
func BusinessHandler(ctx *gactor.Context) {
	var req Request
	if err := ctx.Decode(&req); err != nil { // Decode 不可重复调用
		_ = ctx.ReplyDecodeError()
		return
	}
	_ = ctx.Reply(&Response{Message: "ok"})
}

// 中间件在处理前记录耗时，并调用 ctx.Next() 继续执行后续 Handler。
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

| 方法 | 说明 |
| --- | --- |
| `Actor()` / `RequestType()` / `FromActorUID()` | 读取当前请求信息 |
| `Set()` / `Get()` | 在处理链中传递元数据 |
| `Decode(v)` | 解码请求负载；同一 Context 中不可重复调用 |
| `Reply(v)` / `ReplyDecodeError()` | 回复请求 |
| `Next()` / `Abort()` | 推进或终止处理链 |
| `Clone()` | 复制 Context 供异步场景使用 |
| `RPC()` / `AsyncRPC()` / `Cast()` | 以当前 Actor 发起通信 |

`Context` 的 `RPC()` 与 `AsyncRPC()` 会优先复用当前请求的 deadline。`AsyncRPC()` 与 `AsyncCall()` 会**挂起**处理链，在回调执行完成后自动继续后续 Handler；因此回调中不应把 `Context` 保存到其生命周期之外。

## 1. 定义 Actor

Actor 的元数据与配置由 `ActorDefine` 描述，业务通过 `BehaviorCreator` 把 `ActorBehavior` 绑定到运行时 `Actor`。

```go
// 业务行为：嵌入 gactor.Actor 后即拥有 RPC / Cast / 定时器 / AsyncCall 等能力。
type UserActor struct {
	gactor.Actor
	state int
}

func (a *UserActor) OnStart() error {
	a.state = 0
	return nil
}

func (a *UserActor) OnStop(ctx gactor.ActorStopContext) error {
	if ctx.Reason == gactor.ActorStopReasonMigrated {
		// 业务状态已由迁移流程转交目标节点，仅释放本地资源。
		return nil
	}
	// 普通回收或 Service 停机时执行持久化、资源释放等逻辑。
	return nil
}

// 定义：在注册到 Service 前创建。
var UserActorDefine = gactor.NewActorDefine(gactor.ActorDefineConfig{
	Name:              "user",
	Category:          1,  // ActorUID.Category
	Priority:          0,  // 数值越小优先级越高
	PriMessageBoxSize: 16, // 优先级信箱大小，必须 > 0
	MessageBoxSize:    128, // 普通信箱大小，必须 > 0
	RecycleTime:       10 * time.Minute, // > 0 时空闲超时自动回收；为 0 表示常驻
	BehaviorCreator: func(actor gactor.Actor) gactor.ActorBehavior {
		return &UserActor{Actor: actor}
	},
})
```

面向客户端的 `CActor` 通过 `NewCActorDefine` 定义，其 `BehaviorCreator` 返回 `CActorBehavior`，`RecycleTime` **必须 > 0**：

```go
type PlayerActor struct {
	gactor.CActor
}

func (a *PlayerActor) OnConnected()    {}
func (a *PlayerActor) OnDisconnected() {}

var PlayerDefine = gactor.NewCActorDefine(gactor.CActorDefineConfig{
	Name:              "player",
	Category:          2,
	Priority:          1,
	PriMessageBoxSize: 16,
	MessageBoxSize:    64,
	RecycleTime:       30 * time.Minute, // CActor 必须设置有效回收时间
	BehaviorCreator: func(actor gactor.CActor) gactor.CActorBehavior {
		return &PlayerActor{CActor: actor}
	},
})
```

配置字段说明：

| 字段 | 说明 |
| --- | --- |
| `Name` / `Category` | Actor 名称与分类；`Category` 决定 `ActorUID.Category` |
| `Priority` / `PriMessageBoxSize` | 优先级与优先级信箱大小；数值越小优先级越高 |
| `MessageBoxSize` | 普通信箱大小 |
| `RecycleTime` | 空闲回收时间；`> 0` 时超时自动回收，`0` 表示常驻（CActor 必须 `> 0`） |
| `BehaviorCreator` | 行为工厂，每次启动返回一个新的 Behavior 实例 |

注意事项：

- `PriMessageBoxSize` 与 `MessageBoxSize` 都必须大于 0，`BehaviorCreator` 不能为空。
- 高优先级消息进入优先级信箱，会被优先处理。
- 定义在 `Service` 启动前通过 `ServiceConfig.ActorConfig.ActorDefines` 注册。

## 2. 启动 Service 与 Client

`Service` 负责一个节点内 Actor 的运行与消息分发；`Client` 代表框架外部调用方连接服务端 `CActor`。二者都需要业务侧实现对应的 Handler。

### 2.1 实现 ServiceHandler

`ServiceHandler` 为框架注入六类基础设施：

| 接口 | 职责 |
| --- | --- |
| `ActorRegistry` | Actor 注册、续约、注销与迁移接管 |
| `ActorRouter` | 未注册 Actor 的节点选择 |
| `NetAgent` | 向指定节点发送字节数据 |
| `PacketCodec` | 业务负载编解码，同时也是 `BytesManager` |
| `TimeSystem` | 提供当前时间与时间差 |
| `ServiceMonitor` | Actor、定时器、RPC、Cast 监控；可选实现 `ServiceMigrationMonitor` 接收迁移事件 |

下面给出一份**最小可编译**的实现样板（网络代理与监控此处省略/置空，注册表为内存版；生产环境请替换为共享存储与真实网络层，完整实现见 [internal/examples/s2s](file:///Users/godyy/work/go/godyy/gactor/internal/examples/s2s)）。

```go
// ---- 1. Actor 注册表：内存实现，负责注册、续约、注销与迁移接管 ----
type regEntry struct {
	nodeId   string
	leaseId  string
	expireAt int64
}

type memRegistry struct {
	mu sync.RWMutex
	m  map[gactor.ActorUID]*regEntry
}

func newMemRegistry() *memRegistry {
	return &memRegistry{m: make(map[gactor.ActorUID]*regEntry)}
}

func (r *memRegistry) MakeLeaseID() string { return xid.New().String() }

func (r *memRegistry) RegisterActor(p gactor.ActorRegisterParams) (gactor.ActorRegisterResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	e, ok := r.m[p.UID]
	if !ok {
		e = &regEntry{nodeId: p.NodeId, leaseId: p.LeaseId}
		r.m[p.UID] = e
	} else if e.nodeId != p.NodeId {
		// 已被其它节点注册。
		return gactor.ActorRegisterResult{NodeId: e.nodeId, ExpireAt: e.expireAt}, gactor.ErrActorAlreadyRegistered
	}
	e.leaseId = p.LeaseId
	if p.TTL > 0 {
		e.expireAt = time.Now().Unix() + p.TTL
	}
	return gactor.ActorRegisterResult{NodeId: e.nodeId, ExpireAt: e.expireAt}, nil
}

func (r *memRegistry) TakeOverActor(p gactor.ActorTakeOverParams) (gactor.ActorRegisterResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	e, ok := r.m[p.UID]
	if !ok {
		return gactor.ActorRegisterResult{}, gactor.ErrActorNotExists
	}
	// 仅当旧租约匹配时才原子接管，避免旧租约的续约/注销覆盖新记录。
	if e.leaseId != p.SourceLeaseId {
		return gactor.ActorRegisterResult{NodeId: e.nodeId, ExpireAt: e.expireAt}, gactor.ErrLeaseMismatch
	}
	e.nodeId, e.leaseId = p.NodeId, p.LeaseId
	if p.TTL > 0 {
		e.expireAt = time.Now().Unix() + p.TTL
	}
	return gactor.ActorRegisterResult{NodeId: e.nodeId, ExpireAt: e.expireAt}, nil
}

func (r *memRegistry) UnregisterActor(p gactor.ActorUnregisterParams) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	e, ok := r.m[p.UID]
	if !ok {
		return gactor.ErrActorNotExists
	}
	if e.nodeId != p.NodeId || e.leaseId != p.LeaseId {
		return gactor.ErrLeaseMismatch
	}
	delete(r.m, p.UID)
	return nil
}

func (r *memRegistry) KeepActorAlive(p gactor.ActorKeepAliveParams) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	e, ok := r.m[p.UID]
	if !ok {
		return gactor.ErrActorNotExists
	}
	if e.nodeId != p.NodeId || e.leaseId != p.LeaseId {
		return gactor.ErrLeaseMismatch
	}
	e.expireAt = time.Now().Unix() + p.TTL
	return nil
}

func (r *memRegistry) GetActorLocation(uid gactor.ActorUID) (gactor.ActorLocation, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	e, ok := r.m[uid]
	if !ok {
		return gactor.ActorLocation{}, gactor.ErrActorNotExists
	}
	return gactor.ActorLocation{NodeId: e.nodeId, ExpireAt: e.expireAt}, nil
}

// ---- 2. Actor 路由：Actor 未注册时选择目标节点 ----
type router struct {
	nodes map[gactor.ActorUID]string
}

func (r *router) PickActorNode(uid gactor.ActorUID) (string, error) {
	if nodeId, ok := r.nodes[uid]; ok {
		return nodeId, nil
	}
	return "", gactor.ErrNoAvailableNode
}

// ---- 3. 编解码器：同时实现 PacketCodec 与 BytesManager（此处用 JSON 演示）----
type jsonCodec struct{}

func (c *jsonCodec) GetBytes(n int) []byte { return make([]byte, 0, n) }
func (c *jsonCodec) PutBytes([]byte)       {}

func (c *jsonCodec) Encode(alloc gactor.PacketBufAllocator, ph gactor.PacketHead, payload any) (*gactor.Buffer, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	buf, err := alloc(ph, len(data)) // alloc 已写入包类型与包头，只需追加 payload。
	if err != nil {
		return nil, err
	}
	if _, err := buf.Write(data); err != nil {
		return nil, err
	}
	return buf, nil
}

func (c *jsonCodec) EncodePayload(_ gactor.PacketType, payload any) ([]byte, error) {
	return json.Marshal(payload)
}

func (c *jsonCodec) DecodePayload(_ gactor.PacketType, b *gactor.Buffer, v any) error {
	return json.Unmarshal(b.UnreadData(), v)
}

// ---- 4. 组装 ServiceHandler ----
type serviceHandler struct {
	registry *memRegistry
	router   *router
	agent    gactor.NetAgent // 由具体网络层提供（gnet / gcluster 等）。
}

func (h *serviceHandler) GetActorRegistry() gactor.ActorRegistry { return h.registry }
func (h *serviceHandler) GetActorRouter() gactor.ActorRouter     { return h.router }
func (h *serviceHandler) GetNetAgent() gactor.NetAgent           { return h.agent }
func (h *serviceHandler) GetPacketCodec() gactor.PacketCodec     { return &jsonCodec{} }
func (h *serviceHandler) GetTimeSystem() gactor.TimeSystem       { return gactor.DefTimeSystem }
func (h *serviceHandler) GetMonitor() gactor.ServiceMonitor      { return nil }

// OnNodeBytes 由网络层在收到字节数据后回调，转交 Service 处理。
func (h *serviceHandler) OnNodeBytes(nodeId string, b []byte) error {
	return h.svc.HandlePacket(nodeId, b) // h.svc 指向关联的 *gactor.Service。
}
```

> 说明：`NetAgent` 只需实现 `Send2Node(nodeId string, b []byte) error`；`ServiceMonitor` 可为 `nil`，框架会自动跳过监控上报。

### 2.2 创建与启动 Service

```go
svc := gactor.NewService(&gactor.ServiceConfig{
	NodeId: "node-1",
	ActorConfig: gactor.ActorConfig{
		ActorDefines: []gactor.ActorDefine{UserActorDefine}, // required
		Handler:      gactor.NewHandlersChain(BusinessHandler).Handle, // required
	},
	TimerConfig: gactor.TimerConfig{
		TimeWheelLevels: []gtimewheel.LevelConfig{ // required
			{Name: "second", Span: time.Second, Slots: 60},
			{Name: "minute", Span: time.Minute, Slots: 60},
		},
	},
	RPCConfig: gactor.RPCConfig{
		DefRPCTimeout: 5 * time.Second, // 默认 5s
	},
	Handler: myServiceHandler, // required
})

if err := svc.Start(); err != nil {
	panic(err)
}
defer svc.Stop()

// 网络层收到字节数据后交给 Service 处理。
if err := svc.HandlePacket("node-2", rawBytes); err != nil {
	panic(err)
}
```

必填项：`NodeId`、`ActorConfig.Handler`、`ActorConfig.ActorDefines`、`TimerConfig.TimeWheelLevels`、`Handler`。

默认值与约束：

- `RPCConfig.DefRPCTimeout` 默认 `5s`，`RPCConfig.MaxRPCCallAmount` 默认 `1000`。
- `ActorConfig.RegistryTTL` 默认 `60s`（秒级），`ActorConfig.KeepAliveInterval` 默认 `RegistryTTL/3`（必须小于 `RegistryTTL`）。
- `TimerConfig.MaxTimerAmount` 默认 `1000`。
- `ServiceConfig.MaxRTT` 默认 `50`（ms），`ServiceConfig.QueueWriteTimeAlarmThreshold` 默认 `50ms`。
- 可选项通过 `WithServiceLogger` 注入日志、`WithServiceAckManager` 启用可靠投递重传。

### 2.3 实现 ClientHandler 与启动 Client

`ClientHandler` 需要提供注册表、网络代理、字节池，以及响应、推送、断开连接回调：

```go
type clientHandler struct {
	registry gactor.ActorRegistry
	agent    gactor.NetAgent
	svc      *gactor.Client
}

func (h *clientHandler) GetActorRegistry() gactor.ActorRegistry { return h.registry }
func (h *clientHandler) GetNetAgent() gactor.NetAgent           { return h.agent }
func (h *clientHandler) GetBytesManager() gactor.BytesManager   { return &jsonCodec{} }

func (h *clientHandler) HandleResponse(resp gactor.ClientResponse) {
	if resp.Payload != nil {
		defer resp.Payload.Release() // 使用完需归还负载缓冲。
	}
	// 处理响应（resp.ID、resp.SID、resp.ErrCode、resp.Payload）。
}

func (h *clientHandler) HandlePush(push gactor.ClientPush) {
	if push.Payload != nil {
		defer push.Payload.Release()
	}
	// 处理服务端推送。
}

func (h *clientHandler) HandleDisconnect(id gactor.ActorID, sid uint32) {
	// 处理连接断开。
}

// OnNodeBytes 由网络层回调，转交 Client 处理。
func (h *clientHandler) OnNodeBytes(nodeId string, b []byte) error {
	return h.svc.HandlePacket(nodeId, b)
}
```

`Client` 在创建时自动启动，调用 `Stop()` 停止：

```go
client := gactor.NewClient(&gactor.ClientConfig{
	NodeId:        "client-1",
	ActorCategory: 2, // 与之通信的目标 Actor 分类
	Handler:       myClientHandler,
}, gactor.WithClientLogger(logger))
defer client.Stop()

sid := client.GenSessionId()
if err := client.Connect(1001, sid); err != nil { // 连接 Player Actor(1001)
	panic(err)
}

if err := client.SendRequest(gactor.ClientRequest{
	ID:      1001,
	SID:     sid,
	Timeout: 5 * time.Second, // 未设置时使用 DefRequestTimeout
	Payload: []byte("data"),
}); err != nil {
	panic(err)
}

_ = client.Disconnect(1001, sid)

// 网络层收到字节数据后交给 Client 处理。
_ = client.HandlePacket("node-1", rawBytes)
```

要点：

- `ClientResponse.Payload` 与 `ClientPush.Payload` 是 `*gactor.Buffer`，需要由 `ClientHandler` 自行回收。
- `Client.SendRequest` 不接收 `context.Context`，请求超时由 `ClientRequest.Timeout` 控制。
- 可选项 `WithClientAckManager` 可启用可靠投递重传。

## 3. 基础功能

### 3.1 启动 Actor

```go
uid := gactor.ActorUID{Category: 1, ID: 1001}
if err := svc.StartActor(context.Background(), uid); err != nil {
	panic(err)
}
```

要点：

- `StartActor` 通过向 Actor 投递消息并检查处理结果来判断是否启动成功；即使 Actor 已启动，也会再次校验其仍可用。
- 开始停机后不能再启动 Actor。迁移期间源节点不再创建新的本地 Actor，返回 `ErrCodeServiceMigrating`。
- `RecycleTime > 0` 的 Actor 空闲超时会被自动回收；`RecycleTime == 0` 表示常驻。
- 框架未提供单独停止某个 Actor 的公开接口，Actor 的终止来自空闲自动回收、Service 停机或迁移清理。

### 3.2 与 Actor 通信

`Actor`、`Service` 与 `Context` 都提供通信接口。默认接口使用 `RPCConfig.DefRPCTimeout`；需要显式控制时使用 `WithTimeout`、`WithDeadline` 或 `WithContext` 版本。

```go
target := gactor.ActorUID{Category: 2, ID: 2001}
var reply Reply

// 同步 RPC。
err := actor.RPC(target, &Request{Data: "hello"}, &reply)
err = actor.RPCWithTimeout(target, &Request{Data: "hello"}, &reply, 2*time.Second)
err = actor.RPCWithDeadline(target, &Request{Data: "hello"}, &reply, time.Now().Add(2*time.Second))
err = actor.RPCWithContext(ctx, target, &Request{Data: "hello"}, &reply)

// 异步 RPC；回调仍在发起 Actor 的执行流中运行。
err = actor.AsyncRPC(target, &Request{Data: "hello"}, func(a gactor.Actor, resp gactor.RPCResp) {
	if err := resp.DecodeReply(&reply); err != nil {
		return
	}
	// 通过 resp.Ctx() 取回传入的异步回调上下文（见 6. 其它辅助功能 - CbCtx）。
}, nil)

// 单向投递、透传当前请求来源。
_ = actor.Cast(target, &Message{Data: "event"})
_ = actor.Forward(target, &Message{Data: "forward"})
```

在 Handler 内优先使用 `Context` 版本，以继承当前请求 deadline 并支持继续挂起处理链：

```go
func Handler(ctx *gactor.Context) {
	var r Reply
	if err := ctx.RPC(target, &Request{Data: "hello"}, &r); err != nil {
		return
	}

	// 异步版本：回调执行完之后会继续后续 Handler。
	_ = ctx.AsyncRPC(target, &Request{Data: "hello"}, func(c *gactor.Context, resp gactor.RPCResp) {
		var r2 Reply
		_ = resp.DecodeReply(&r2)
	}, nil)
}
```

`Service` 也可以本节点身份直接发起调用，无需先持有 Actor 句柄：

```go
err := svc.RPC(target, &Request{Data: "hello"}, &reply)
err = svc.AsyncRPC(target, &Request{Data: "hello"}, func(resp gactor.RPCResp) {
	// 处理响应。
}, nil)
err = svc.Cast(target, &Message{Data: "event"})
```

三种通信语义：

| 接口 | 语义 |
| --- | --- |
| `RPC` / `AsyncRPC` | 有回复、有超时；本地与远端统一寻址 |
| `Cast` | 单向投递，无回复 |
| `Forward` | 透传当前请求的来源，接收方感知原始来源 |

### 3.3 定时器

```go
// 一次性定时器与周期定时器；回调在 Actor 邮箱中串行执行。
tid := actor.StartTimer(time.Second, true, func(args gactor.ActorTimerArgs) {
	// args.Actor、args.TID、args.Ctx 可用。
}, "tick")
actor.StopTimer(tid)
```

`Service` 也提供 `StartTimer` / `StopTimer`，用于注册与具体 Actor 无关的定时任务。

### 3.4 异步调用与 Context 挂起

`Actor.AsyncCall` 可将任意外部异步操作的结果安全地带回 Actor：

```go
caller, err := actor.AsyncCall(func(args gactor.ActorFuncArgs) {
	if args.Err != nil {
		return
	}
	// args.Value 为外部异步操作的结果。
}, 3*time.Second)
if err != nil {
	return
}

// 在超时前，在任意 goroutine 中执行 caller，把结果送回 Actor 邮箱。
go func() {
	value, err := doSomethingAsync()
	_ = caller(value, err)
}()
```

若在超时时间内未调用返回的 `ActorAsyncCaller`，框架会自动以 `ErrTimeout` 触发回调。

`Context.AsyncCall` 与之类似，区别在于它会挂起处理链，回调完成后继续后续 Handler：

```go
func Handler(ctx *gactor.Context) {
	caller, err := ctx.AsyncCall(func(args gactor.ContextFuncArgs) {
		if args.Err != nil {
			return
		}
		_ = args.Value
	}, 3*time.Second)
	if err != nil {
		return
	}
	go func() {
		v, e := doSomethingAsync()
		_ = caller(v, e)
	}()
}
```

需要跨多个异步步骤手动挂起时，使用 `ContextSuspender`：

```go
s := gactor.NewContextSuspender(ctx).Suspend()
go func() {
	// ... 异步完成后恢复处理链。
	s.Resume()
}()
```

若需把 `Context` 带出其生命周期（例如在另一 goroutine 中稍后回复），先 `Clone()`：

```go
cp := ctx.Clone()
go func() {
	// ... 异步完成后使用副本回复。
	_ = cp.Reply(&Response{})
}()
```

## 4. 停机流程

`Service.Stop()`：

- 状态按 `init → started → stopping → stopped` 流转，属于**一次性生命周期**，`Stop` 之后不可再次 `Start`。
- `Stop` **阻塞**直到所有内部协程退出，可在收到退出信号后直接调用。
- 停机时会先唤醒正在等待迁移收敛的调用方，并返回明确的停机错误；**停机不等同于迁移收敛**。
- 随后按优先级分层停机所有 Actor。

Actor 停机过程：

- 先清空信箱：Service 已停机时，未处理的普通消息以 `ErrCodeServiceStopped` 回复；优先级信箱中的消息仍会被处理。
- 等待 Actor 自身的未完成工作结束（信箱清空、未完成的异步调用结束、外部引用归零）。
- 最后调用 `OnStop`，停机原因优先取 `ActorStopReasonShutdown`（Service 停机），其次为 `ActorStopReasonMigrated`。

停机顺序：

- 所有 Actor 按**优先级分层**依次停机：先停低优先级层，再逐级推进到高优先级层；每一层需连续多次确认已停机后，才推进到下一层。

注意事项：

- 停机是同步阻塞的，建议在收到信号的主流程中调用 `svc.Stop()`。
- 停机期间不再受理新的业务处理：未处理的请求会被以 `ErrCodeServiceStopped` 回复。
- 已挂起的 `AsyncRPC` / `AsyncCall` 等在 Service 停机时会以停机错误（如 `errServiceStopped`）完成回调。
- `Client.Stop()` 同样阻塞直到内部协程退出，停机后所有接口返回 `ErrClientStopped`。
- 若在停机前仍有外部持有的 Actor 引用未释放，停机流程会等待其结束，请确保引用及时释放。

## 5. 动态迁移（扩缩容）

`Service` 可将当前运行中的 Actor 迁移到目标节点。迁移以**冻结的迁移计划**为边界：开始前通过规划函数为每个 Actor 确定目标节点，后续重试与转发都复用该结果。

迁移流程分为三个阶段：

1. **接管**：源 Actor 触发 `OnMigrateOut` 保存快照，向目标节点发起迁移；目标节点拉起 Actor 并执行 `OnMigrateIn` 恢复状态，接管注册表租约。
2. **转发**（`migrated` 态）：完成接管的源 Actor 仍驻留源节点，把后续收到的业务消息（请求 / Cast / 连接 / 断连）转发到目标节点，避免流量切换期间丢消息。
3. **清理**：上层流量切换完成后，调用方结束转发并停止源端 Actor，触发 `OnStop(ActorStopReasonMigrated)`。

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

`ActorMigrationContext` 提供 Actor UID、源/目标节点、源租约、目标租约、迁移序号和 CActor 会话副本（`TargetLeaseID` 仅 `OnMigrateIn` 时有值）。

> **幂等性要求**：`OnMigrateOut` 与 `OnMigrateIn` 都必须具备幂等性，避免自动重试或重复迁移包造成重复副作用。完整可运行示例见 [internal/examples/s2s/actors/migration.go](file:///Users/godyy/work/go/godyy/gactor/internal/examples/s2s/actors/migration.go)。

### 迁移配置

`ServiceConfig.Migration`（类型 `MigrationConfig`）：

| 字段 | 含义 | 默认值 |
| --- | --- | --- |
| `MaxConcurrentActors` | 源节点同时等待目标端接管结果的 Actor 数 | 256 |
| `MaxConcurrentInboundActors` | 目标节点同时执行接管的 Actor 数，超出时源端退避重试 | 256 |
| `MaxConcurrentForwards` | 源节点同时发送的迁移转发请求数 | 256 |

### 迁移 API

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

// 1. 启动迁移，将本节点所有 Actor 迁移到 node-2；cb 可选，用于接收收敛通知。
if err := svc.StartMigration("node-2", func(snapshot gactor.ServiceMigrationSnapshot) {
	log.Printf("migration converged: %d actors", snapshot.MigratedActorCount)
}); err != nil {
	panic(err)
}

// 2. 等待冻结计划全部进入 migrated 或 stopped 终态。
if err := svc.WaitMigrationConverged(ctx); err != nil {
	panic(err)
}

// 3. 在上层流量切换完成后，停止源端业务转发。
if err := svc.FinishMigrationForwarding(); err != nil {
	panic(err)
}

// 4. 停止源端已迁移 Actor，触发 OnStop(ActorStopReasonMigrated)，并清理迁移状态。
if err := svc.FinalizeMigration(ctx); err != nil {
	panic(err)
}
```

扩容时可通过规划函数把同一源节点上的 Actor 分散到多个目标节点：

```go
err := svc.StartMigrationWithPlanner(func(uid gactor.ActorUID) (string, error) {
	if uid.ID%2 == 0 {
		return "node-b", nil
	}
	return "node-c", nil
}, nil)
if err != nil {
	panic(err)
}
```

规划函数只会在迁移开始前为冻结计划中的每个 Actor 调用一次；重试和转发始终复用冻结结果。通过 `svc.MigrationSnapshot()` 可查询迁移的阶段统计、重试信息、转发统计和每个 Actor 的迁移状态：单目标计划可读取 `TargetNodeID`，多目标计划中该字段为空，应读取 `Actors` 内每个 Actor 的 `TargetNodeID`。

节点身份认证与网络访问控制由上层部署及 `NetAgent` 负责。

## 6. 其它辅助功能

以下能力均已在框架中实现。是否使用、如何使用由业务决定，这里仅列出入口与用途。

| 功能 | 入口 | 说明 |
| --- | --- | --- |
| 回调上下文 CbCtx | `NewCbCtx(pairs...)` / `CbCtxRelease(km)`；`AsyncRPC(..., cbctx)` / `StartTimer(..., cbctx)` | 随异步 RPC、定时器携带任意键值，回调中通过 `RPCResp.Ctx()` / `ActorTimerArgs.Ctx` 取回。 |
| 可靠投递重传 | `AckConfig` + `WithServiceAckManager` / `WithClientAckManager` | 记录已发出待确认的数据包，超时后按 `TickInterval` 重试，达到 `MaxRetry` 判定失败。未启用时不做确认。 |
| 监控上报 | `ServiceMonitor`（`ServiceHandler.GetMonitor()`）；`ServiceMigrationMonitor` | 上报 Actor / 定时器 / RPC / Cast 动作计数（`Monitor*Action` 枚举）；额外实现 `ServiceMigrationMonitor` 可收到迁移运行时事件与收敛耗时。 |
| 统一错误码 | `ErrCode` 常量；`Err2ErrCode(err)` | 覆盖编解码、服务状态、Actor 状态与迁移状态，便于在响应中返回稳定错误码。 |
| 优先级邮箱 | `ActorDefineConfig.Priority` / `PriMessageBoxSize` | 数值越小优先级越高；高优先级消息优先投递执行，停机时也优先处理。 |
| Context 挂起 / 复制 | `ContextSuspender`；`Context.Clone()` | 手动挂起处理链，异步完成后 `Resume` 继续；`Clone` 用于把 `Context` 安全地带入异步场景。 |
| 负载缓冲复用 | `BytesManager`（`GetBytes` / `PutBytes`） | `Buffer` 来自缓冲池，使用完毕后需 `Release` 归还，以降低 GC 压力。 |
| 日志注入 | `WithServiceLogger` / `WithClientLogger` | 注入 `glog.Logger`；未注入时框架提供默认日志实现。 |
| 节点内直连通信 | `Service.RPC` / `AsyncRPC` / `Cast` | 以本节点身份直接向任意 Actor 发起调用，无需先持有 Actor 句柄。 |

回调上下文（CbCtx）示例：

```go
// 包级复用的键，用于存取回调上下文中的业务数据。
var cbKeyReqID = kv.NewK[int64]("reqID") // import "github.com/godyy/gutils/container/kv"

cbctx := gactor.NewCbCtx()
cbKeyReqID.Set(cbctx, reqID)

if err := actor.AsyncRPC(target, &req, func(a gactor.Actor, resp gactor.RPCResp) {
	// resp.Ctx() 即发起时传入的 cbctx。
	if km, ok := resp.Ctx().(*kv.M); ok {
		reqID, _ := kv.Get(km, cbKeyReqID)
		_ = reqID
		gactor.CbCtxRelease(km) // 由业务自行归还。
	}
}, cbctx); err != nil {
	panic(err)
}
```

> `CbCtx` 是框架通用的“回调载荷”机制：RPC 通过 `RPCResp.Ctx()` 取回、定时器通过 `ActorTimerArgs.Ctx` 取回；框架内部同样以 `kv.M` 承载单次调用的状态。

## 项目结构

- `actor.go`：Actor、CActor、生命周期和迁移公共接口。
- `actor_define.go`：Actor 定义及邮箱、优先级、回收配置。
- `actor_impl.go` / `actor_migration.go`：Actor 运行时与源端迁移状态机。
- `context.go` / `handler.go`：请求上下文与处理链。
- `service.go` / `service_actor.go`：Service 生命周期、Actor 管理与分发。
- `service_rpc.go`：同步和异步 RPC 实现。
- `service_migration.go`：动态迁移、收敛、转发和最终清理。
- `service_monitor.go`：监控接口与动作定义。
- `client.go`：客户端组件与 `ClientHandler`。
- `packet.go` / `net.go` / `bytes.go`：数据包协议、网络与字节缓冲抽象。
- `actor_location.go`：`ActorRegistry` 与 `ActorRouter` 抽象。
- `internal/examples/c2s`：客户端与服务端通信示例。
- `internal/examples/s2s`：服务端集群通信及迁移行为示例。

## 示例与测试

运行 S2S 示例（两个节点演示集群通信与迁移行为）：

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
