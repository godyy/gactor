# gactor

gactor是一个基于Actor模型的Go语言并发框架，提供了高性能、可扩展的并发编程解决方案。

## 特性

- 基于Actor模型的并发编程
- 支持RPC调用和消息传递
- 内置定时器系统
- 支持Actor优先级管理
- 网络通信支持
- 可扩展的元数据驱动
- 高性能的时间轮实现

## 核心概念

### Actor

Actor是gactor框架中的基本执行单元，每个Actor都有：

- 唯一的标识符(ActorUID)
- 独立的状态
- 消息处理能力
- 定时器支持
- RPC调用能力

### CActor

CActor是面向客户端的Actor，除了具备普通Actor的所有特性外，还具有：

- 网络会话管理
- 客户端连接状态管理
- 向客户端推送消息的能力
- 断开客户端连接的能力

### Client

Client是gactor框架中的客户端组件，用于与服务端进行通信：

- 支持向Service发送请求
- 支持接收Service的响应数据
- 支持接收Service的推送数据
- 支持处理Actor断开连接的通知

## 快速开始

### Service

#### 1. 定义Actor

```go
// 定义Actor
type MyActor struct {
    gactor.Actor
}

func (a *MyActor) OnStart() error {
    // 初始化逻辑
    return nil
}

func (a *MyActor) OnStop() error {
    // 清理逻辑
    return nil
}

// 定义Actor定义
var MyActorDefine = &gactor.ActorDefine{
    ActorDefineCommon: &gactor.ActorDefineCommon{
        // Actor名称，用于日志和调试
        Name: "MyActor",
        
        // Actor类别，用于区分不同类型的Actor
        Category: 1,
        
        // Actor优先级，值越小优先级越高
        Priority: 0,
        
        // Actor信箱大小，用于缓存待处理的消息
        MessageBoxSize: 100,
        
        // Actor能够容纳的已触发的待执行定时器的最大数量
        // 当容量达到上限时，新触发的定时器将等待先前的定时器执行后才能继续排队
        MaxTriggeredTimerAmount: 10,
        
        // Actor能够容纳的已完成异步RPC调用的最大数量
        // 当容量达到上限时，新完成的异步RPC调用将等待先前的异步RPC调用执行后才能继续排队
        MaxCompletedAsyncRPCAmount: 1,
        
        // Actor的回收时间，Actor空闲超过该时间后会被系统回收
        RecycleTime: 5 * time.Minute,
        
        // Actor请求处理器，用于处理接收到的请求
        Handler: func(ctx context.Context, req *gactor.Request) error {
            // 处理请求
            return nil
        },
    },
    // Actor行为构造器，用于创建Actor行为实例
    BehaviorCreator: func(actor gactor.Actor) gactor.ActorBehavior {
        return &MyActor{Actor: actor}
    },
}

// 定义CActor
type MyCActor struct {
    gactor.CActor
}

func (a *MyCActor) OnStart() error {
    // 初始化逻辑
    return nil
}

func (a *MyCActor) OnStop() error {
    // 清理逻辑
    return nil
}

func (a *MyCActor) OnConnected() {
    // 客户端连接时的处理逻辑
}

func (a *MyCActor) OnDisconnected() {
    // 客户端断开连接时的处理逻辑
}

// 定义CActor定义
var MyCActorDefine = &gactor.CActorDefine{
    ActorDefineCommon: &gactor.ActorDefineCommon{
        // Actor名称，用于日志和调试
        Name: "MyCActor",
        
        // Actor类别，用于区分不同类型的Actor
        Category: 2,
        
        // Actor优先级，值越小优先级越高
        Priority: 0,
        
        // Actor信箱大小，用于缓存待处理的消息
        MessageBoxSize: 100,
        
        // Actor能够容纳的已触发的待执行定时器的最大数量
        // 当容量达到上限时，新触发的定时器将等待先前的定时器执行后才能继续排队
        MaxTriggeredTimerAmount: 10,
        
        // Actor能够容纳的已完成异步RPC调用的最大数量
        // 当容量达到上限时，新完成的异步RPC调用将等待先前的异步RPC调用执行后才能继续排队
        MaxCompletedAsyncRPCAmount: 1,
        
        // Actor的回收时间，Actor空闲超过该时间后会被系统回收
        RecycleTime: 5 * time.Minute,
        
        // Actor请求处理器，用于处理接收到的请求
        Handler: func(ctx context.Context, req *gactor.Request) error {
            // 处理请求
            return nil
        },
    },
    // CActor行为构造器，用于创建CActor行为实例
    BehaviorCreator: func(actor gactor.CActor) gactor.CActorBehavior {
        return &MyCActor{CActor: actor}
    },
}
```

#### 2. 定义ServiceHandler

ServiceHandler是Service的核心处理器，负责提供网络通信、元数据管理、数据包编解码和时间系统等功能。

```go
// 定义ServiceHandler
type MyServiceHandler struct {
    // 元数据驱动，用于管理Actor的元数据
    metaDriver gactor.MetaDriver
    // 网络代理，用于处理网络通信
    netAgent gactor.NetAgent
    // 数据包编解码器，用于编解码网络数据包
    packetCodec gactor.PacketCodec
    // 时间系统，用于提供时间相关功能
    timeSystem gactor.TimeSystem
}

// 创建ServiceHandler
func NewMyServiceHandler() *MyServiceHandler {
    return &MyServiceHandler{
        metaDriver:  &MyMetaDriver{},
        netAgent:    &MyNetAgent{},
        packetCodec: &MyPacketCodec{},
        timeSystem:  &MyTimeSystem{},
    }
}

// 获取Meta数据驱动
// MetaDriver负责管理Actor的元数据，包括：
// 1. 获取Actor的元数据信息
// 2. 管理Actor的部署信息
func (h *MyServiceHandler) GetMetaDriver() gactor.MetaDriver {
    return h.metaDriver
}

// 获取网络代理
// NetAgent负责处理网络通信，包括：
// 1. 获取本地节点ID
// 2. 发送数据包到指定节点
func (h *MyServiceHandler) GetNetAgent() gactor.NetAgent {
    return h.netAgent
}

// 获取数据包编解码器
// PacketCodec负责编解码网络数据包，包括：
// 1. 获取和回收数据包
// 2. 编码数据包和负载数据
// 3. 解码数据包负载
func (h *MyServiceHandler) GetPacketCodec() gactor.PacketCodec {
    return h.packetCodec
}

// 获取时间系统
// TimeSystem负责提供时间相关功能，包括：
// 1. 获取当前时间
// 2. 管理定时器
func (h *MyServiceHandler) GetTimeSystem() gactor.TimeSystem {
    return h.timeSystem
}

// 实现MetaDriver接口
type MyMetaDriver struct {
    // Actor UID到Meta的映射
    metaMap map[gactor.ActorUID]*gactor.Meta
}

// GetMeta 获取Actor的元数据
// 当Meta数据不存在时返回ErrMetaNotExists
func (d *MyMetaDriver) GetMeta(uid gactor.ActorUID) (*gactor.Meta, error) {
    meta, ok := d.metaMap[uid]
    if !ok {
        return nil, gactor.ErrMetaNotExists
    }
    return meta, nil
}

// 实现NetAgent接口
type MyNetAgent struct {
    nodeId string
}

// NodeId 返回本地节点ID
func (a *MyNetAgent) NodeId() string {
    return a.nodeId
}

// SendPacket 发送数据包p到nodeId指定的节点
func (a *MyNetAgent) SendPacket(ctx context.Context, nodeId string, p gactor.Packet) error {
    // 实现发送数据包的逻辑
    return nil
}

// 实现PacketCodec接口
type MyPacketCodec struct {
    // 数据包管理器
    packetManager gactor.PacketManager
}

// GetPacket 获取容量为size的数据包
func (c *MyPacketCodec) GetPacket(size int) gactor.Packet {
    return c.packetManager.GetPacket(size)
}

// PutPacket 回收数据包
func (c *MyPacketCodec) PutPacket(p gactor.Packet) {
    c.packetManager.PutPacket(p)
}

// Encode 编码数据包
// allocator提供了获取数据包类型和分配数据包实体的功能
func (c *MyPacketCodec) Encode(allocator gactor.PacketAllocator, payload any) (gactor.Packet, error) {
    // 实现数据包编码逻辑
    return nil, nil
}

// EncodePayload 编码负载数据
// 根据数据包类型pt编码payload并生成数据包返回
func (c *MyPacketCodec) EncodePayload(pt gactor.PacketType, payload any) (gactor.Packet, error) {
    // 实现负载数据编码逻辑
    return nil, nil
}

// DecodePayload 解码负载数据
// 根据数据包类型pt解码p中负载数据并生成负载数据对象
func (c *MyPacketCodec) DecodePayload(pt gactor.PacketType, p gactor.Packet, v any) error {
    // 实现负载数据解码逻辑
    return nil
}

// 实现TimeSystem接口
type MyTimeSystem struct {
    // 基础时间系统，提供基本的时间功能
}

// Now 获取当前时间
func (t *MyTimeSystem) Now() time.Time {
    return time.Now()
}

// Until 返回t距离当前时间的时间差
func (t *MyTimeSystem) Until(t time.Time) time.Duration {
    return time.Until(t)
}
```

#### 3. 创建Service

```go
// 创建Service配置
config := &gactor.ServiceConfig{
    // Actor定义列表
    ActorDefines: []gactor.IActorDefine{
        MyActorDefine,
        MyCActorDefine,
    },
    
    // 时间轮配置
    TimeWheelLevels: []gtimewheel.LevelConfig{
        {
            Name:  "ms",
            Span:  time.Millisecond,
            Slots: 1000,
        },
        {
            Name:  "s",
            Span:  time.Second,
            Slots: 60,
        },
        {
            Name:  "min",
            Span:  time.Minute,
            Slots: 60,
        },
    },
    
    // 最大定时器延迟，默认为时间轮配置支持的最大值
    MaxTimerDelay: time.Hour,
    
    // 能够容纳的已触发的待执行定时器的最大数量
    // 当容量达到上限时，新触发的定时器将等待先前的定时器执行后才能继续排队
    MaxTriggerdTimerAmount: 1000,
    
    // 默认RPC超时时间
    DefRPCTimeout: 5 * time.Second,
    
    // 能够容纳的已完成调用且排队等待执行回调的最大RPC调用数量
    // 当容量达到上限时，新完成的RPC调用将等待先前的RPC调用执行回调后才能继续排队
    MaxCompletedRPCAmount: 1000,
    
    // Actor在接收已完成异步RPC调用时的超时时间
    ActorReceiveCompletedAsyncRPCTimeout: time.Second,
    
    // 最大网络延迟(ms)，用于控制因为网络RTT导致的超时衰减
    MaxRTT: 50,
    
    // Service处理器
    Handler: NewMyServiceHandler(),
}

// 创建Service
// 可以通过ServiceOption配置Service，例如设置日志工具
service := gactor.NewService(config, gactor.WithServiceLogger(myLogger))

// 启动Service
if err := service.Start(); err != nil {
    // 处理错误
    return
}

// 停止Service
service.Stop()
```

#### 4. 启动Actor

```go
// 通过Service启动Actor
// 注意：Actor的Category必须与ActorDefine中定义的Category一致
err := service.StartActor(ctx, gactor.ActorUID{
    Category: 1,  // Actor类别，必须与ActorDefine中定义的Category一致
    ID:       1,  // Actor实例ID，在类别内唯一
})
if err != nil {
    // 处理错误
    return
}

// 通过Service启动CActor
err = service.StartActor(ctx, gactor.ActorUID{
    Category: 2,  // CActor类别，必须与CActorDefine中定义的Category一致
    ID:       1,  // CActor实例ID，在类别内唯一
})
if err != nil {
    // 处理错误
    return
}

// 注意：
// 1. Actor的创建和启动都是由Service统一管理的
// 2. Service会根据ActorDefine的定义创建对应的Actor实例
// 3. Actor启动后会执行以下操作：
//    - 调用Actor的OnStart方法进行初始化
//    - 启动Actor的主循环，处理消息、定时器和异步RPC调用
//    - 重置Actor的回收定时器
// 4. Actor停止时会执行以下操作：
//    - 清空Actor的消息信箱
//    - 调用Actor的OnStop方法进行清理
//    - 通知Service该Actor已停止
```

#### 5. Actor间通信

```go
// RPC调用
var reply MyReply
err := actor.RPC(ctx, targetUID, &MyRequest{Data: "hello"}, &reply)
if err != nil {
    // 处理错误
    return
}

// 异步RPC调用
err = actor.AsyncRPC(ctx, targetUID, &MyRequest{Data: "hello"}, func(a gactor.Actor, resp *gactor.RPCResp) {
    // 处理响应
})

// 消息投递
err = actor.Cast(ctx, targetUID, &MyMessage{Data: "hello"})
if err != nil {
    // 处理错误
    return
}
```

#### 6. 使用定时器

```go
// 启动定时器
timerId := actor.StartTimer(time.Second, true, "timer args", func(args *gactor.ActorTimerArgs) {
    // 处理定时器回调
})

// 停止定时器
actor.StopTimer(timerId)
```

### Client

#### 1. 定义ClientHandler

ClientHandler是Client的核心处理器，负责提供网络通信、元数据管理、数据包管理和消息处理等功能。

```go
// 定义ClientHandler
type MyClientHandler struct {
    // 元数据驱动，用于管理Actor的元数据
    metaDriver gactor.MetaDriver
    // 网络代理，用于处理网络通信
    netAgent gactor.NetAgent
    // 数据包管理器，用于管理数据包
    packetManager gactor.PacketManager
}

// 创建ClientHandler
func NewMyClientHandler() *MyClientHandler {
    return &MyClientHandler{
        metaDriver:    &MyMetaDriver{},
        netAgent:      &MyNetAgent{},
        packetManager: &MyPacketManager{},
    }
}

// 获取Meta数据驱动
func (h *MyClientHandler) GetMetaDriver() gactor.MetaDriver {
    return h.metaDriver
}

// 获取网络代理
func (h *MyClientHandler) GetNetAgent() gactor.NetAgent {
    return h.netAgent
}

// 获取数据包管理器
func (h *MyClientHandler) GetPacketManager() gactor.PacketManager {
    return h.packetManager
}

// 处理响应数据
func (h *MyClientHandler) HandleResponse(resp gactor.ClientResponse) {
    // 处理响应数据
    // 注意：需要自行回收resp.Payload
}

// 处理推送数据
func (h *MyClientHandler) HandlePush(push gactor.ClientPush) {
    // 处理推送数据
    // 注意：需要自行回收push.Payload
}

// 处理Actor断开连接
func (h *MyClientHandler) HandleDisconnect(uid gactor.ActorUID, sid uint32) {
    // 处理Actor断开连接
}
```

#### 2. 创建Client

```go
// 创建Client配置
config := &gactor.ClientConfig{
    // Client处理器，负责提供网络通信、元数据管理、数据包管理和消息处理等功能
    Handler: NewMyClientHandler(),
}

// 创建Client
// 可以通过ClientOption配置Client，例如设置日志工具
client := gactor.NewClient(config, gactor.WithClientLogger(myLogger))

// 注意：
// 1. Client的创建需要提供ClientHandler，用于处理网络通信、元数据管理、数据包管理和消息处理等功能
// 2. ClientHandler必须实现以下接口：
//    - GetMetaDriver：获取元数据驱动，用于管理Actor的元数据
//    - GetNetAgent：获取网络代理，用于处理网络通信
//    - GetPacketManager：获取数据包管理器，用于管理数据包
//    - HandleResponse：处理响应数据
//    - HandlePush：处理推送数据
//    - HandleDisconnect：处理Actor断开连接
// 3. 可以通过ClientOption配置Client，例如：
//    - WithClientLogger：设置日志工具
```

#### 3. 使用Client

```go
// 生成会话ID
sid := client.GenSessionId()

// 发送请求
err := client.SendRequest(ctx, gactor.ClientRequest{
    UID:     targetUID,
    SID:     sid,
    Payload: encodedPayload,
})
if err != nil {
    // 处理错误
    return
}

// 通知Actor断开连接
err = client.NotifyDisconnect(ctx, targetUID, sid)
if err != nil {
    // 处理错误
    return
}
```