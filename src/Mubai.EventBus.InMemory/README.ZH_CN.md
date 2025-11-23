# Mubai.EventBus.InMemory
[English](./README.md) | 简体中文

进程内、纯内存的事件总线实现，适合模块化单体或本地开发测试场景。事件发布后同步分发到当前进程内已订阅的处理器，无持久化、无跨进程能力。

## 特性
- 同步分发：`PublishAsync` 调用链内依次执行所有处理器。
- 事件名路由：支持 `EventNameAttribute`，未标注则使用事件类型名。
- DI 友好：处理器从 `IServiceProvider` 解析，自动创建作用域。
- 简单 API：只包含发布、订阅、取消订阅接口。

## 安装
- 在你的项目中引用 `Mubai.EventBus.InMemory` 包，或在解决方案中项目引用本库。

## 快速开始
```csharp
// 定义事件
public record OrderCreated(Guid OrderId) : IntegrationEvent(OrderId, DateTimeOffset.UtcNow);

// 定义处理器
public sealed class OrderCreatedHandler : IIntegrationEventHandler<OrderCreated>
{
    public Task HandleAsync(OrderCreated @event, CancellationToken ct = default)
    {
        // TODO: 业务逻辑
        return Task.CompletedTask;
    }
}

// 注册
services.AddInMemoryEventBus();
services.AddIntegrationEventHandlersFromAssemblyContaining<OrderCreatedHandler>();

// 发布
await eventBus.PublishAsync(new OrderCreated(orderId), ct);
```

## 事件名自定义
```csharp
[EventName("InventoryReserved")]
public record InventoryReservedEvent(Guid OrderId) : IntegrationEvent(OrderId, DateTimeOffset.UtcNow);
```
订阅与发布都使用同一个事件名；如果未标注 `EventNameAttribute`，则使用类型名。

## 注意事项
- 处理器同步执行，会阻塞发布方；将耗时 IO 或长事务放在业务层控制。
- 没有重试、失败回调、并行度或队列；失败会直接抛出回到调用方。
- 无跨进程/跨机器能力，进程退出后订阅与事件都会丢失。
