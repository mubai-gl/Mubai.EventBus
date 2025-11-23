# Mubai.EventBus
[English](./README.md) | 简体中文

.NET 事件驱动架构的核心抽象，包含事件基类、处理器契约和最小化的 `IEventBus` 接口。适合模块化单体或集成场景，不绑定具体传输方式。

## 特性
- 简单契约：`IntegrationEvent`、`IEventBus`、`IIntegrationEventHandler<TEvent>`。
- 事件名路由：`EventNameAttribute` 可让事件名与 CLR 类型解耦。
- 传输无关：可自定义实现，也可搭配 `Mubai.EventBus.InMemory` 做进程内分发。
- DI 友好：处理器面向依赖注入设计。

## 安装
```bash
dotnet add package Mubai.EventBus
```

## 快速开始
定义事件：
```csharp
using Mubai.EventBus.Events;

public record OrderCreated(Guid OrderId)
    : IntegrationEvent(); // 默认生成 Id 和时间戳
```

定义处理器：
```csharp
using Mubai.EventBus.Abstractions;

public sealed class OrderCreatedHandler : IIntegrationEventHandler<OrderCreated>
{
    public Task HandleAsync(OrderCreated @event, CancellationToken ct = default)
    {
        // 业务逻辑
        return Task.CompletedTask;
    }
}
```

通过 `IEventBus` 发布（例如使用 Mubai.EventBus.InMemory 实现）：
```csharp
await eventBus.PublishAsync(new OrderCreated(orderId), ct);
```

## 事件名
需要稳定事件名时使用 `EventNameAttribute`：
```csharp
[EventName("InventoryReserved")]
public record InventoryReserved(Guid OrderId) : IntegrationEvent();
```

## 说明
- 本包只提供抽象，需要选择或实现 `IEventBus` 才能发送/接收事件。
- 进程内场景可使用 `Mubai.EventBus.InMemory`。
- `IntegrationEvent` 提供无参构造（自动生成 Id/时间戳）和带参构造供自定义。
