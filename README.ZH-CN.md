# Mubai.EventBus
[English](./README.md) | 简体中文

面向 .NET 的事件驱动基础设施：包含传输无关的抽象包（`Mubai.EventBus`）和进程内实现（`Mubai.EventBus.InMemory`），适合模块化单体、本地开发与测试。

## 包结构
- **Mubai.EventBus**：仅含契约 —— `IntegrationEvent`、`IEventBus`、`IIntegrationEventHandler<TEvent>`、`EventNameAttribute`。
- **Mubai.EventBus.InMemory**：进程内、同步分发的内存事件总线，按 `EventNameAttribute` 路由（默认类型名），DI 友好，无持久化/重试/队列。

## 安装
```bash
dotnet add package Mubai.EventBus
dotnet add package Mubai.EventBus.InMemory
```

## 快速开始（进程内）
```csharp
using Mubai.EventBus.Abstractions;
using Mubai.EventBus.Events;
using Mubai.EventBus.InMemory;

// 事件
public record OrderCreated(Guid OrderId) : IntegrationEvent();

// 处理器
public sealed class OrderCreatedHandler : IIntegrationEventHandler<OrderCreated>
{
    public Task HandleAsync(OrderCreated @event, CancellationToken ct = default)
    {
        // 业务逻辑
        return Task.CompletedTask;
    }
}

// 注册
var services = new ServiceCollection();
services.AddInMemoryEventBus();
services.AddIntegrationEventHandlersFromAssemblyContaining<OrderCreatedHandler>();
var provider = services.BuildServiceProvider();
var bus = provider.GetRequiredService<IEventBus>();

// 发布
await bus.PublishAsync(new OrderCreated(orderId), ct);
```

## 事件名
通过 `EventNameAttribute` 自定义稳定事件名：
```csharp
[EventName("InventoryReserved")]
public record InventoryReserved(Guid OrderId) : IntegrationEvent();
```

## 说明
- `Mubai.EventBus` 仅提供抽象，需要选择或实现 `IEventBus`。进程内可用 `Mubai.EventBus.InMemory`。
>- 内存总线为同步分发，会阻塞调用方；请保持处理器轻量。
- 无持久化、无队列、无重试、无跨进程支持。

## 许可
MIT
