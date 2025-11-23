# Mubai.EventBus
[English](./README.md) | 简体中文

面向 .NET 的事件驱动基础设施：包含传输无关的抽象包（`Mubai.EventBus`）和进程内实现（`Mubai.EventBus.InMemory`），适合模块化单体、本地开发与测试。

## 包概览
- **Mubai.EventBus**：仅含契约——`IntegrationEvent`、`IEventBus`、`IIntegrationEventHandler<TEvent>`、`EventNameAttribute`。
- **Mubai.EventBus.InMemory**：进程内、同步分发的内存事件总线，按 `EventNameAttribute` 路由（默认类型名），DI 友好（总线为 Scoped，处理器从调用方作用域解析），无持久化/重试/队列。

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

// 领域服务示例（与 UoW/仓储共享同一作用域，构造函数注入 IEventBus）
public sealed class OrderService : IOrderService
{
    private readonly IOrderRepository _orders;
    private readonly IInventoryRepository _inventory;
    private readonly IUnitOfWork<ShopDbContext> _uow;
    private readonly IEventBus _eventBus;

    public OrderService(
        IOrderRepository orders,
        IInventoryRepository inventory,
        IUnitOfWork<ShopDbContext> uow,
        IEventBus eventBus)
    {
        _orders = orders;
        _inventory = inventory;
        _uow = uow;
        _eventBus = eventBus;
    }

    public async Task PlaceOrderAsync(PlaceOrderRequestDto request, CancellationToken token = default)
    {
        await _uow.ExecuteInTransactionAsync(async ct =>
        {
            var order = BuildOrder(request);
            await _orders.AddAsync(order, ct);

            await _eventBus.PublishAsync(new AIntegrationEvent(order.Id), ct);
            await _eventBus.PublishAsync(new BIntegrationEvent(order.Id), ct);
            await _uow.SaveChangesAsync(ct);
        }, token);
    }
}

// 注册
var services = new ServiceCollection();
services.AddInMemoryEventBus();
services.AddIntegrationEventHandlersFromAssemblyContaining<OrderCreatedHandler>();
services.AddScoped<IOrderService, OrderService>();
```

## 事件名
使用 `EventNameAttribute` 定义稳定事件名：
```csharp
[EventName("InventoryReserved")]
public record InventoryReserved(Guid OrderId) : IntegrationEvent();
```

## 说明
- `Mubai.EventBus` 仅提供抽象，需要选择或实现 `IEventBus`；进程内可用内存总线。
- 内存总线注册为 Scoped，并在调用上下文内同步分发；保持处理器轻量，避免长时间阻塞调用方。
- 无持久化、无队列、无重试、无跨进程支持。

## 许可
MIT
