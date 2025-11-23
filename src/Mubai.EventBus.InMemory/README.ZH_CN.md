# Mubai.EventBus.InMemory
[English](./README.md) | 简体中文

进程内、纯内存的事件总线实现，适合模块化单体或本地开发测试场景。事件发布后同步分发到当前进程内已订阅的处理器，无持久化、无跨进程能力。

## 亮点
- 同步分发：`PublishAsync` 调用链内依次执行所有处理器。
- 事件名路由：支持 `EventNameAttribute`，未标注则使用事件类型名。
- DI 友好：总线注册为 Scoped，处理器从当前作用域解析（不会为每次调用额外创建作用域）。
- 简洁 API：发布、订阅、取消订阅。

## 安装
在项目中引用 `Mubai.EventBus.InMemory` 包（或在解决方案中项目引用）。

## 快速开始

### 1) Program.cs - 注册总线与服务
```csharp
var builder = WebApplication.CreateBuilder(args);
var services = builder.Services;

services.AddInMemoryEventBus(); // IEventBus 注册为 Scoped
services.AddIntegrationEventHandlersFromAssemblyContaining<OrderCreatedHandler>();
services.AddDbContext<OrdersDbContext>(options => options.UseInMemoryDatabase("orders"));
services.AddScoped<OrderService>();

var app = builder.Build();
app.Run();
```

### 2) 使用（DI + 事务流程）
```csharp
// 事件
public record OrderCreated(Guid OrderId) : IntegrationEvent(OrderId, DateTimeOffset.UtcNow);

// 处理器
public sealed class OrderCreatedHandler : IIntegrationEventHandler<OrderCreated>
{
    public Task HandleAsync(OrderCreated @event, CancellationToken ct = default)
    {
        return Task.CompletedTask;
    }
}

// 业务服务
public sealed class OrderService
{
    private readonly IEventBus _eventBus;
    private readonly OrdersDbContext _db;

    public OrderService(IEventBus eventBus, OrdersDbContext db)
    {
        _eventBus = eventBus;
        _db = db;
    }

    public async Task PlaceAsync(Guid orderId, CancellationToken ct = default)
    {
        await _db.Database.BeginTransactionAsync(ct);
        _db.Orders.Add(new Order(orderId));
        await _db.SaveChangesAsync(ct);
        await _eventBus.PublishAsync(new OrderCreated(orderId), ct);
        await _db.Database.CommitTransactionAsync(ct);
    }
}

public sealed class OrdersDbContext : DbContext
{
    public OrdersDbContext(DbContextOptions<OrdersDbContext> options) : base(options) { }
    public DbSet<Order> Orders => Set<Order>();
}

public sealed record Order(Guid Id);
```

## 事件名自定义
```csharp
[EventName("InventoryReserved")]
public record InventoryReservedEvent(Guid OrderId) : IntegrationEvent(OrderId, DateTimeOffset.UtcNow);
```
订阅与发布需使用同一事件名；未标注 `EventNameAttribute` 时默认使用类型名。

## 注意事项
- 总线为 Scoped，同步在调用方上下文执行；保持处理器轻量，避免长时间阻塞。
- 无持久化、无队列、无重试、无跨进程支持；异常会直接抛回调用方。
