# Mubai.EventBus
English | [简体中文](./README.ZH-CN.md)

Event-driven building blocks for .NET: a transport-agnostic abstraction package (`Mubai.EventBus`) and an in-process implementation (`Mubai.EventBus.InMemory`) suitable for modular monoliths, local development, and testing.

## Packages
- **Mubai.EventBus**: contracts only - `IntegrationEvent`, `IEventBus`, `IIntegrationEventHandler<TEvent>`, and `EventNameAttribute`.
- **Mubai.EventBus.InMemory**: synchronous, in-process, in-memory event bus that routes by `EventNameAttribute` (fallback to type name); DI-friendly (scoped bus that resolves handlers from the caller scope); no persistence/retry/queue.
- **Mubai.EventBus.RabbitMQ**: RabbitMQ transport with topic/direct exchanges, routing key `{service}.{domain}.{event}`, JSON serializer carrying `x-version`/`x-schema` headers, DI extensions, and an Outbox dispatcher.

## Install
```bash
dotnet add package Mubai.EventBus
dotnet add package Mubai.EventBus.InMemory
```

## Quick start

### 1) Program.cs - register the in-memory bus and services
```csharp
var builder = WebApplication.CreateBuilder(args);
var services = builder.Services;

// DI registrations
services.AddInMemoryEventBus(); // IEventBus registered as scoped
services.AddIntegrationEventHandlersFromAssemblyContaining<OrderCreatedHandler>();
services.AddDbContext<OrdersDbContext>(options => options.UseInMemoryDatabase("orders"));
services.AddScoped<OrderService>();

var app = builder.Build();
// configure middleware, endpoints...
app.Run();
```

### 2) Usage (in-process, DI with transactional workflow)
```csharp
// Event
public record OrderCreated(Guid OrderId) : IntegrationEvent();

// Handler
public sealed class OrderCreatedHandler : IIntegrationEventHandler<OrderCreated>
{
    public Task HandleAsync(OrderCreated @event, CancellationToken ct = default)
    {
        // business logic
        return Task.CompletedTask;
    }
}

// Application service (constructor-injected DbContext + IEventBus)
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
        // Same scope: DbContext + IEventBus share the transaction boundary.
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

### RabbitMQ transport (topic + direct)
```csharp
services.AddRabbitMqEventBus(
    options =>
    {
        options.ServiceName = "orders";
        options.Domain = "sales";
        // options.TopicExchangeName = "events-topic";
        // options.DirectExchangeName = "events-direct";
        // options.QueueName = "orders.events";
    },
    connection =>
    {
        connection.HostName = "localhost";
        connection.UserName = "guest";
        connection.Password = "guest";
        connection.DispatchConsumersAsync = true;
    });
services.AddIntegrationEventHandlersFromAssemblyContaining<OrderCreatedHandler>();
```
事件名来自 `EventNameAttribute`（无则用类型名），发布/订阅路由键为 `{service}.{domain}.{event}`，Exchange 同时发布到 topic/direct，序列化为 JSON 并附带 `x-version`/`x-schema` 头。队列命名默认 `{service}.events`，启动时自动声明 exchange/queue/bindings（可通过 `RabbitMqOptions.DeclareTopology` 控制）。

## Event names
Use `EventNameAttribute` to define a stable name independent of the CLR type:
```csharp
[EventName("InventoryReserved")]
public record InventoryReserved(Guid OrderId) : IntegrationEvent();
```

## Notes
- `Mubai.EventBus` is contract-only; choose/implement an `IEventBus`. For in-process scenarios, use the in-memory package.
- The in-memory bus is registered as scoped and dispatches synchronously in the caller's context; resolve/use it within a scope and keep handlers lightweight.
- There is no persistence, queueing, retry, or cross-process support.

## License
MIT
