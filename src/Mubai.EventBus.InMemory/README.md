# Mubai.EventBus.InMemory
English | [简体中文](./README.ZH_CN.md)

In-process, in-memory event bus for modular monoliths or local development/testing. Events are dispatched synchronously to handlers registered in the current process. No persistence, no cross-process capability.

## Highlights
- Synchronous dispatch: `PublishAsync` executes all handlers in-call.
- Event name routing: honors `EventNameAttribute`; falls back to the event type name.
- DI friendly: bus is registered as scoped; handlers resolve from the current scope (no extra scope per invocation).
- Minimal API: publish, subscribe, unsubscribe.

## Install
Reference the `Mubai.EventBus.InMemory` package (or project reference inside the solution).

## Quick start

### 1) Program.cs - register the in-memory bus and services
```csharp
var builder = WebApplication.CreateBuilder(args);
var services = builder.Services;

services.AddInMemoryEventBus(); // IEventBus registered as scoped
services.AddIntegrationEventHandlersFromAssemblyContaining<OrderCreatedHandler>();
services.AddDbContext<OrdersDbContext>(options => options.UseInMemoryDatabase("orders"));
services.AddScoped<OrderService>();

var app = builder.Build();
app.Run();
```

### 2) Usage (DI with transactional workflow)
```csharp
// Event
public record OrderCreated(Guid OrderId) : IntegrationEvent(OrderId, DateTimeOffset.UtcNow);

// Handler
public sealed class OrderCreatedHandler : IIntegrationEventHandler<OrderCreated>
{
    public Task HandleAsync(OrderCreated @event, CancellationToken ct = default)
    {
        return Task.CompletedTask;
    }
}

// Application service
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

## Custom event names
```csharp
[EventName("InventoryReserved")]
public record InventoryReservedEvent(Guid OrderId) : IntegrationEvent(OrderId, DateTimeOffset.UtcNow);
```
Publisher and subscriber must use the same event name; if the attribute is omitted, the type name is used.

## Notes
- Registered as `Scoped`; resolve and use the bus within a scope so handlers share the same DI scope. No per-handler scope is created automatically.
- Handlers run synchronously and will block the publisher; keep handler work light to avoid long transactions.
- There is no retry, failure callback, queue, or parallel fan-out. Exceptions bubble up to the caller.
- No cross-process/machine support; subscriptions and events are lost when the process exits.
