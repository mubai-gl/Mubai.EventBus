# Mubai.EventBus
English | [简体中文](./README.ZH-CN.md)

Event-driven building blocks for .NET: a transport-agnostic abstraction package (`Mubai.EventBus`) and an in-process implementation (`Mubai.EventBus.InMemory`) suitable for modular monoliths, local development, and testing.

## Packages
- **Mubai.EventBus**: contracts only — `IntegrationEvent`, `IEventBus`, `IIntegrationEventHandler<TEvent>`, and `EventNameAttribute`.
- **Mubai.EventBus.InMemory**: synchronous, in-process, in-memory event bus that routes by `EventNameAttribute` (fallback to type name); DI-friendly; no persistence/retry/queue.

## Install
```bash
dotnet add package Mubai.EventBus
dotnet add package Mubai.EventBus.InMemory
```

## Quick start (in-process)
```csharp
using Mubai.EventBus.Abstractions;
using Mubai.EventBus.Events;
using Mubai.EventBus.InMemory;

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

// Registration
var services = new ServiceCollection();
services.AddInMemoryEventBus();
services.AddIntegrationEventHandlersFromAssemblyContaining<OrderCreatedHandler>();
var provider = services.BuildServiceProvider();
var bus = provider.GetRequiredService<IEventBus>();

// Publish
await bus.PublishAsync(new OrderCreated(orderId), ct);
```

## Event names
Use `EventNameAttribute` to define a stable name independent of the CLR type:
```csharp
[EventName("InventoryReserved")]
public record InventoryReserved(Guid OrderId) : IntegrationEvent();
```

## Notes
- `Mubai.EventBus` is contract-only; choose/implement an `IEventBus`. For in-process scenarios, use the in-memory package.
- The in-memory bus dispatches synchronously in the caller’s context; keep handlers lightweight.
- There is no persistence, queueing, retry, or cross-process support.

## License
MIT
