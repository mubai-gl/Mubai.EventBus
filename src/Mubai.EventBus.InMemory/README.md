# Mubai.EventBus.InMemory
English | [简体中文](./README.ZH-CN.md)

In-process, in-memory event bus for modular monoliths or local development/testing. Events are dispatched synchronously to handlers registered in the current process. No persistence, no cross-process capability.

## Highlights
- Synchronous dispatch: `PublishAsync` executes all handlers in-call.
- Event name routing: honors `EventNameAttribute`; falls back to the event type name.
- DI friendly: handlers are resolved from `IServiceProvider` with a scoped lifetime per handler invocation.
- Minimal API: publish, subscribe, unsubscribe.

## Install
Reference the `Mubai.EventBus.InMemory` package (or project reference inside the solution).

## Quick start
```csharp
// Event
public record OrderCreated(Guid OrderId) : IntegrationEvent(OrderId, DateTimeOffset.UtcNow);

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
services.AddInMemoryEventBus();
services.AddIntegrationEventHandlersFromAssemblyContaining<OrderCreatedHandler>();

// Publish
await eventBus.PublishAsync(new OrderCreated(orderId), ct);
```

## Custom event names
```csharp
[EventName("InventoryReserved")]
public record InventoryReservedEvent(Guid OrderId) : IntegrationEvent(OrderId, DateTimeOffset.UtcNow);
```
Publisher and subscriber must use the same event name; if the attribute is omitted, the type name is used.

## Notes
- Handlers run synchronously and will block the publisher; keep handler work light to avoid long transactions.
- There is no retry, failure callback, queue, or parallel fan-out. Exceptions bubble up to the caller.
- No cross-process/machine support; subscriptions and events are lost when the process exits.
