# Mubai.EventBus
English | [简体中文](./README.ZH-CN.md)

Core abstractions for an event-driven architecture in .NET: event base types, handler contracts, and a minimal `IEventBus` interface. Designed for modular monoliths and integration-style messaging without prescribing a specific transport.

## Highlights
- Simple contracts: `IntegrationEvent`, `IEventBus`, `IIntegrationEventHandler<TEvent>`.
- Event name routing: optional `EventNameAttribute` to decouple wire names from CLR type names.
- Transport-agnostic: implement your own bus or pair with `Mubai.EventBus.InMemory` for in-process dispatch.
- DI friendly: handlers are designed to be resolved via dependency injection.

## Install
```bash
dotnet add package Mubai.EventBus
```

## Quick start
Define an event:
```csharp
using Mubai.EventBus.Events;

public record OrderCreated(Guid OrderId)
    : IntegrationEvent(); // uses default Id/OccurredOn
```

Implement a handler:
```csharp
using Mubai.EventBus.Abstractions;

public sealed class OrderCreatedHandler : IIntegrationEventHandler<OrderCreated>
{
    public Task HandleAsync(OrderCreated @event, CancellationToken ct = default)
    {
        // business logic
        return Task.CompletedTask;
    }
}
```

Publish via an `IEventBus` implementation (e.g., Mubai.EventBus.InMemory):
```csharp
await eventBus.PublishAsync(new OrderCreated(orderId), ct);
```

## Event names
Use `EventNameAttribute` when you need a stable name independent of the CLR type:
```csharp
[EventName("InventoryReserved")]
public record InventoryReserved(Guid OrderId) : IntegrationEvent();
```

## Notes
- This package only contains contracts. Choose or implement an `IEventBus` to send/receive events.
- For in-process scenarios, use `Mubai.EventBus.InMemory`.
- `IntegrationEvent` has a default constructor that sets a new `Id` and `OccurredOn` timestamp; you can still pass explicit values via the parameterized constructor.

## 中文
请参考仓库中的 `README.zh-CN.md` 获取中文说明。
