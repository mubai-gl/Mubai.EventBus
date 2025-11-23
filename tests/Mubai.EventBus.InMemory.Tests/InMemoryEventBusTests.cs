using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Mubai.EventBus.Abstractions;
using Mubai.EventBus.Events;
using Mubai.EventBus.InMemory;
using Xunit;

namespace Mubai.EventBus.InMemory.Tests;

public class InMemoryEventBusTests
{
    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var services = new ServiceCollection();
        services.AddInMemoryEventBus();
        var provider = services.BuildServiceProvider();
        var bus = (InMemoryEventBus)provider.GetRequiredService<IEventBus>();

        bus.Dispose();
        bus.Dispose();
    }

    [Fact]
    public async Task PublishAsync_InvokesTypedHandler()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInMemoryEventBus();
        services.AddSingleton<TestHandler>();

        var provider = services.BuildServiceProvider();
        var bus = provider.GetRequiredService<IEventBus>();

        await bus.SubscribeAsync<TestEvent, TestHandler>();
        await bus.PublishAsync(new TestEvent("ping"));

        var handler = provider.GetRequiredService<TestHandler>();
        Assert.Equal(new[] { "ping" }, handler.Messages);
    }

    [Fact]
    public async Task UnsubscribeDelegate_StopsReceiving()
    {
        var services = new ServiceCollection();
        services.AddInMemoryEventBus();
        var provider = services.BuildServiceProvider();
        var bus = provider.GetRequiredService<IEventBus>();

        var count = 0;

        ValueTask Handler(TestEvent evt, CancellationToken token)
        {
            Interlocked.Increment(ref count);
            return ValueTask.CompletedTask;
        }

        await bus.SubscribeAsync<TestEvent>(Handler);
        await bus.PublishAsync(new TestEvent("first"));

        await bus.UnsubscribeAsync<TestEvent>(Handler);
        await bus.PublishAsync(new TestEvent("second"));

        Assert.Equal(1, count);
    }

    [Fact]
    public async Task UnsubscribeTypedHandler_PreventsFurtherDelivery()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInMemoryEventBus();
        services.AddSingleton<TestHandler>();

        var provider = services.BuildServiceProvider();
        var bus = provider.GetRequiredService<IEventBus>();

        await bus.SubscribeAsync<TestEvent, TestHandler>();
        await bus.UnsubscribeAsync<TestEvent, TestHandler>();

        await bus.PublishAsync(new TestEvent("ping"));

        var handler = provider.GetRequiredService<TestHandler>();
        Assert.Empty(handler.Messages);
    }

    [Fact]
    public async Task DisposingSubscriptionHandle_UnsubscribesTypedHandler()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInMemoryEventBus();
        services.AddSingleton<TestHandler>();

        var provider = services.BuildServiceProvider();
        var bus = provider.GetRequiredService<IEventBus>();

        var subscription = await bus.SubscribeAsync<TestEvent, TestHandler>();
        subscription.Dispose();

        await bus.PublishAsync(new TestEvent("ping"));

        var handler = provider.GetRequiredService<TestHandler>();
        Assert.Empty(handler.Messages);
    }

    [Fact]
    public async Task DisposingInlineSubscriptionHandle_UnsubscribesDelegate()
    {
        var services = new ServiceCollection();
        services.AddInMemoryEventBus();
        var provider = services.BuildServiceProvider();
        var bus = provider.GetRequiredService<IEventBus>();

        var count = 0;
        ValueTask Handler(TestEvent evt, CancellationToken token)
        {
            Interlocked.Increment(ref count);
            return ValueTask.CompletedTask;
        }

        var subscription = await bus.SubscribeAsync<TestEvent>(Handler);
        subscription.Dispose();

        await bus.PublishAsync(new TestEvent("ignored"));

        Assert.Equal(0, count);
    }

    [Fact]
    public async Task PublishAsync_UsesEventNameAttributeForRouting()
    {
        var services = new ServiceCollection();
        services.AddInMemoryEventBus();
        var provider = services.BuildServiceProvider();
        var bus = provider.GetRequiredService<IEventBus>();

        var hit = 0;
        await bus.SubscribeAsync<NamedEvent>((evt, token) =>
        {
            Interlocked.Increment(ref hit);
            return ValueTask.CompletedTask;
        });

        await bus.PublishAsync(new NamedEvent("named"));

        Assert.Equal(1, hit);
    }

    [Fact]
    public async Task PublishAsync_AfterDisposal_ThrowsObjectDisposedException()
    {
        var services = new ServiceCollection();
        services.AddInMemoryEventBus();
        var provider = services.BuildServiceProvider();
        var bus = (InMemoryEventBus)provider.GetRequiredService<IEventBus>();

        bus.Dispose();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => bus.PublishAsync(new TestEvent("disposed")).AsTask());
    }

    private sealed record TestEvent(string Payload)
        : IntegrationEvent(Guid.NewGuid(), DateTimeOffset.UtcNow);

    [EventName("CustomNameEvent")]
    private sealed record NamedEvent(string Payload)
        : IntegrationEvent(Guid.NewGuid(), DateTimeOffset.UtcNow);

    private sealed class TestHandler : IIntegrationEventHandler<TestEvent>
    {
        public List<string> Messages { get; } = new();

        public Task HandleAsync(TestEvent @event, CancellationToken cancellationToken = default)
        {
            Messages.Add(@event.Payload);
            return Task.CompletedTask;
        }
    }
}
