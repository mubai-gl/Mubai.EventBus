using Microsoft.Extensions.DependencyInjection;
using Mubai.EventBus.Abstractions;
using Mubai.EventBus.InMemory;
using Xunit;

namespace Mubai.EventBus.InMemory.Tests;

public class InMemoryEventBusTests
{
    [Fact]
    public async Task PublishAsync_InvokesTypedHandler()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInMemoryEventBus();
        services.AddSingleton<TestHandler>();

        var provider = services.BuildServiceProvider();
        var bus = provider.GetRequiredService<IEventBus>();

        using var _ = bus.Subscribe<TestEvent, TestHandler>();

        await bus.PublishAsync(new TestEvent("ping"));

        var handler = provider.GetRequiredService<TestHandler>();
        Assert.Equal(new[] { "ping" }, handler.Messages);
    }

    [Fact]
    public async Task DisposedSubscription_StopsReceiving()
    {
        var services = new ServiceCollection();
        services.AddInMemoryEventBus();
        var provider = services.BuildServiceProvider();
        var bus = provider.GetRequiredService<IEventBus>();

        var count = 0;

        using (bus.Subscribe<TestEvent>((evt, _) =>
               {
                   Interlocked.Increment(ref count);
                   return Task.CompletedTask;
               }))
        {
            await bus.PublishAsync(new TestEvent("first"));
        }

        await bus.PublishAsync(new TestEvent("second"));

        Assert.Equal(1, count);
    }

    private sealed record TestEvent(string Payload);

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
