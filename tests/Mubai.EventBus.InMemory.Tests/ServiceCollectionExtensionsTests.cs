using Microsoft.Extensions.DependencyInjection;
using Mubai.EventBus.Abstractions;
using Mubai.EventBus.Events;
using Mubai.EventBus.InMemory;
using Xunit;

namespace Mubai.EventBus.InMemory.Tests;

public class ServiceCollectionExtensionsTests
{
    [Fact]
    public void AddInMemoryEventBus_RegistersSingleton()
    {
        var services = new ServiceCollection();
        services.AddInMemoryEventBus();

        var provider = services.BuildServiceProvider();

        var bus1 = provider.GetRequiredService<IEventBus>();
        var bus2 = provider.GetRequiredService<IEventBus>();

        Assert.Same(bus1, bus2);
    }

    [Fact]
    public void AddIntegrationEventHandlersFromAssemblies_RegistersHandlers()
    {
        var services = new ServiceCollection();
        services.AddIntegrationEventHandlersFromAssemblyContaining<RegisteredHandler>();

        var provider = services.BuildServiceProvider();
        var handler = provider.GetRequiredService<IIntegrationEventHandler<RegisteredEvent>>();

        Assert.IsType<RegisteredHandler>(handler);
    }

    private sealed record RegisteredEvent()
        : IntegrationEvent(Guid.NewGuid(), DateTimeOffset.UtcNow);

    private sealed class RegisteredHandler : IIntegrationEventHandler<RegisteredEvent>
    {
        public Task HandleAsync(RegisteredEvent @event, CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }
    }
}

