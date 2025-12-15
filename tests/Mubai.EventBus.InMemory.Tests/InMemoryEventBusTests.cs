using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
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
        using var provider = BuildProvider();
        var bus = (InMemoryEventBus)provider.GetRequiredService<IEventBus>();

        bus.Dispose();
        bus.Dispose();
    }

    [Fact]
    public async Task PublishAsync_InvokesTypedHandler()
    {
        using var provider = BuildProvider(services =>
        {
            services.AddLogging();
            services.AddSingleton<TestHandler>();
        });
        var bus = provider.GetRequiredService<IEventBus>();

        await bus.SubscribeAsync<TestEvent, TestHandler>();
        await bus.PublishAsync(new TestEvent("ping"));

        var handler = provider.GetRequiredService<TestHandler>();
        Assert.Equal(new[] { "ping" }, handler.Messages);
    }

    [Fact]
    public async Task UnsubscribeDelegate_StopsReceiving()
    {
        using var provider = BuildProvider();
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
        using var provider = BuildProvider(services =>
        {
            services.AddLogging();
            services.AddSingleton<TestHandler>();
        });
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
        using var provider = BuildProvider(services =>
        {
            services.AddLogging();
            services.AddSingleton<TestHandler>();
        });
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
        using var provider = BuildProvider();
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
        using var provider = BuildProvider();
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
        using var provider = BuildProvider();
        var bus = (InMemoryEventBus)provider.GetRequiredService<IEventBus>();

        bus.Dispose();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => bus.PublishAsync(new TestEvent("disposed")).AsTask());
    }

    [Fact]
    public async Task PublishAsync_FromScopedService_UsesSameScopeForHandlers()
    {
        using var provider = BuildProvider(services =>
        {
            services.AddLogging();
            services.AddDbContext<ShopDbContext>(opts => opts.UseInMemoryDatabase(Guid.NewGuid().ToString()));
            services.AddScoped<ScopedDependency>();
            services.AddScoped<OrderPlacedHandler>();
            services.AddScoped<OrderService>();
        });
        using var scope = provider.CreateScope();

        var bus = scope.ServiceProvider.GetRequiredService<IEventBus>();
        await bus.SubscribeAsync<OrderPlacedEvent, OrderPlacedHandler>();

        var service = scope.ServiceProvider.GetRequiredService<OrderService>();
        await service.PlaceOrderAsync("item-1");

        var scoped = scope.ServiceProvider.GetRequiredService<ScopedDependency>();
        Assert.Equal(new[] { "order:item-1" }, scoped.Messages);
        var db = scope.ServiceProvider.GetRequiredService<ShopDbContext>();
        Assert.Equal(1, await db.OrderLogs.CountAsync());
    }

    [Fact]
    public async Task PublishAsync_MultipleEvents_DispatchesAllHandlersWithinScope()
    {
        using var provider = BuildProvider(services =>
        {
            services.AddLogging();
            services.AddDbContext<ShopDbContext>(opts => opts.UseInMemoryDatabase(Guid.NewGuid().ToString()));
            services.AddScoped<ScopedDependency>();
            services.AddScoped<OrderPlacedHandler>();
            services.AddScoped<PaymentCapturedHandler>();
            services.AddScoped<CheckoutService>();
        });
        using var scope = provider.CreateScope();

        var bus = scope.ServiceProvider.GetRequiredService<IEventBus>();
        await bus.SubscribeAsync<OrderPlacedEvent, OrderPlacedHandler>();
        await bus.SubscribeAsync<PaymentCapturedEvent, PaymentCapturedHandler>();

        var service = scope.ServiceProvider.GetRequiredService<CheckoutService>();
        await service.PlaceAndPayAsync("item-2");

        var scoped = scope.ServiceProvider.GetRequiredService<ScopedDependency>();
        var db = scope.ServiceProvider.GetRequiredService<ShopDbContext>();
        Assert.Equal(new[] { "order:item-2", "payment:item-2" }, scoped.Messages);
        Assert.Equal(1, await db.Orders.CountAsync());
        Assert.Equal(1, await db.PaymentLogs.CountAsync());
    }

    private sealed record TestEvent(string Payload)
        : IntegrationEvent(Guid.NewGuid(), DateTimeOffset.UtcNow);

    [EventName("CustomNameEvent")]
    private sealed record NamedEvent(string Payload)
        : IntegrationEvent(Guid.NewGuid(), DateTimeOffset.UtcNow);

    private sealed record OrderPlacedEvent(string Item)
        : IntegrationEvent(Guid.NewGuid(), DateTimeOffset.UtcNow);

    private sealed record PaymentCapturedEvent(string Item)
        : IntegrationEvent(Guid.NewGuid(), DateTimeOffset.UtcNow);

    private sealed class OrderService
    {
        private readonly IEventBus _eventBus;

        public OrderService(IEventBus eventBus) => _eventBus = eventBus;

        public Task PlaceOrderAsync(string item, CancellationToken cancellationToken = default)
        {
            return _eventBus.PublishAsync(new OrderPlacedEvent(item), cancellationToken).AsTask();
        }
    }

    private sealed class CheckoutService
    {
        private readonly IEventBus _eventBus;
        private readonly ShopDbContext _dbContext;

        public CheckoutService(IEventBus eventBus, ShopDbContext dbContext)
        {
            _eventBus = eventBus;
            _dbContext = dbContext;
        }

        public async Task PlaceAndPayAsync(string item, CancellationToken cancellationToken = default)
        {
            _dbContext.Orders.Add(new Order { Id = Guid.NewGuid(), Item = item });
            await _dbContext.SaveChangesAsync(cancellationToken);

            await _eventBus.PublishAsync(new OrderPlacedEvent(item), cancellationToken).AsTask();
            await _eventBus.PublishAsync(new PaymentCapturedEvent(item), cancellationToken).AsTask();
        }
    }

    private sealed class OrderPlacedHandler : IIntegrationEventHandler<OrderPlacedEvent>
    {
        private readonly ScopedDependency _dependency;
        private readonly ShopDbContext _dbContext;

        public OrderPlacedHandler(ScopedDependency dependency, ShopDbContext dbContext)
        {
            _dependency = dependency;
            _dbContext = dbContext;
        }

        public Task HandleAsync(OrderPlacedEvent @event, CancellationToken cancellationToken = default)
        {
            _dependency.Messages.Add($"order:{@event.Item}");
            _dbContext.OrderLogs.Add(new OrderLog { Id = Guid.NewGuid(), Item = @event.Item });
            return _dbContext.SaveChangesAsync(cancellationToken);
        }
    }

    private sealed class PaymentCapturedHandler : IIntegrationEventHandler<PaymentCapturedEvent>
    {
        private readonly ScopedDependency _dependency;
        private readonly ShopDbContext _dbContext;

        public PaymentCapturedHandler(ScopedDependency dependency, ShopDbContext dbContext)
        {
            _dependency = dependency;
            _dbContext = dbContext;
        }

        public Task HandleAsync(PaymentCapturedEvent @event, CancellationToken cancellationToken = default)
        {
            _dependency.Messages.Add($"payment:{@event.Item}");
            _dbContext.PaymentLogs.Add(new PaymentLog { Id = Guid.NewGuid(), Item = @event.Item });
            return _dbContext.SaveChangesAsync(cancellationToken);
        }
    }

    private sealed class ScopedDependency
    {
        public List<string> Messages { get; } = new();
    }

    private static ServiceProvider BuildProvider(Action<IServiceCollection>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddInMemoryEventBus();
        configure?.Invoke(services);
        return services.BuildServiceProvider();
    }

    private sealed class ShopDbContext : DbContext
    {
        public ShopDbContext(DbContextOptions<ShopDbContext> options) : base(options)
        {
        }

        public DbSet<Order> Orders => Set<Order>();
        public DbSet<OrderLog> OrderLogs => Set<OrderLog>();
        public DbSet<PaymentLog> PaymentLogs => Set<PaymentLog>();
    }

    private sealed class Order
    {
        public Guid Id { get; set; }
        public string Item { get; set; } = string.Empty;
    }

    private sealed class PaymentLog
    {
        public Guid Id { get; set; }
        public string Item { get; set; } = string.Empty;
    }

    private sealed class OrderLog
    {
        public Guid Id { get; set; }
        public string Item { get; set; } = string.Empty;
    }

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
