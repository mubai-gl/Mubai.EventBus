using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Mubai.EventBus.Abstractions;
using Mubai.EventBus.Events;
using Mubai.EventBus.InMemory;
using Mubai.EventBus.Exceptions;
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
    public async Task PublishAsync_RetriesUntilSuccess_WhenHandlerEventuallySucceeds()
    {
        var services = new ServiceCollection();
        services.AddInMemoryEventBus(options =>
        {
            options.MaxRetryAttempts = 3;
            options.InitialRetryDelay = TimeSpan.Zero;
            options.ShouldRetry = _ => true;
        });

        var provider = services.BuildServiceProvider();
        var bus = provider.GetRequiredService<IEventBus>();

        var attempts = 0;
        await bus.SubscribeAsync<TestEvent>((evt, token) =>
        {
            attempts++;
            if (attempts < 2)
            {
                throw new InvalidOperationException("try again");
            }

            return ValueTask.CompletedTask;
        });

        await bus.PublishAsync(new TestEvent("retry"));

        Assert.Equal(2, attempts);
    }

    [Fact]
    public async Task PublishAsync_InvokesOnHandlerFailedAfterRetriesExhausted()
    {
        var services = new ServiceCollection();
        IntegrationEvent? failedEvent = null;
        int failedAttempt = 0;

        services.AddInMemoryEventBus(options =>
        {
            options.MaxRetryAttempts = 2;
            options.InitialRetryDelay = TimeSpan.Zero;
            options.ShouldRetry = _ => true;
            options.OnHandlerFailed = (evt, ex, attempt) =>
            {
                failedEvent = evt;
                failedAttempt = attempt;
            };
        });

        var provider = services.BuildServiceProvider();
        var bus = provider.GetRequiredService<IEventBus>();

        await bus.SubscribeAsync<TestEvent>((evt, token) => throw new InvalidOperationException("boom"));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => bus.PublishAsync(new TestEvent("boom")).AsTask());
        Assert.Equal("boom", ex.Message);
        Assert.NotNull(failedEvent);
        Assert.Equal(2, failedAttempt);
    }

    [Fact]
    public async Task PublishAsync_DefaultShouldRetry_RetriesTimeoutException()
    {
        var services = new ServiceCollection();
        services.AddInMemoryEventBus(options =>
        {
            options.MaxRetryAttempts = 2;
            options.InitialRetryDelay = TimeSpan.Zero;
            // Keep ShouldRetry null to use default.
        });

        var provider = services.BuildServiceProvider();
        var bus = provider.GetRequiredService<IEventBus>();

        var attempts = 0;
        await bus.SubscribeAsync<TestEvent>((evt, token) =>
        {
            attempts++;
            throw new TimeoutException("transient");
        });

        await Assert.ThrowsAsync<TimeoutException>(() => bus.PublishAsync(new TestEvent("timeout")).AsTask());
        Assert.Equal(2, attempts);
    }

    [Fact]
    public async Task PublishAsync_UsesExponentialBackoffBetweenAttempts()
    {
        var services = new ServiceCollection();
        services.AddInMemoryEventBus(options =>
        {
            options.MaxRetryAttempts = 3;
            options.InitialRetryDelay = TimeSpan.FromMilliseconds(5);
            options.BackoffFactor = 2;
            options.UseExponentialBackoff = true;
            options.ShouldRetry = _ => true;
        });

        var provider = services.BuildServiceProvider();
        var bus = provider.GetRequiredService<IEventBus>();

        var attempts = 0;
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        await bus.SubscribeAsync<TestEvent>((evt, token) =>
        {
            attempts++;
            if (attempts < 3)
            {
                throw new InvalidOperationException("retry");
            }

            return ValueTask.CompletedTask;
        });

        await bus.PublishAsync(new TestEvent("backoff"));
        stopwatch.Stop();

        // Expected delay: 5ms + 10ms = 15ms (first attempt has no delay).
        Assert.True(stopwatch.ElapsedMilliseconds >= 12, $"Elapsed {stopwatch.ElapsedMilliseconds}ms was shorter than expected backoff.");
        Assert.Equal(3, attempts);
    }

    [Fact]
    public async Task PublishAsync_HonorsNormalizedMaxRetryAttemptsMinimum()
    {
        var services = new ServiceCollection();
        int failedAttempt = 0;

        services.AddInMemoryEventBus(options =>
        {
            options.MaxRetryAttempts = 0; // normalized to 1
            options.InitialRetryDelay = TimeSpan.Zero;
            options.ShouldRetry = _ => true;
            options.OnHandlerFailed = (_, _, attempt) => failedAttempt = attempt;
        });

        var provider = services.BuildServiceProvider();
        var bus = provider.GetRequiredService<IEventBus>();

        await bus.SubscribeAsync<TestEvent>((evt, token) => throw new TimeoutException("fail once"));

        await Assert.ThrowsAsync<TimeoutException>(() => bus.PublishAsync(new TestEvent("once")).AsTask());
        Assert.Equal(1, failedAttempt);
    }

    [Fact]
    public async Task PublishAsync_SameEventIdDeliveredOnlyOnce()
    {
        var services = new ServiceCollection();
        services.AddInMemoryEventBus();
        var provider = services.BuildServiceProvider();
        var bus = provider.GetRequiredService<IEventBus>();

        var deliveries = 0;
        var sharedEvent = new DuplicateEvent(Guid.NewGuid());

        await bus.SubscribeAsync<DuplicateEvent>((evt, token) =>
        {
            Interlocked.Increment(ref deliveries);
            return ValueTask.CompletedTask;
        });

        await bus.PublishAsync(sharedEvent);
        await bus.PublishAsync(sharedEvent);

        Assert.Equal(1, deliveries);
    }

    [Fact]
    public async Task PublishAsync_FailureAllowsRetryingSameEvent()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInMemoryEventBus(options =>
        {
            options.MaxRetryAttempts = 1;
            options.InitialRetryDelay = TimeSpan.Zero;
            options.ShouldRetry = _ => false;
        });
        var provider = services.BuildServiceProvider();
        var bus = provider.GetRequiredService<IEventBus>();

        var attempts = 0;
        var shouldFail = true;
        await bus.SubscribeAsync<TestEvent>((evt, token) =>
        {
            Interlocked.Increment(ref attempts);
            if (shouldFail)
            {
                throw new InvalidOperationException("boom");
            }

            return ValueTask.CompletedTask;
        });

        var evtInstance = new TestEvent("first");
        await Assert.ThrowsAsync<InvalidOperationException>(() => bus.PublishAsync(evtInstance).AsTask());

        shouldFail = false;
        await bus.PublishAsync(evtInstance);

        Assert.Equal(2, attempts);
    }

    [Fact]
    public async Task PublishAsync_IdempotenceCanBeDisabled()
    {
        var services = new ServiceCollection();
        services.AddInMemoryEventBus(options => options.EnableIdempotence = false);

        var provider = services.BuildServiceProvider();
        var bus = provider.GetRequiredService<IEventBus>();

        var deliveries = 0;
        var sharedEvent = new DuplicateEvent(Guid.NewGuid());
        await bus.SubscribeAsync<DuplicateEvent>((evt, token) =>
        {
            Interlocked.Increment(ref deliveries);
            return ValueTask.CompletedTask;
        });

        await bus.PublishAsync(sharedEvent);
        await bus.PublishAsync(sharedEvent);

        Assert.Equal(2, deliveries);
    }

    [Fact]
    public async Task PublishAsync_MaxParallelHandlersRespected()
    {
        var services = new ServiceCollection();
        services.AddInMemoryEventBus(options =>
        {
            options.MaxParallelHandlers = 2;
        });

        var provider = services.BuildServiceProvider();
        var bus = provider.GetRequiredService<IEventBus>();

        var current = 0;
        var maxObserved = 0;

        async ValueTask Handler(TestEvent evt, CancellationToken token)
        {
            var inFlight = Interlocked.Increment(ref current);
            var snapshot = maxObserved;
            while (inFlight > snapshot)
            {
                var previous = Interlocked.CompareExchange(ref maxObserved, inFlight, snapshot);
                if (previous == snapshot)
                {
                    break;
                }

                snapshot = previous;
            }

            try
            {
                await Task.Delay(50, token);
            }
            finally
            {
                Interlocked.Decrement(ref current);
            }
        }

        for (var i = 0; i < 5; i++)
        {
            await bus.SubscribeAsync<TestEvent>(Handler);
        }

        await bus.PublishAsync(new TestEvent("parallel"));

        Assert.InRange(maxObserved, 1, 2);
    }

    [Fact]
    public async Task PublishAsync_NonRetryableException_StopsRetry()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInMemoryEventBus(options =>
        {
            options.ShouldRetry = _ => true;
            options.MaxRetryAttempts = 5;
            options.InitialRetryDelay = TimeSpan.Zero;
        });

        var provider = services.BuildServiceProvider();
        var bus = provider.GetRequiredService<IEventBus>();

        var attempts = 0;
        await bus.SubscribeAsync<TestEvent>((evt, token) =>
        {
            Interlocked.Increment(ref attempts);
            throw new NonRetryableException("stop");
        });

        await Assert.ThrowsAsync<NonRetryableException>(() => bus.PublishAsync(new TestEvent("non-retry")).AsTask());
        Assert.Equal(1, attempts);
    }

    [Fact]
    public async Task PublishAsync_UsesCustomSerializerOptions()
    {
        var services = new ServiceCollection();
        services.AddInMemoryEventBus(options =>
        {
            options.SerializerOptions.PropertyNameCaseInsensitive = true;
        });
        var provider = services.BuildServiceProvider();
        var bus = provider.GetRequiredService<IEventBus>();

        string? observed = null;
        await bus.SubscribeAsync<AlternatePayloadEvent>((evt, token) =>
        {
            observed = evt.payload;
            return ValueTask.CompletedTask;
        });

        await bus.PublishAsync(new PascalPayloadEvent("UPPER"));

        Assert.Equal("UPPER", observed);
    }

    [Fact]
    public async Task PublishAsync_MultipleHandlers_AllReceiveEvent()
    {
        var services = new ServiceCollection();
        services.AddInMemoryEventBus();
        var provider = services.BuildServiceProvider();
        var bus = provider.GetRequiredService<IEventBus>();

        var count1 = 0;
        var count2 = 0;

        await bus.SubscribeAsync<TestEvent>((evt, token) =>
        {
            Interlocked.Increment(ref count1);
            return ValueTask.CompletedTask;
        });

        await bus.SubscribeAsync<TestEvent>((evt, token) =>
        {
            Interlocked.Increment(ref count2);
            return ValueTask.CompletedTask;
        });

        await bus.PublishAsync(new TestEvent("all"));

        Assert.Equal(1, count1);
        Assert.Equal(1, count2);
    }

    [Fact]
    public async Task PublishAsync_ReSubscribeAfterAllUnsubscribed_Works()
    {
        var services = new ServiceCollection();
        services.AddInMemoryEventBus();
        var provider = services.BuildServiceProvider();
        var bus = provider.GetRequiredService<IEventBus>();

        var callCount = 0;
        ValueTask Handler(TestEvent evt, CancellationToken token)
        {
            Interlocked.Increment(ref callCount);
            return ValueTask.CompletedTask;
        }

        var sub = await bus.SubscribeAsync<TestEvent>(Handler);
        sub.Dispose();
        await bus.PublishAsync(new TestEvent("ignored")); // should not invoke

        await bus.SubscribeAsync<TestEvent>(Handler);
        await bus.PublishAsync(new TestEvent("delivered"));

        Assert.Equal(1, callCount);
    }

    [Fact]
    public async Task PublishAsync_OnHandlerFailedException_IsSuppressed()
    {
        var services = new ServiceCollection();
        services.AddInMemoryEventBus(options =>
        {
            options.MaxRetryAttempts = 1;
            options.InitialRetryDelay = TimeSpan.Zero;
            options.ShouldRetry = _ => false;
            options.OnHandlerFailed = (_, _, _) => throw new Exception("hook failure");
        });

        var provider = services.BuildServiceProvider();
        var bus = provider.GetRequiredService<IEventBus>();

        await bus.SubscribeAsync<TestEvent>((evt, token) => throw new InvalidOperationException("handler failure"));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => bus.PublishAsync(new TestEvent("fail")).AsTask());
        Assert.Equal("handler failure", ex.Message);
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

    private sealed record DuplicateEvent(Guid EventId)
        : IntegrationEvent(EventId, DateTimeOffset.UtcNow);

    [EventName("SharedPayloadEvent")]
    private sealed record PascalPayloadEvent(string Payload)
        : IntegrationEvent(Guid.NewGuid(), DateTimeOffset.UtcNow);

    [EventName("SharedPayloadEvent")]
    private sealed record AlternatePayloadEvent(string payload)
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
