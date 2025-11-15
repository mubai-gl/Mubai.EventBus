using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Mubai.EventBus.Abstractions;

namespace Mubai.EventBus.InMemory
{
    /// <summary>
    /// Simple in-memory implementation of <see cref="IEventBus"/>.
    /// </summary>
    public sealed class InMemoryEventBus : IEventBus
    {
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly ILogger<InMemoryEventBus> _logger;
        private readonly ConcurrentDictionary<Type, ConcurrentDictionary<Guid, Func<IServiceProvider, object, CancellationToken, Task>>> _handlers = new();
        private bool _disposed;

        public InMemoryEventBus(IServiceScopeFactory scopeFactory, ILogger<InMemoryEventBus> logger)
        {
            _scopeFactory = scopeFactory ?? throw new ArgumentNullException(nameof(scopeFactory));
            _logger = logger ?? NullLogger<InMemoryEventBus>.Instance;
        }

        public async Task PublishAsync<TEvent>(TEvent @event, CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();
            if (@event is null)
            {
                throw new ArgumentNullException(nameof(@event));
            }

            if (!_handlers.TryGetValue(typeof(TEvent), out var handlers) || handlers.IsEmpty)
            {
                _logger.LogDebug("No handlers registered for event {EventType}", typeof(TEvent).Name);
                return;
            }

            var callbacks = handlers.Values.ToArray();

            using var scope = _scopeFactory.CreateScope();
            var provider = scope.ServiceProvider;

            foreach (var callback in callbacks)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await callback(provider, @event!, cancellationToken).ConfigureAwait(false);
            }
        }

        public IDisposable Subscribe<TEvent, THandler>()
            where THandler : class, IIntegrationEventHandler<TEvent>
        {
            ThrowIfDisposed();

            var subscriptionId = Register(typeof(TEvent), async (provider, evt, ct) =>
            {
                var handler = provider.GetRequiredService<THandler>();
                await handler.HandleAsync((TEvent)evt, ct).ConfigureAwait(false);
            });

            _logger.LogInformation(
                "Subscribed handler {Handler} to event {EventType}",
                typeof(THandler).FullName,
                typeof(TEvent).Name);

            return new Subscription(this, typeof(TEvent), subscriptionId);
        }

        public IDisposable Subscribe<TEvent>(Func<TEvent, CancellationToken, Task> handler)
        {
            ThrowIfDisposed();
            if (handler is null)
            {
                throw new ArgumentNullException(nameof(handler));
            }

            var subscriptionId = Register(typeof(TEvent), (_, evt, ct) => handler((TEvent)evt, ct));

            _logger.LogInformation("Subscribed inline handler to event {EventType}", typeof(TEvent).Name);

            return new Subscription(this, typeof(TEvent), subscriptionId);
        }

        private Guid Register(Type eventType, Func<IServiceProvider, object, CancellationToken, Task> callback)
        {
            var handlers = _handlers.GetOrAdd(
                eventType,
                _ => new ConcurrentDictionary<Guid, Func<IServiceProvider, object, CancellationToken, Task>>());

            var id = Guid.NewGuid();
            handlers[id] = callback;
            return id;
        }

        internal void Remove(Type eventType, Guid subscriptionId)
        {
            if (_handlers.TryGetValue(eventType, out var handlers))
            {
                handlers.TryRemove(subscriptionId, out _);

                if (handlers.IsEmpty)
                {
                    _handlers.TryRemove(eventType, out _);
                }
            }
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            _handlers.Clear();
        }

        public ValueTask DisposeAsync()
        {
            Dispose();
            return new ValueTask(Task.CompletedTask);
        }

        private void ThrowIfDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(InMemoryEventBus));
            }
        }

        private sealed class Subscription : IDisposable, IAsyncDisposable
        {
            private readonly InMemoryEventBus _bus;
            private readonly Type _eventType;
            private readonly Guid _subscriptionId;
            private int _disposed;

            public Subscription(InMemoryEventBus bus, Type eventType, Guid subscriptionId)
            {
                _bus = bus ?? throw new ArgumentNullException(nameof(bus));
                _eventType = eventType ?? throw new ArgumentNullException(nameof(eventType));
                _subscriptionId = subscriptionId;
            }

            public void Dispose()
            {
                if (Interlocked.Exchange(ref _disposed, 1) == 1)
                {
                    return;
                }

                _bus.Remove(_eventType, _subscriptionId);
            }

            public ValueTask DisposeAsync()
            {
                Dispose();
                return new ValueTask(Task.CompletedTask);
            }
        }
    }
}
