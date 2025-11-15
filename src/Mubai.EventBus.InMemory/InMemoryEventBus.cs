using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Mubai.EventBus.Abstractions;
using Mubai.EventBus.Events;

namespace Mubai.EventBus.InMemory
{
    /// <summary>
    /// Simple in-memory implementation of <see cref="IEventBus"/>.
    /// </summary>
    public sealed class InMemoryEventBus : IEventBus, IDisposable
    {
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly ILogger<InMemoryEventBus> _logger;
        private readonly ConcurrentDictionary<Type, ConcurrentDictionary<object, Func<IServiceProvider, object, CancellationToken, ValueTask>>> _handlers = new();
        private bool _disposed;

        public InMemoryEventBus(IServiceScopeFactory scopeFactory, ILogger<InMemoryEventBus> logger)
        {
            _scopeFactory = scopeFactory ?? throw new ArgumentNullException(nameof(scopeFactory));
            _logger = logger ?? NullLogger<InMemoryEventBus>.Instance;
        }

        public async ValueTask PublishAsync<TEvent>(TEvent @event, CancellationToken cancellationToken = default)
            where TEvent : IntegrationEvent
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

        public ValueTask<IDisposable> SubscribeAsync<TEvent, THandler>(CancellationToken cancellationToken = default)
            where TEvent : IntegrationEvent
            where THandler : IIntegrationEventHandler<TEvent>
        {
            ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            var handlers = _handlers.GetOrAdd(
                typeof(TEvent),
                _ => new ConcurrentDictionary<object, Func<IServiceProvider, object, CancellationToken, ValueTask>>());

            var handlerKey = typeof(THandler);
            handlers[handlerKey] = async (provider, evt, ct) =>
            {
                var handler = provider.GetRequiredService<THandler>();
                await handler.HandleAsync((TEvent)evt, ct).ConfigureAwait(false);
            };

            _logger.LogInformation(
                "Subscribed handler {Handler} to event {EventType}",
                typeof(THandler).FullName,
                typeof(TEvent).Name);

            return new ValueTask<IDisposable>(new Subscription(() =>
            {
                Remove(typeof(TEvent), handlerKey);
                _logger.LogInformation(
                    "Unsubscribed handler {Handler} from event {EventType}",
                    typeof(THandler).FullName,
                    typeof(TEvent).Name);
            }));
        }

        public ValueTask<IDisposable> SubscribeAsync<TEvent>(
            Func<TEvent, CancellationToken, ValueTask> handler,
            CancellationToken cancellationToken = default)
            where TEvent : IntegrationEvent
        {
            ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();
            if (handler is null)
            {
                throw new ArgumentNullException(nameof(handler));
            }

            var handlers = _handlers.GetOrAdd(
                typeof(TEvent),
                _ => new ConcurrentDictionary<object, Func<IServiceProvider, object, CancellationToken, ValueTask>>());

            handlers[handler] = (_, evt, ct) => handler((TEvent)evt, ct);

            _logger.LogInformation("Subscribed inline handler to event {EventType}", typeof(TEvent).Name);

            return new ValueTask<IDisposable>(new Subscription(() =>
            {
                Remove(typeof(TEvent), handler);
                _logger.LogInformation("Unsubscribed inline handler from event {EventType}", typeof(TEvent).Name);
            }));
        }

        public ValueTask UnsubscribeAsync<TEvent, THandler>(CancellationToken cancellationToken = default)
            where TEvent : IntegrationEvent
            where THandler : IIntegrationEventHandler<TEvent>
        {
            ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            Remove(typeof(TEvent), typeof(THandler));
            _logger.LogInformation(
                "Unsubscribed handler {Handler} from event {EventType}",
                typeof(THandler).FullName,
                typeof(TEvent).Name);

            return default;
        }

        public ValueTask UnsubscribeAsync<TEvent>(
            Func<TEvent, CancellationToken, ValueTask> handler,
            CancellationToken cancellationToken = default)
            where TEvent : IntegrationEvent
        {
            ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();
            if (handler is null)
            {
                throw new ArgumentNullException(nameof(handler));
            }

            Remove(typeof(TEvent), handler);
            _logger.LogInformation("Unsubscribed inline handler from event {EventType}", typeof(TEvent).Name);

            return default;
        }

        private void Remove(Type eventType, object key)
        {
            if (_handlers.TryGetValue(eventType, out var handlers))
            {
                handlers.TryRemove(key, out _);

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

        private void ThrowIfDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(InMemoryEventBus));
            }
        }

        private sealed class Subscription : IDisposable
        {
            private readonly Action _dispose;
            private int _disposed;

            public Subscription(Action dispose) => _dispose = dispose ?? throw new ArgumentNullException(nameof(dispose));

            public void Dispose()
            {
                if (Interlocked.Exchange(ref _disposed, 1) == 0)
                {
                    _dispose();
                }
            }
        }
    }
}
