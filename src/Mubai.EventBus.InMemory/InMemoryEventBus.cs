using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Reflection;
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
    /// In-process, in-memory event bus with synchronous dispatch.
    /// </summary>
    public sealed class InMemoryEventBus : IEventBus, IDisposable
    {
        private readonly IServiceProvider _serviceProvider;
        private readonly ILogger<InMemoryEventBus> _logger;
        private readonly ConcurrentDictionary<string, ConcurrentDictionary<object, HandlerRegistration>> _handlers = new();

        private bool _disposed;

        private sealed class HandlerRegistration
        {
            public HandlerRegistration(Type eventType, Func<IServiceProvider, IntegrationEvent, CancellationToken, ValueTask> invoker)
            {
                EventType = eventType ?? throw new ArgumentNullException(nameof(eventType));
                Invoker = invoker ?? throw new ArgumentNullException(nameof(invoker));
            }

            public Type EventType { get; }
            public Func<IServiceProvider, IntegrationEvent, CancellationToken, ValueTask> Invoker { get; }
        }

        public InMemoryEventBus(
            IServiceProvider serviceProvider,
            ILogger<InMemoryEventBus> logger)
        {
            _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
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

            cancellationToken.ThrowIfCancellationRequested();
            var eventName = ResolveEventName(@event.GetType());

            _logger.LogDebug("Publishing event {EventType} ({EventName}), Id={EventId}", @event.GetType().Name, eventName, @event.Id);
            await DispatchEventAsync(eventName, @event, cancellationToken).ConfigureAwait(false);
        }

        public ValueTask<IDisposable> SubscribeAsync<TEvent, THandler>(CancellationToken cancellationToken = default)
            where TEvent : IntegrationEvent
            where THandler : IIntegrationEventHandler<TEvent>
        {
            ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            var eventName = ResolveEventName(typeof(TEvent));
            var handlers = _handlers.GetOrAdd(eventName, _ => new ConcurrentDictionary<object, HandlerRegistration>());
            var handlerKey = typeof(THandler);

            handlers[handlerKey] = new HandlerRegistration(typeof(TEvent), async (provider, evt, token) =>
            {
                var handler = provider.GetRequiredService<THandler>();
                await handler.HandleAsync((TEvent)evt, token).ConfigureAwait(false);
            });

            _logger.LogDebug("Subscribed handler {Handler} to event {EventType} ({EventName})", typeof(THandler).FullName, typeof(TEvent).Name, eventName);

            return new ValueTask<IDisposable>(new Subscription(() =>
            {
                Remove(eventName, handlerKey);
                _logger.LogDebug("Unsubscribed handler {Handler} from event {EventType} ({EventName})", typeof(THandler).FullName, typeof(TEvent).Name, eventName);
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

            var eventName = ResolveEventName(typeof(TEvent));
            var handlers = _handlers.GetOrAdd(eventName, _ => new ConcurrentDictionary<object, HandlerRegistration>());
            handlers[handler] = new HandlerRegistration(typeof(TEvent), (_, evt, token) => handler((TEvent)evt, token));

            _logger.LogDebug("Subscribed inline handler to event {EventType} ({EventName})", typeof(TEvent).Name, eventName);

            return new ValueTask<IDisposable>(new Subscription(() =>
            {
                Remove(eventName, handler);
                _logger.LogDebug("Unsubscribed inline handler from event {EventType} ({EventName})", typeof(TEvent).Name, eventName);
            }));
        }

        public ValueTask UnsubscribeAsync<TEvent, THandler>(CancellationToken cancellationToken = default)
            where TEvent : IntegrationEvent
            where THandler : IIntegrationEventHandler<TEvent>
        {
            ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            var eventName = ResolveEventName(typeof(TEvent));
            Remove(eventName, typeof(THandler));
            _logger.LogDebug("Unsubscribed handler {Handler} from event {EventType} ({EventName})", typeof(THandler).FullName, typeof(TEvent).Name, eventName);

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

            var eventName = ResolveEventName(typeof(TEvent));
            Remove(eventName, handler);
            _logger.LogDebug("Unsubscribed inline handler from event {EventType} ({EventName})", typeof(TEvent).Name, eventName);

            return default;
        }

        private void Remove(string eventName, object handlerKey)
        {
            if (_handlers.TryGetValue(eventName, out var registrations))
            {
                registrations.TryRemove(handlerKey, out _);
                if (registrations.IsEmpty)
                {
                    _handlers.TryRemove(eventName, out _);
                }
            }
        }

        private async Task DispatchEventAsync(string eventName, IntegrationEvent @event, CancellationToken cancellationToken)
        {
            if (!_handlers.TryGetValue(eventName, out var registrations) || registrations.IsEmpty)
            {
                _logger.LogDebug("No subscribers for event {EventType} ({EventName}); skipping dispatch", @event.GetType().Name, eventName);
                return;
            }

            var snapshot = registrations.Values.ToArray();
            foreach (var registration in snapshot)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await ExecuteHandlerAsync(registration, @event, cancellationToken).ConfigureAwait(false);
            }
        }

        private async Task ExecuteHandlerAsync(HandlerRegistration registration, IntegrationEvent @event, CancellationToken cancellationToken)
        {
            if (!registration.EventType.IsInstanceOfType(@event))
            {
                _logger.LogWarning("Event name matched but handler expects {HandlerType}; event instance is {EventType}. Skipped.", registration.EventType.Name, @event.GetType().Name);
                return;
            }

            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                await registration.Invoker(_serviceProvider, @event, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                _logger.LogWarning("Event handling was canceled: event {EventType}, handler {HandlerType}", @event.GetType().Name, registration.EventType.Name);
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Event {EventType} (Id={EventId}) threw in handler {HandlerType}", @event.GetType().Name, @event.Id, registration.EventType.Name);
                throw;
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

        private static string ResolveEventName(Type eventType)
        {
            var attribute = eventType.GetCustomAttribute<EventNameAttribute>();
            return string.IsNullOrWhiteSpace(attribute?.Name) ? eventType.Name : attribute.Name;
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
