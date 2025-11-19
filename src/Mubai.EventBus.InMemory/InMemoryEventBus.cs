using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Mubai.EventBus.Abstractions;
using Mubai.EventBus.Events;
using Mubai.EventBus.Exceptions;

namespace Mubai.EventBus.InMemory
{
    /// <summary>
    /// Simple in-memory implementation of <see cref="IEventBus"/>.
    /// Intended for local/testing scenarios; no durability across process restarts.
    /// </summary>
    public sealed class InMemoryEventBus : IEventBus, IDisposable
    {
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly ILogger<InMemoryEventBus> _logger;
        private readonly InMemoryEventBusOptions _options;
        private readonly JsonSerializerOptions _serializerOptions;

        // Event name -> handlers keyed by handler identity (type or delegate instance).
        private readonly ConcurrentDictionary<string, ConcurrentDictionary<object, HandlerRegistration>> _handlers = new();

        // Tracks handled IntegrationEvent.Id to provide in-memory idempotence.
        private readonly ConcurrentDictionary<Guid, DateTimeOffset> _processedEvents = new();
        private readonly ConcurrentQueue<(Guid EventId, DateTimeOffset Timestamp)> _processedEventOrder = new();

        private bool _disposed;

        private sealed class HandlerRegistration
        {
            public HandlerRegistration(Type eventType, Func<IServiceProvider, object, CancellationToken, ValueTask> callback)
            {
                EventType = eventType ?? throw new ArgumentNullException(nameof(eventType));
                Callback = callback ?? throw new ArgumentNullException(nameof(callback));
            }

            public Type EventType { get; }
            public Func<IServiceProvider, object, CancellationToken, ValueTask> Callback { get; }
        }

        public InMemoryEventBus(
            IServiceScopeFactory scopeFactory,
            ILogger<InMemoryEventBus> logger,
            InMemoryEventBusOptions options = null)
        {
            _scopeFactory = scopeFactory ?? throw new ArgumentNullException(nameof(scopeFactory));
            _logger = logger ?? NullLogger<InMemoryEventBus>.Instance;
            _options = (options ?? InMemoryEventBusOptions.Default).CloneAndNormalize();
            _serializerOptions = _options.SerializerOptions;
        }

        public async ValueTask PublishAsync<TEvent>(TEvent @event, CancellationToken cancellationToken = default)
            where TEvent : IntegrationEvent
        {
            ThrowIfDisposed();
            if (@event is null)
            {
                throw new ArgumentNullException(nameof(@event));
            }

            var eventName = ResolveEventName(typeof(TEvent));

            // Idempotence: ensure we only process each event id once per process lifetime.
            if (!TryMarkEventProcessed(@event.Id))
            {
                _logger.LogDebug("Event {EventId} of type {EventType} already processed, skipping.", @event.Id, typeof(TEvent).Name);
                return;
            }

            if (!_handlers.TryGetValue(eventName, out var handlers) || handlers.IsEmpty)
            {
                _logger.LogDebug("No handlers registered for event {EventType}", typeof(TEvent).Name);
                RemoveProcessedEvent(@event.Id, "no handlers registered", null, LogLevel.Debug);
                return;
            }

            var registrations = handlers.Values.ToArray();
            SemaphoreSlim concurrencyLimiter = null;
            if (_options.MaxParallelHandlers > 0)
            {
                concurrencyLimiter = new SemaphoreSlim(_options.MaxParallelHandlers);
            }

            try
            {
                var tasks = new Task[registrations.Length];
                for (var i = 0; i < registrations.Length; i++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    tasks[i] = ExecuteHandlerAsync(registrations[i], @event, concurrencyLimiter, cancellationToken);
                }

                await Task.WhenAll(tasks).ConfigureAwait(false);
            }
            catch (Exception publishException)
            {
                RemoveProcessedEvent(@event.Id, "handler failure", publishException, LogLevel.Warning);
                throw;
            }
            finally
            {
                concurrencyLimiter?.Dispose();
            }
        }

        public ValueTask<IDisposable> SubscribeAsync<TEvent, THandler>(CancellationToken cancellationToken = default)
            where TEvent : IntegrationEvent
            where THandler : IIntegrationEventHandler<TEvent>
        {
            ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            var eventName = ResolveEventName(typeof(TEvent));
            var handlers = _handlers.GetOrAdd(
                eventName,
                _ => new ConcurrentDictionary<object, HandlerRegistration>());

            var handlerKey = typeof(THandler);
            handlers[handlerKey] = new HandlerRegistration(typeof(TEvent), async (provider, evt, ct) =>
            {
                var handler = provider.GetRequiredService<THandler>();
                await handler.HandleAsync((TEvent)evt, ct).ConfigureAwait(false);
            });

            _logger.LogInformation("Subscribed handler {Handler} to event {EventType} ({EventName})", typeof(THandler).FullName, typeof(TEvent).Name, eventName);

            return new ValueTask<IDisposable>(new Subscription(() =>
            {
                Remove(eventName, handlerKey);
                _logger.LogInformation("Unsubscribed handler {Handler} from event {EventType} ({EventName})", typeof(THandler).FullName, typeof(TEvent).Name, eventName);
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
            var handlers = _handlers.GetOrAdd(
                eventName,
                _ => new ConcurrentDictionary<object, HandlerRegistration>());

            handlers[handler] = new HandlerRegistration(typeof(TEvent), (_, evt, ct) => handler((TEvent)evt, ct));

            _logger.LogInformation("Subscribed inline handler to event {EventType} ({EventName})", typeof(TEvent).Name, eventName);

            return new ValueTask<IDisposable>(new Subscription(() =>
            {
                Remove(eventName, handler);
                _logger.LogInformation("Unsubscribed inline handler from event {EventType} ({EventName})", typeof(TEvent).Name, eventName);
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
            _logger.LogInformation("Unsubscribed handler {Handler} from event {EventType} ({EventName})", typeof(THandler).FullName, typeof(TEvent).Name, eventName);

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
            _logger.LogInformation("Unsubscribed inline handler from event {EventType} ({EventName})", typeof(TEvent).Name, eventName);

            return default;
        }

        private void Remove(string eventName, object handlerKey)
        {
            if (_handlers.TryGetValue(eventName, out var handlers))
            {
                handlers.TryRemove(handlerKey, out _);

                if (handlers.IsEmpty)
                {
                    _handlers.TryRemove(eventName, out _);
                }
            }
        }

        private async Task ExecuteHandlerAsync(
            HandlerRegistration registration,
            IntegrationEvent @event,
            SemaphoreSlim concurrencyLimiter,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (concurrencyLimiter is not null)
            {
                await concurrencyLimiter.WaitAsync(cancellationToken).ConfigureAwait(false);
            }

            try
            {
                using var scope = _scopeFactory.CreateScope();
                var provider = scope.ServiceProvider;

                var payload = ConvertEvent(@event, registration.EventType);
                await InvokeWithRetryAsync(
                    () => registration.Callback(provider, payload, cancellationToken),
                    @event,
                    cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                concurrencyLimiter?.Release();
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
            _processedEvents.Clear();
            while (_processedEventOrder.TryDequeue(out _))
            {
            }
        }

        private static string ResolveEventName(Type eventType)
        {
            var attribute = eventType.GetCustomAttribute<EventNameAttribute>();
            return string.IsNullOrWhiteSpace(attribute?.Name) ? eventType.Name : attribute.Name;
        }

        private bool TryMarkEventProcessed(Guid eventId)
        {
            if (!_options.EnableIdempotence)
            {
                return true;
            }

            var now = DateTimeOffset.UtcNow;
            CleanupProcessedEvents(now);

            if (_processedEvents.TryAdd(eventId, now))
            {
                _processedEventOrder.Enqueue((eventId, now));
                return true;
            }

            return false;
        }

        private void CleanupProcessedEvents(DateTimeOffset now)
        {
            if (!_options.EnableIdempotence)
            {
                return;
            }

            var ttl = _options.ProcessedEventTtl;
            var capacity = _options.ProcessedEventCapacity;

            if (ttl <= TimeSpan.Zero && capacity <= 0)
            {
                return;
            }

            while (_processedEventOrder.TryPeek(out var entry))
            {
                var expired = ttl > TimeSpan.Zero && now - entry.Timestamp >= ttl;
                var overCapacity = capacity > 0 && _processedEvents.Count > capacity;
                if (!expired && !overCapacity)
                {
                    break;
                }

                if (_processedEventOrder.TryDequeue(out var removed))
                {
                    if (expired)
                    {
                        RemoveProcessedEvent(removed.EventId, "entry expired", null, LogLevel.Debug);
                    }
                    else if (overCapacity)
                    {
                        RemoveProcessedEvent(removed.EventId, "capacity limit", null, LogLevel.Debug);
                    }
                }
            }
        }

        private void RemoveProcessedEvent(Guid eventId, string reason, Exception exception = null, LogLevel level = LogLevel.Information)
        {
            if (!_options.EnableIdempotence)
            {
                return;
            }

            if (_processedEvents.TryRemove(eventId, out _))
            {
                _logger.Log(level, exception, "Removed processed event {EventId} due to {Reason}.", eventId, reason);
            }
        }

        private object ConvertEvent(object sourceEvent, Type targetType)
        {
            if (targetType.IsInstanceOfType(sourceEvent))
            {
                return sourceEvent;
            }

            var json = JsonSerializer.Serialize(sourceEvent, sourceEvent.GetType(), _serializerOptions);
            var deserialized = JsonSerializer.Deserialize(json, targetType, _serializerOptions);
            return deserialized ?? throw new InvalidOperationException($"Failed to deserialize event {sourceEvent.GetType().Name} to {targetType.Name}.");
        }

        private void ThrowIfDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(InMemoryEventBus));
            }
        }

        private async ValueTask InvokeWithRetryAsync(
            Func<ValueTask> callback,
            IntegrationEvent @event,
            CancellationToken cancellationToken)
        {
            var attempt = 0;
            var maxAttempts = _options.MaxRetryAttempts;

            while (true)
            {
                attempt++;
                try
                {
                    await callback().ConfigureAwait(false);
                    return;
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    var isNonRetryable = ex is NonRetryableException;
                    var shouldRetry = !isNonRetryable && attempt < maxAttempts && (_options.ShouldRetry?.Invoke(ex) ?? false);

                    _logger.LogWarning(
                        ex,
                        "Handler attempt {Attempt}/{MaxAttempts} failed for event {EventType} ({EventId}).",
                        attempt,
                        maxAttempts,
                        @event.GetType().Name,
                        @event.Id);

                    if (!shouldRetry)
                    {
                        NotifyFinalFailure(@event, ex, attempt);
                        throw;
                    }

                    var delay = CalculateDelay(attempt);
                    if (delay > TimeSpan.Zero)
                    {
                        await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                    }
                }
            }
        }

        private TimeSpan CalculateDelay(int attempt)
        {
            if (_options.InitialRetryDelay == TimeSpan.Zero)
            {
                return TimeSpan.Zero;
            }

            if (!_options.UseExponentialBackoff || attempt <= 1)
            {
                return _options.InitialRetryDelay;
            }

            var factor = Math.Pow(_options.BackoffFactor, attempt - 1);
            var milliseconds = _options.InitialRetryDelay.TotalMilliseconds * factor;
            return milliseconds switch
            {
                <= 0 => TimeSpan.Zero,
                > int.MaxValue => TimeSpan.FromMilliseconds(int.MaxValue),
                _ => TimeSpan.FromMilliseconds(milliseconds)
            };
        }

        private void NotifyFinalFailure(IntegrationEvent @event, Exception exception, int attempt)
        {
            if (_options.OnHandlerFailed is null)
            {
                return;
            }

            try
            {
                _options.OnHandlerFailed.Invoke(@event, exception, attempt);
            }
            catch (Exception hookEx)
            {
                _logger.LogError(
                    hookEx,
                    "OnHandlerFailed callback threw an exception for event {EventType} ({EventId}).",
                    @event.GetType().Name,
                    @event.Id);
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
