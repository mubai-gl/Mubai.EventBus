using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Mubai.EventBus.Abstractions;
using Mubai.EventBus.Events;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Collections.Concurrent;

namespace Mubai.EventBus.RabbitMQ;

/// <summary>
/// RabbitMQ implementation of IEventBus supporting topic/direct exchanges and routing key {service}.{domain}.{event}.
/// </summary>
public sealed class RabbitMqEventBus : IEventBus, IDisposable
{
    private readonly IServiceProvider _serviceProvider;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<RabbitMqEventBus> _logger;
    private readonly IIntegrationEventSerializer _serializer;
    private readonly RabbitMqOptions _options;
    private readonly IConnection _connection;
    private readonly IModel _channel;
    private readonly AsyncEventingBasicConsumer _consumer;
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

    public RabbitMqEventBus(
        IConnectionFactory connectionFactory,
        IOptions<RabbitMqOptions> options,
        IServiceProvider serviceProvider,
        IIntegrationEventSerializer serializer,
        ILogger<RabbitMqEventBus> logger)
    {
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        _scopeFactory = _serviceProvider.GetRequiredService<IServiceScopeFactory>();
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        _logger = logger ?? NullLogger<RabbitMqEventBus>.Instance;
        _connection = (connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory))).CreateConnection();
        _channel = _connection.CreateModel();

        if (_options.DeclareTopology)
        {
            DeclareTopology();
        }

        _channel.BasicQos(0, _options.PrefetchCount, global: false);

        _consumer = new AsyncEventingBasicConsumer(_channel);
        _consumer.Received += OnMessageReceivedAsync;
        _channel.BasicConsume(queue: _options.GetQueueName(), autoAck: false, consumer: _consumer);
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
        var eventName = IntegrationEventNameResolver.Resolve(@event.GetType());
        var routingKey = _options.BuildRoutingKey(eventName);
        var serialized = _serializer.Serialize(@event, _options.DefaultVersion, _options.DefaultSchema);

        var props = _channel.CreateBasicProperties();
        props.MessageId = @event.Id.ToString();
        props.Type = eventName;
        props.Timestamp = new AmqpTimestamp(@event.OccurredOn.ToUnixTimeSeconds());
        props.ContentType = serialized.ContentType;
        props.DeliveryMode = 2; // persistent
        props.Headers = serialized.Headers?.ToDictionary(kv => kv.Key, kv => kv.Value) ?? new System.Collections.Generic.Dictionary<string, object?>();

        PublishToExchange(_options.TopicExchangeName, routingKey, props, serialized.Payload);
        PublishToExchange(_options.DirectExchangeName, routingKey, props, serialized.Payload);

        _logger.LogDebug("Published event {EventName} with routing {RoutingKey} (Id={EventId})", eventName, routingKey, @event.Id);
        await Task.CompletedTask;
    }

    public ValueTask<IDisposable> SubscribeAsync<TEvent, THandler>(CancellationToken cancellationToken = default)
        where TEvent : IntegrationEvent
        where THandler : IIntegrationEventHandler<TEvent>
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();
        var eventName = IntegrationEventNameResolver.Resolve(typeof(TEvent));
        var handlers = _handlers.GetOrAdd(eventName, _ => new ConcurrentDictionary<object, HandlerRegistration>());
        var handlerKey = typeof(THandler);

        handlers[handlerKey] = new HandlerRegistration(typeof(TEvent), async (provider, evt, token) =>
        {
            var handler = provider.GetRequiredService<THandler>();
            await handler.HandleAsync((TEvent)evt, token).ConfigureAwait(false);
        });

        BindQueue(eventName);
        _logger.LogDebug("Subscribed handler {Handler} to {EventName}", typeof(THandler).FullName, eventName);

        return new ValueTask<IDisposable>(new Subscription(() =>
        {
            Remove(eventName, handlerKey);
            _logger.LogDebug("Unsubscribed handler {Handler} from {EventName}", typeof(THandler).FullName, eventName);
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

        var eventName = IntegrationEventNameResolver.Resolve(typeof(TEvent));
        var handlers = _handlers.GetOrAdd(eventName, _ => new ConcurrentDictionary<object, HandlerRegistration>());
        handlers[handler] = new HandlerRegistration(typeof(TEvent), (_, evt, token) => handler((TEvent)evt, token));

        BindQueue(eventName);
        _logger.LogDebug("Subscribed inline handler to {EventName}", eventName);

        return new ValueTask<IDisposable>(new Subscription(() =>
        {
            Remove(eventName, handler);
            _logger.LogDebug("Unsubscribed inline handler from {EventName}", eventName);
        }));
    }

    public ValueTask UnsubscribeAsync<TEvent, THandler>(CancellationToken cancellationToken = default)
        where TEvent : IntegrationEvent
        where THandler : IIntegrationEventHandler<TEvent>
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();
        var eventName = IntegrationEventNameResolver.Resolve(typeof(TEvent));
        Remove(eventName, typeof(THandler));
        _logger.LogDebug("Unsubscribed handler {Handler} from {EventName}", typeof(THandler).FullName, eventName);
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

        var eventName = IntegrationEventNameResolver.Resolve(typeof(TEvent));
        Remove(eventName, handler);
        _logger.LogDebug("Unsubscribed inline handler from {EventName}", eventName);
        return default;
    }

    private void PublishToExchange(string exchange, string routingKey, IBasicProperties props, ReadOnlyMemory<byte> body)
    {
        if (string.IsNullOrWhiteSpace(exchange))
        {
            return;
        }

        _channel.BasicPublish(
            exchange: exchange,
            routingKey: routingKey,
            mandatory: false,
            basicProperties: props,
            body: body);
    }

    private void BindQueue(string eventName)
    {
        var routingKey = _options.BuildRoutingKey(eventName);
        var queue = _options.GetQueueName();
        _channel.QueueBind(queue, _options.TopicExchangeName, routingKey);
        _channel.QueueBind(queue, _options.DirectExchangeName, routingKey);
    }

    private async Task OnMessageReceivedAsync(object sender, BasicDeliverEventArgs args)
    {
        var eventName = args.BasicProperties?.Type;
        if (string.IsNullOrWhiteSpace(eventName))
        {
            _logger.LogWarning("Received message without event name (Type header). Acking and skipping.");
            _channel.BasicAck(args.DeliveryTag, multiple: false);
            return;
        }

        if (!_handlers.TryGetValue(eventName, out var registrations) || registrations.IsEmpty)
        {
            _logger.LogDebug("No subscribers for {EventName}. Acking.", eventName);
            _channel.BasicAck(args.DeliveryTag, false);
            return;
        }

        var registration = registrations.Values.FirstOrDefault();
        if (registration is null)
        {
            _channel.BasicAck(args.DeliveryTag, false);
            return;
        }

        try
        {
            var evt = _serializer.Deserialize(args.Body, registration.EventType);
            foreach (var handler in registrations.Values.ToArray())
            {
                await ExecuteHandlerAsync(handler, evt, CancellationToken.None).ConfigureAwait(false);
            }

            _channel.BasicAck(args.DeliveryTag, false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed processing event {EventName}; nacking", eventName);
            _channel.BasicNack(args.DeliveryTag, multiple: false, requeue: false);
        }
    }

    private async ValueTask ExecuteHandlerAsync(HandlerRegistration registration, IntegrationEvent evt, CancellationToken cancellationToken)
    {
        if (!registration.EventType.IsInstanceOfType(evt))
        {
            return;
        }

        await using var scope = _scopeFactory.CreateAsyncScope();
        await registration.Invoker.Invoke(scope.ServiceProvider, evt, cancellationToken);
    }

    private void Remove(string eventName, object handlerKey)
    {
        if (_handlers.TryGetValue(eventName, out var regs))
        {
            regs.TryRemove(handlerKey, out _);
            if (regs.IsEmpty)
            {
                _handlers.TryRemove(eventName, out _);
            }
        }
    }

    private void DeclareTopology()
    {
        _channel.ExchangeDeclare(_options.TopicExchangeName, ExchangeType.Topic, durable: true, autoDelete: false);
        _channel.ExchangeDeclare(_options.DirectExchangeName, ExchangeType.Direct, durable: true, autoDelete: false);
        _channel.QueueDeclare(queue: _options.GetQueueName(), durable: true, exclusive: false, autoDelete: false, arguments: null);
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(RabbitMqEventBus));
        }
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _channel?.Close();
        _connection?.Close();
        _channel?.Dispose();
        _connection?.Dispose();
    }

    private sealed class Subscription : IDisposable
    {
        private readonly Action _dispose;
        private int _disposed;

        public Subscription(Action dispose)
        {
            _dispose = dispose ?? throw new ArgumentNullException(nameof(dispose));
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                _dispose();
            }
        }
    }
}
