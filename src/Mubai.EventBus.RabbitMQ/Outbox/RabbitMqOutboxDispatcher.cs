using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace Mubai.EventBus.RabbitMQ.Outbox;

public sealed class RabbitMqOutboxDispatcher : IOutboxDispatcher
{
    private readonly IConnectionFactory _connectionFactory;
    private readonly RabbitMqOptions _options;
    private readonly ILogger<RabbitMqOutboxDispatcher> _logger;

    public RabbitMqOutboxDispatcher(
        IConnectionFactory connectionFactory,
        IOptions<RabbitMqOptions> options,
        ILogger<RabbitMqOutboxDispatcher> logger)
    {
        _connectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? NullLogger<RabbitMqOutboxDispatcher>.Instance;
    }

    public Task DispatchAsync(OutboxMessage message, CancellationToken cancellationToken = default)
    {
        if (message is null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        cancellationToken.ThrowIfCancellationRequested();

        using var connection = _connectionFactory.CreateConnection();
        using var channel = connection.CreateModel();

        var props = channel.CreateBasicProperties();
        props.MessageId = message.Id.ToString();
        props.Type = message.EventName;
        props.Timestamp = new AmqpTimestamp(message.OccurredOn.ToUnixTimeSeconds());
        props.ContentType = message.ContentType;
        props.DeliveryMode = 2;
        if (message.Headers is not null)
        {
            props.Headers = new System.Collections.Generic.Dictionary<string, object?>(message.Headers);
        }

        var body = Encoding.UTF8.GetBytes(message.Payload);
        var routingKey = _options.BuildRoutingKey(message.EventName);

        channel.BasicPublish(_options.TopicExchangeName, routingKey, props, body);
        channel.BasicPublish(_options.DirectExchangeName, routingKey, props, body);

        _logger.LogInformation("Outbox dispatched {EventName} ({MessageId})", message.EventName, message.Id);
        return Task.CompletedTask;
    }
}
