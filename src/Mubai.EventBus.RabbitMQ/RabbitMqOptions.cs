using System;

namespace Mubai.EventBus.RabbitMQ;

public sealed class RabbitMqOptions
{
    public string ServiceName { get; set; } = string.Empty;
    public string Domain { get; set; } = "default";
    public string TopicExchangeName { get; set; } = "events-topic";
    public string DirectExchangeName { get; set; } = "events-direct";
    public string? QueueName { get; set; }
        = null; // default: {ServiceName}.events
    public bool DeclareTopology { get; set; } = true;
    public ushort PrefetchCount { get; set; } = 10;
    public string DefaultVersion { get; set; } = "v1";
    public string? DefaultSchema { get; set; }
        = null;

    public string GetQueueName() => string.IsNullOrWhiteSpace(QueueName)
        ? $"{ServiceName}.events"
        : QueueName;

    public string BuildRoutingKey(string eventName)
    {
        if (string.IsNullOrWhiteSpace(ServiceName))
        {
            throw new InvalidOperationException("ServiceName must be configured for routing");
        }

        if (string.IsNullOrWhiteSpace(Domain))
        {
            throw new InvalidOperationException("Domain must be configured for routing");
        }

        return $"{ServiceName}.{Domain}.{eventName}";
    }
}
