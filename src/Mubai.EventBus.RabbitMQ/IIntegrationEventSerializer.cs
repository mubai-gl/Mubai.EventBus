using Mubai.EventBus.Events;

namespace Mubai.EventBus.RabbitMQ;

public interface IIntegrationEventSerializer
{
    SerializedMessage Serialize(IntegrationEvent @event, string? version, string? schema);
    IntegrationEvent Deserialize(ReadOnlyMemory<byte> payload, Type eventType);
}

public sealed record SerializedMessage(
    byte[] Payload,
    string ContentType,
    IReadOnlyDictionary<string, object?> Headers);
