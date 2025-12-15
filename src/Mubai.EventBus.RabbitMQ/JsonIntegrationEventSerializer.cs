using System;
using System.Collections.Generic;
using System.Text.Json;
using Mubai.EventBus.Events;

namespace Mubai.EventBus.RabbitMQ;

public sealed class JsonIntegrationEventSerializer : IIntegrationEventSerializer
{
    private readonly JsonSerializerOptions _options;

    public JsonIntegrationEventSerializer(JsonSerializerOptions? options = null)
    {
        _options = options ?? new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };
    }

    public SerializedMessage Serialize(IntegrationEvent @event, string? version, string? schema)
    {
        ArgumentNullException.ThrowIfNull(@event);

        var payload = JsonSerializer.SerializeToUtf8Bytes(@event, @event.GetType(), _options);
        var headers = new Dictionary<string, object?>();
        if (!string.IsNullOrWhiteSpace(version))
        {
            headers["x-version"] = version;
        }

        if (!string.IsNullOrWhiteSpace(schema))
        {
            headers["x-schema"] = schema;
        }

        return new SerializedMessage(payload, "application/json", headers);
    }

    public IntegrationEvent Deserialize(ReadOnlyMemory<byte> payload, Type eventType)
    {
        ArgumentNullException.ThrowIfNull(eventType);

        return (IntegrationEvent)(JsonSerializer.Deserialize(payload.Span, eventType, _options)
            ?? throw new InvalidOperationException($"Failed to deserialize event type {eventType.Name}"));
    }
}
