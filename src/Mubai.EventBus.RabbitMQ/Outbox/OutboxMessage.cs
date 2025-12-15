using System;
using System.Collections.Generic;

namespace Mubai.EventBus.RabbitMQ.Outbox;

public sealed record OutboxMessage(
    Guid Id,
    string EventName,
    string Payload,
    DateTimeOffset OccurredOn,
    IReadOnlyDictionary<string, object?>? Headers = null,
    string ContentType = "application/json");
