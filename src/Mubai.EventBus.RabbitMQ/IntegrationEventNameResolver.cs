using Mubai.EventBus.Events;
using System.Reflection;

namespace Mubai.EventBus.RabbitMQ;

internal static class IntegrationEventNameResolver
{
    public static string Resolve(Type eventType)
    {
        ArgumentNullException.ThrowIfNull(eventType);

        var attribute = eventType.GetCustomAttribute<EventNameAttribute>();
        return string.IsNullOrWhiteSpace(attribute?.Name) ? eventType.Name : attribute.Name;
    }
}
