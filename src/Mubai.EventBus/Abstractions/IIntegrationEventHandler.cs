using System.Threading;
using System.Threading.Tasks;
using Mubai.EventBus.Events;

namespace Mubai.EventBus.Abstractions
{
    /// <summary>
    /// Typed handler for events.
    /// </summary>
    public interface IIntegrationEventHandler<in TEvent>
        where TEvent : IntegrationEvent
    {
        Task HandleAsync(TEvent @event, CancellationToken cancellationToken = default);
    }
}

