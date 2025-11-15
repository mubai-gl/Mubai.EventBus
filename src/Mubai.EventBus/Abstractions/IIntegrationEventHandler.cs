using System.Threading;
using System.Threading.Tasks;

namespace Mubai.EventBus.Abstractions
{
    /// <summary>
    /// Typed handler for events.
    /// </summary>
    public interface IIntegrationEventHandler<in TEvent>
    {
        Task HandleAsync(TEvent @event, CancellationToken cancellationToken = default);
    }
}
