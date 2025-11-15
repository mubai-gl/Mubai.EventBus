using System;
using System.Threading;
using System.Threading.Tasks;

namespace Mubai.EventBus.Abstractions
{
    /// <summary>
    /// Minimal event bus supporting publish and subscribe.
    /// </summary>
    public interface IEventBus : IDisposable, IAsyncDisposable
    {
        /// <summary>
        /// Publish an event to all registered handlers.
        /// </summary>
        Task PublishAsync<TEvent>(TEvent @event, CancellationToken cancellationToken = default);

        /// <summary>
        /// Subscribe a handler type that will be resolved from DI.
        /// </summary>
        IDisposable Subscribe<TEvent, THandler>()
            where THandler : class, IIntegrationEventHandler<TEvent>;

        /// <summary>
        /// Subscribe with an inline delegate.
        /// </summary>
        IDisposable Subscribe<TEvent>(Func<TEvent, CancellationToken, Task> handler);
    }
}
