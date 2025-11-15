using System;
using System.Threading;
using System.Threading.Tasks;
using Mubai.EventBus.Events;

namespace Mubai.EventBus.Abstractions
{
    /// <summary>
    /// Minimal event bus supporting publish and subscribe.
    /// </summary>
    public interface IEventBus
    {
        /// <summary>
        /// Publish an event to all registered handlers.
        /// </summary>
        ValueTask PublishAsync<TEvent>(TEvent @event, CancellationToken cancellationToken = default)
            where TEvent : IntegrationEvent;

        /// <summary>
        /// Subscribe a handler type that will be resolved from DI and receive a disposable handle that can be used to unsubscribe.
        /// </summary>
        ValueTask<IDisposable> SubscribeAsync<TEvent, THandler>(CancellationToken cancellationToken = default)
            where TEvent : IntegrationEvent
            where THandler : IIntegrationEventHandler<TEvent>;

        /// <summary>
        /// Subscribe with an inline delegate and receive a disposable handle that can be used to unsubscribe.
        /// </summary>
        ValueTask<IDisposable> SubscribeAsync<TEvent>(
            Func<TEvent, CancellationToken, ValueTask> handler,
            CancellationToken cancellationToken = default)
            where TEvent : IntegrationEvent;

        /// <summary>
        /// Remove a typed handler subscription.
        /// </summary>
        ValueTask UnsubscribeAsync<TEvent, THandler>(CancellationToken cancellationToken = default)
            where TEvent : IntegrationEvent
            where THandler : IIntegrationEventHandler<TEvent>;

        /// <summary>
        /// Remove an inline delegate subscription.
        /// </summary>
        ValueTask UnsubscribeAsync<TEvent>(
            Func<TEvent, CancellationToken, ValueTask> handler,
            CancellationToken cancellationToken = default)
            where TEvent : IntegrationEvent;
    }
}
