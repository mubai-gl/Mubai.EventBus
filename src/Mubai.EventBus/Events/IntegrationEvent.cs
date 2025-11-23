using System;

namespace Mubai.EventBus.Events
{
    /// <summary>
    /// Describes the message data carried by an integration event.
    /// </summary>
    public record IntegrationEvent
    {
        /// <summary>
        /// Unique identifier of the event message.
        /// </summary>
        public Guid Id { get; }

        /// <summary>
        /// Timestamp indicating when the event occurred.
        /// </summary>
        public DateTimeOffset OccurredOn { get; }

        /// <summary>
        /// Create an event with a generated Id and current UTC timestamp.
        /// </summary>
        protected IntegrationEvent() : this(Guid.NewGuid(), DateTimeOffset.UtcNow)
        {
        }

        public IntegrationEvent(Guid id, DateTimeOffset occurredOn)
        {
            Id = id;
            OccurredOn = occurredOn;
        }
    }
}
