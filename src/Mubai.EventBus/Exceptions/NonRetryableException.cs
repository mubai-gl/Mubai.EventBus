using System;

namespace Mubai.EventBus.Exceptions
{
    /// <summary>
    /// Marker exception for business errors that should not be retried by the event bus.
    /// </summary>
    public class NonRetryableException : Exception
    {
        public NonRetryableException(string message) : base(message)
        {
        }

        public NonRetryableException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
