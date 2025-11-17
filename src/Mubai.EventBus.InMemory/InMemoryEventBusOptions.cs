using System;
using System.Net.Http;
using Mubai.EventBus.Events;

namespace Mubai.EventBus.InMemory
{
    /// <summary>
    /// Configuration for retry behavior of the in-memory event bus.
    /// </summary>
    public sealed class InMemoryEventBusOptions
    {
        private static readonly InMemoryEventBusOptions _default = new InMemoryEventBusOptions();

        /// <summary>
        /// Maximum number of attempts for a handler (including the first call).
        /// </summary>
        public int MaxRetryAttempts { get; set; } = 3;

        /// <summary>
        /// Initial delay before retrying a failed handler.
        /// </summary>
        public TimeSpan InitialRetryDelay { get; set; } = TimeSpan.FromMilliseconds(200);

        /// <summary>
        /// Exponential backoff multiplier applied to the delay for each subsequent attempt.
        /// </summary>
        public double BackoffFactor { get; set; } = 2d;

        /// <summary>
        /// Whether to use exponential backoff. If false, the initial delay is used for every retry.
        /// </summary>
        public bool UseExponentialBackoff { get; set; } = true;

        /// <summary>
        /// Optional predicate to decide if an exception should be retried.
        /// If null, a conservative default that only retries transient faults is used.
        /// </summary>
        public Func<Exception, bool>? ShouldRetry { get; set; }

        /// <summary>
        /// Optional callback when a handler fails after all retry attempts are exhausted.
        /// Parameters: the event, the last exception, and the attempt count at failure.
        /// </summary>
        public Action<IntegrationEvent, Exception, int>? OnHandlerFailed { get; set; }

        internal InMemoryEventBusOptions CloneAndNormalize()
        {
            var copy = new InMemoryEventBusOptions
            {
                MaxRetryAttempts = MaxRetryAttempts < 1 ? 1 : MaxRetryAttempts,
                InitialRetryDelay = InitialRetryDelay < TimeSpan.Zero ? TimeSpan.Zero : InitialRetryDelay,
                BackoffFactor = BackoffFactor < 1d ? 1d : BackoffFactor,
                UseExponentialBackoff = UseExponentialBackoff,
                ShouldRetry = ShouldRetry ?? DefaultShouldRetry,
                OnHandlerFailed = OnHandlerFailed
            };

            return copy;
        }

        /// <summary>
        /// Default transient-fault retry policy: DbUpdateConcurrencyException, TimeoutException,
        /// HttpRequestException, SocketException, or other common transient errors.
        /// </summary>
        internal static bool DefaultShouldRetry(Exception exception)
        {
            if (exception is null)
            {
                return false;
            }

            if (exception is TimeoutException ||
                exception is HttpRequestException)
            {
                return true;
            }

            if (exception is System.Net.Sockets.SocketException)
            {
                return true;
            }

            // Avoid hard dependency on EF Core; detect by full name.
            const string efConcurrencyExceptionName = "Microsoft.EntityFrameworkCore.DbUpdateConcurrencyException";
            var type = exception.GetType();
            if (type.FullName == efConcurrencyExceptionName)
            {
                return true;
            }

            return false;
        }

        internal static InMemoryEventBusOptions Default => _default;
    }
}
