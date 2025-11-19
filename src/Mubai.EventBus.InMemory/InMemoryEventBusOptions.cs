using System;
using System.Net.Http;
using System.Text.Json;
using Mubai.EventBus.Events;
using Mubai.EventBus.Exceptions;

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
        public Func<Exception, bool> ShouldRetry { get; set; }

        /// <summary>
        /// Optional callback when a handler fails after all retry attempts are exhausted.
        /// Parameters: the event, the last exception, and the attempt count at failure.
        /// </summary>
        public Action<IntegrationEvent, Exception, int> OnHandlerFailed { get; set; }

        /// <summary>
        /// Whether to enable in-memory idempotence tracking for published events.
        /// </summary>
        public bool EnableIdempotence { get; set; } = true;

        /// <summary>
        /// Optional time-to-live for processed event entries. Zero disables TTL eviction.
        /// </summary>
        public TimeSpan ProcessedEventTtl { get; set; } = TimeSpan.FromMinutes(10);

        /// <summary>
        /// Maximum number of processed event ids kept in-memory. Zero disables capacity enforcement.
        /// </summary>
        public int ProcessedEventCapacity { get; set; } = 10000;

        /// <summary>
        /// Maximum number of handlers processed concurrently per publish call. Zero means no limit.
        /// </summary>
        public int MaxParallelHandlers { get; set; }

        /// <summary>
        /// Serializer options used when converting events between compatible types.
        /// </summary>
        public JsonSerializerOptions SerializerOptions { get; set; } = new JsonSerializerOptions();

        internal InMemoryEventBusOptions CloneAndNormalize()
        {
            var copy = new InMemoryEventBusOptions
            {
                MaxRetryAttempts = MaxRetryAttempts < 1 ? 1 : MaxRetryAttempts,
                InitialRetryDelay = InitialRetryDelay < TimeSpan.Zero ? TimeSpan.Zero : InitialRetryDelay,
                BackoffFactor = BackoffFactor < 1d ? 1d : BackoffFactor,
                UseExponentialBackoff = UseExponentialBackoff,
                ShouldRetry = ShouldRetry ?? DefaultShouldRetry,
                OnHandlerFailed = OnHandlerFailed,
                EnableIdempotence = EnableIdempotence,
                ProcessedEventTtl = ProcessedEventTtl < TimeSpan.Zero ? TimeSpan.Zero : ProcessedEventTtl,
                ProcessedEventCapacity = ProcessedEventCapacity < 0 ? 0 : ProcessedEventCapacity,
                MaxParallelHandlers = MaxParallelHandlers < 0 ? 0 : MaxParallelHandlers,
                SerializerOptions = SerializerOptions is null
                    ? new JsonSerializerOptions()
                    : new JsonSerializerOptions(SerializerOptions)
            };

            return copy;
        }

        /// <summary>
        /// Default transient-fault retry policy: DbUpdateConcurrencyException, TimeoutException,
        /// HttpRequestException, SocketException, or other common transient errors.
        /// </summary>
        internal static bool DefaultShouldRetry(Exception exception)
        {
            if (exception is null || exception is NonRetryableException)
            {
                return false;
            }

            if (exception is TimeoutException || exception is HttpRequestException)
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
