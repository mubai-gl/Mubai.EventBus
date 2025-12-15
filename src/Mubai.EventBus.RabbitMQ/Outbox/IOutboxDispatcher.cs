using System.Threading;
using System.Threading.Tasks;

namespace Mubai.EventBus.RabbitMQ.Outbox;

public interface IOutboxDispatcher
{
    Task DispatchAsync(OutboxMessage message, CancellationToken cancellationToken = default);
}
