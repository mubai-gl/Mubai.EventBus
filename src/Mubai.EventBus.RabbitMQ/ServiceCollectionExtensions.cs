using Microsoft.Extensions.DependencyInjection;
using Mubai.EventBus.Abstractions;
using RabbitMQ.Client;

namespace Mubai.EventBus.RabbitMQ;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddRabbitMqEventBus(
        this IServiceCollection services,
        Action<RabbitMqOptions> configureOptions,
        Action<ConnectionFactory>? configureConnection = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        ArgumentNullException.ThrowIfNull(configureOptions);

        services.Configure(configureOptions);

        services.AddSingleton<IConnectionFactory>(sp =>
        {
            var factory = new ConnectionFactory
            {
                DispatchConsumersAsync = true,
                HostName = "localhost"
            };
            configureConnection?.Invoke(factory);
            return factory;
        });

        services.AddSingleton<IIntegrationEventSerializer, JsonIntegrationEventSerializer>();
        services.AddSingleton<IEventBus, RabbitMqEventBus>();
        return services;
    }
}
