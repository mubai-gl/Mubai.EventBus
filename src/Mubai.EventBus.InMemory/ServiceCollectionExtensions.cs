using System;
using System.Linq;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Mubai.EventBus.Abstractions;

namespace Mubai.EventBus.InMemory
{
    /// <summary>
    /// DI helpers for the in-memory event bus.
    /// </summary>
    public static class ServiceCollectionExtensions
    {
        /// <summary>
        /// Register the in-process in-memory event bus.
        /// </summary>
        public static IServiceCollection AddInMemoryEventBus(this IServiceCollection services)
        {
            services.TryAddSingleton<IEventBus>(provider =>
            {
                var scopeFactory = provider.GetRequiredService<IServiceScopeFactory>();
                var logger = provider.GetService<ILogger<InMemoryEventBus>>() ?? NullLogger<InMemoryEventBus>.Instance;
                return new InMemoryEventBus(scopeFactory, logger);
            });
            return services;
        }

        /// <summary>
        /// Scan and register all IIntegrationEventHandler implementations from assemblies.
        /// </summary>
        public static IServiceCollection AddIntegrationEventHandlersFromAssemblies(
            this IServiceCollection services,
            ServiceLifetime handlerLifetime = ServiceLifetime.Scoped,
            params Assembly[] assemblies)
        {
            if (assemblies is null || assemblies.Length == 0)
            {
                throw new ArgumentException("At least one assembly is required.", nameof(assemblies));
            }

            var handlerInterface = typeof(IIntegrationEventHandler<>);

            foreach (var assembly in assemblies.Where(a => a is not null))
            {
                foreach (var type in assembly.DefinedTypes)
                {
                    if (type.IsAbstract || type.IsInterface)
                    {
                        continue;
                    }

                    foreach (var handlerContract in type.ImplementedInterfaces.Where(
                                 i => i.IsGenericType && i.GetGenericTypeDefinition() == handlerInterface))
                    {
                        services.TryAdd(ServiceDescriptor.Describe(type.AsType(), type.AsType(), handlerLifetime));
                        services.TryAddEnumerable(ServiceDescriptor.Describe(handlerContract, type.AsType(), handlerLifetime));
                    }
                }
            }

            return services;
        }

        /// <summary>
        /// Scan the assembly containing the specified type for handlers.
        /// </summary>
        public static IServiceCollection AddIntegrationEventHandlersFromAssemblyContaining<T>(
            this IServiceCollection services,
            ServiceLifetime handlerLifetime = ServiceLifetime.Scoped)
        {
            return services.AddIntegrationEventHandlersFromAssemblies(handlerLifetime, typeof(T).Assembly);
        }
    }
}
