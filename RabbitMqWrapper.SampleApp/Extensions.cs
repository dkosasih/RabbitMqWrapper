using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace RabbitMqWrapper.SampleApp
{
    public static class Extensions
    {
        public static void UseRabbitWrapper(this IServiceCollection services, IConfiguration configuration)
        {
            services
                .Configure<RabbitMqConnection>(options => configuration.GetSection("RabbitWrapper:Connection").Bind(options));

            services
                .Configure<RabbitWrapperConfiguration>(options => configuration.GetSection("RabbitWrapper:configuration").Bind(options));

            services.AddSingleton<IConnectionFactory, ConnectionFactory>(c =>
            {
                var connection = c.GetService<IOptions<RabbitMqConnection>>();

                return new ConnectionFactory
                {
                    DispatchConsumersAsync = true,
                    HostName = connection.Value.Hostname,
                    UserName = connection.Value.Username,
                    Password = connection.Value.Password
                };
            });

            services.AddSingleton<IRabbitSubscriber, RabbitSubscriber>();
        }
    }
}