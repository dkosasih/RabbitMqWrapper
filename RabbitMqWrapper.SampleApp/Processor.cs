using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace RabbitMqWrapper.SampleApp
{
    public class SampleMessage
    {
        public string Message { get; init; }
    }
    public class Processor : BackgroundService
    {
        private readonly IRabbitSubscriber rabbitWrapper;
        private readonly ILogger<Processor> logger;

        public Processor(IRabbitSubscriber rabbitWrapper, ILogger<Processor> logger)
        {
            this.rabbitWrapper = rabbitWrapper;
            this.logger = logger;
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            rabbitWrapper.StartProcess<SampleMessage>(
            (o, ea) =>
            {
                if (o.Message == "no h")
                {
                    throw new Exception("error");
                }

                logger.LogInformation("[x] Worker Received {0}", $"{o.Message}; publisher:{ ea.BasicProperties.ReplyTo};");

                return Task.CompletedTask;
            },
            (m, ea) =>
            {
               logger.LogInformation("Processing dlx with message: {0}; error: {1}", m.Message.Message, m.Error);

                return Task.CompletedTask;
            });

            return Task.CompletedTask;
        }
    }
}