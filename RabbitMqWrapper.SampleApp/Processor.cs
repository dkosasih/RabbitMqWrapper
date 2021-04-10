using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMqWrapper.Models;
using RabbitMqWrapper.Subscribers;

namespace RabbitMqWrapper.SampleApp
{
    public class SampleMessage
    {
        public string Message { get; init; }
    }

    public class Processor : BackgroundService
    {
        private readonly IRabbitSubscriber rabbitWrapper;
        private readonly List<RabbitSubscriberConfiguration> options;
        private readonly ILogger<Processor> logger;

        public Processor(IRabbitSubscriber rabbitWrapper, IOptions<List<RabbitSubscriberConfiguration>> options, ILogger<Processor> logger)
        {
            this.rabbitWrapper = rabbitWrapper;
            this.options = options.Value;
            this.logger = logger;
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            rabbitWrapper.StartProcess<SampleMessage>(
            options.Single(x => x.QueueName == "hello.queue"),
            (o, ea) =>
            {
                if (o.Message == "no h")
                {
                    throw new Exception("error");
                }

                logger.LogInformation("[x] Worker Received {0}", $"{o.Message};");

                return Task.CompletedTask;
            },
            (m, ea) =>
            {
                logger.LogInformation("Processing dlx with message: {0}; error: {1}", m.Message.Message, m.Error);

                return Task.CompletedTask;
            });

            rabbitWrapper.StartProcess<SampleMessage>(
            options.Single(x => x.QueueName == "directHello.queue"),
            (o, ea) =>
            {

                if (o.Message == "no h")
                {
                    throw new Exception("error");
                }

                logger.LogInformation("[x] Direct Worker Received {0}", $"{o.Message};");

                return Task.CompletedTask;
            },
            (m, ea) =>
            {
                logger.LogInformation("Processing Direct dlx with message: {0}; error: {1}", m.Message.Message, m.Error);

                return Task.CompletedTask;
            });

            return Task.CompletedTask;
        }
    }
}