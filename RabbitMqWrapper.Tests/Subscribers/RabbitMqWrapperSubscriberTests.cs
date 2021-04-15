using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NSubstitute;
using RabbitMQ.Client;
using RabbitMQ.Fakes.DotNetStandard;
using RabbitMqWrapper.Models;
using RabbitMqWrapper.Subscribers;
using Xunit;

namespace RabbitMqWrapper.Tests.Subscribers
{
    public class SampleMessage
    {
        public string Message { get; set; }
    }

    public class RabbitMqWrapperSubscriberTests
    {
        private readonly IRabbitSubscriber sut;
        private IModel channel;

        private RabbitServer rabbitServer;
        public RabbitMqWrapperSubscriberTests()
        {
            rabbitServer = new RabbitServer();
            var connectionFactory = new FakeConnectionFactory(rabbitServer);
            var logger = Substitute.For<ILogger<RabbitSubscriber>>();
            var connection = connectionFactory.CreateConnection();
            channel = connection.CreateModel();

            sut = new RabbitSubscriber(connectionFactory, logger);
        }

        [Fact(DisplayName = "Should publish to correct queue")]
        public void Test1()
        {
            var option = new RabbitSubscriberConfiguration
            {
                ExchangeName = "ex",
                QueueName = "ex.q",
                ConsumerTag = "ct",
                ExchangeType = "Fanout"
            };

            var message = "{\"message\": \"no h\"}";
            var body = Encoding.UTF8.GetBytes(message);

            sut.StartProcess<SampleMessage>(option, (payload) =>
            {
                Console.WriteLine("model {0}", payload);

                Assert.Equal("no h", payload.Message);
                return Task.CompletedTask;
            }, null);

            channel.BasicPublish(option.ExchangeName, "", body: body);
        }

        [Fact(DisplayName = "Should run dlx process when message fail without retry")]
        public void Test2()
        {
            var option = new RabbitSubscriberConfiguration
            {
                ExchangeName = "ex",
                QueueName = "ex.q",
                ConsumerTag = "ct",
                ExchangeType = "Fanout"
            };
            var waitHandle = new AutoResetEvent(false);

            var message = "{\"message\": \"no h\"}";
            var body = Encoding.UTF8.GetBytes(message);

            sut.StartProcess<SampleMessage>(option, (payload) =>
            {
                if (payload.Message == "no h")
                {
                    throw new Exception("error");
                }

                return Task.CompletedTask;
            }, payload =>
            {
                Assert.Equal("no h", payload.Message.Message);
                waitHandle.Set();
                return Task.CompletedTask;
            });

            channel.BasicPublish(option.ExchangeName, "", body: body);

            if (!waitHandle.WaitOne(1000, false))
            {
                Assert.False(true, "Timeout");
            }
        }

        // NOTE: Test library does not support TTL; this library count on TTL for its retry mechanism
        // [Fact(DisplayName = "Should retry")]
        // public void Test3()
        // {
        //     var option = new RabbitSubscriberConfiguration
        //     {
        //         ExchangeName = "ex",
        //         QueueName = "ex.q",
        //         ConsumerTag = "ct",
        //         ExchangeType = "Fanout",
        //         BackoffRateInSeconds = 1,
        //         MaxRetry = 2
        //     };
        //     var waitHandle = new AutoResetEvent(false);

        //     var message = "{\"message\": \"no h\"}";
        //     var body = Encoding.UTF8.GetBytes(message);

        //     var count = 0;

        //     sut.StartProcess<SampleMessage>(option, (payload) =>
        //     {
        //         if (payload.Message == "no h" && count <= 0)
        //         {
        //             count++;
        //             throw new Exception("error");
        //         }

        //         if (count > 0)
        //         {
        //             Assert.Equal("no h", payload.Message);
        //             waitHandle.Set();
        //         }

        //         return Task.CompletedTask;
        //     }, null);

        //     channel.BasicPublish(option.ExchangeName, "", body: body);

        //     if (!waitHandle.WaitOne(5000, false))
        //     {
        //         Assert.False(true, "Timeout");
        //     }
        // }
    }
}
