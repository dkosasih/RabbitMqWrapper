using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitMqWrapper.SampleApp
{
    public class RabbitMqConnection
    {
        public string Hostname { get; init; }
        public string Username { get; init; }
        public string Password { get; init; }
    }

    public class RabbitWrapperConfiguration
    {
        public string exchangeName { get; init; }
        public string queueName { get; init; }
        public string consumerTag { get; init; }
        public int? maxRetry { get; init; }
        public int? backoffRateInSeconds { get; init; }
    }

    public class DlxMessage<T> where T : class
    {
        public string Error { get; set; }
        public T Message { get; init; }
    }

    public interface IRabbitSubscriber
    {
        Task StartProcess<T>(
            Func<T, BasicDeliverEventArgs, Task> queueProcessor,
            Func<DlxMessage<T>, BasicDeliverEventArgs, Task> dlxProcessor) where T : class;
    }
    public class RabbitSubscriber : IRabbitSubscriber, IDisposable
    {
        private readonly IConnectionFactory rabbitConnFactory;
        private readonly ILogger<RabbitSubscriber> logger;
        private readonly string exchangeName;
        private readonly string dlxName;
        private readonly string retryExchangeName;
        private readonly string queueName;
        private readonly string dlqName;
        private readonly string retryQueueName;
        private readonly int? maxRetry;
        private readonly int? backoffRateInSeconds;
        private readonly string consumerTag;
        private bool disposed = false;
        private IModel channel;
        private IConnection connection;

        public RabbitSubscriber(IConnectionFactory rabbitConnFactory, IOptions<RabbitWrapperConfiguration> option, ILogger<RabbitSubscriber> logger)
        {
            this.rabbitConnFactory = rabbitConnFactory;
            this.logger = logger;
            this.queueName = option.Value.queueName;
            this.exchangeName = option.Value.exchangeName;
            this.consumerTag = option.Value.consumerTag;
            this.maxRetry = option.Value.maxRetry;
            this.backoffRateInSeconds = option.Value.backoffRateInSeconds;
            dlxName = $"{exchangeName}.dlx";
            dlqName = $"{exchangeName}.dlq";
            retryExchangeName = $"{exchangeName}.retry";
            retryQueueName = $"{exchangeName}.retry.queue";

            if (this.maxRetry.HasValue && !backoffRateInSeconds.HasValue)
            {
                throw new ArgumentException("Retry count should be accompanied by back off rate");
            }
        }

        public Task StartProcess<T>(
            Func<T, BasicDeliverEventArgs, Task> queueProcessor,
            Func<DlxMessage<T>, BasicDeliverEventArgs, Task> dlxProcessor) where T : class
        {
            connection = rabbitConnFactory.CreateConnection();
            channel = connection.CreateModel();

            CreateDlx(dlxProcessor);

            channel.ExchangeDeclare(exchange: exchangeName, type: ExchangeType.Fanout);

            var qName = channel.QueueDeclare(queueName, true, false, false, null).QueueName;

            channel.QueueBind(qName, exchangeName, "", null);

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.Received += async (model, ea) =>
            {
                T deserializedObject = default;
                try
                {
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);

                    if (message is null)
                    {
                        logger.LogInformation("Null message payload, not processing message");
                        channel.BasicAck(ea.DeliveryTag, false);
                        return;
                    }

                    deserializedObject = JsonSerializer.Deserialize<T>(message, new JsonSerializerOptions
                    {
                        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                    });

                    if (deserializedObject is null)
                    {
                        logger.LogInformation("Deserialised message resulted in null object, not processing message");
                        return;
                    }

                    await queueProcessor(deserializedObject, ea);
                    channel.BasicAck(ea.DeliveryTag, false);
                }
                catch (Exception e)
                {
                    // Get routing keys from (first (latest)) headers to determine the current retry backoff
                    var routingKeysInMillis = ea.BasicProperties.Headers != null ? ((ea.BasicProperties.Headers["x-death"] as List<object>).FirstOrDefault() as Dictionary<string, object>)?["routing-keys"] : null;

                    try
                    {
                        if (!maxRetry.HasValue || maxRetry <= 0)
                        {
                            // No retry, just dead letter directly
                            channel.BasicPublish(dlxName, "", basicProperties: ea.BasicProperties, body: ea.Body.ToArray());
                            channel.BasicAck(ea.DeliveryTag, false);
                            return;
                        }


                        // try convert routingkeys backoff and convert to retry count below
                        if (routingKeysInMillis != null && Int32.TryParse(Encoding.UTF8.GetString((routingKeysInMillis as List<object>)[0] as byte[]), out int retryCount))
                        {
                            retryCount = (retryCount / (backoffRateInSeconds.Value * 1000)) + 1; // when it's in here, means it has gone through first round
                            if (retryCount <= maxRetry)
                            {
                                SendToRetryExchange(retryCount, ea);
                                return;
                            }

                            var dlxMessage = new DlxMessage<T>
                            {
                                Error = e.Message,
                                Message = deserializedObject
                            };
                            var payloadString = JsonSerializer.Serialize(dlxMessage, new JsonSerializerOptions
                            {
                                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                            });
                            channel.BasicPublish(dlxName, "", basicProperties: ea.BasicProperties, body: Encoding.UTF8.GetBytes(payloadString));
                            channel.BasicAck(ea.DeliveryTag, false);

                            return;
                        }

                        SendToRetryExchange(1, ea);
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "Error: {0}", ex.Message);
                    }
                }
            };

            channel.BasicConsume(
                queue: qName,
                autoAck: false,
                consumerTag: $"{consumerTag ?? Assembly.GetExecutingAssembly().ToString()} - queue",
                consumer: consumer);

            return Task.CompletedTask;
        }

        private void SendToRetryExchange(int currentRetryCount, BasicDeliverEventArgs ea)
        {
            var delayInMilliseconds = (currentRetryCount * backoffRateInSeconds.Value * 1000);
            CreateRetryQueue(delayInMilliseconds);
            channel.BasicPublish(retryExchangeName, delayInMilliseconds.ToString(), basicProperties: ea.BasicProperties, body: ea.Body.ToArray());
            channel.BasicAck(ea.DeliveryTag, false);
        }

        private void CreateRetryQueue(int delayInMilliseconds)
        {
            channel.ExchangeDeclare(exchange: retryExchangeName, type: ExchangeType.Direct);
            var args = new Dictionary<string, object>()
                                {
                                    {"x-dead-letter-exchange", exchangeName},
                                    {"x-message-ttl", delayInMilliseconds}
                                };
            var retryQName = channel.QueueDeclare($"{ retryQueueName}.{ delayInMilliseconds}", true, false, true, args).QueueName;
            channel.QueueBind(retryQName, retryExchangeName, delayInMilliseconds.ToString(), null);
        }

        private void CreateDlx<T>(Func<DlxMessage<T>, BasicDeliverEventArgs, Task> dlxProcessor) where T : class
        {
            channel.ExchangeDeclare(exchange: dlxName, type: ExchangeType.Fanout);

            var dlxqName = channel.QueueDeclare(dlqName, true, false, false, null).QueueName;
            channel.QueueBind(dlxqName, dlxName, "", null);
            if (dlxProcessor is not null)
            {
                var consumer = new AsyncEventingBasicConsumer(channel);

                consumer.Received += async (model, ea) =>
                {
                    try
                    {
                        var body = ea.Body.ToArray();
                        var message = Encoding.UTF8.GetString(body);

                        var deserializedObject = JsonSerializer.Deserialize<DlxMessage<T>>(message, new JsonSerializerOptions
                        {
                            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                        });

                        await dlxProcessor(deserializedObject, ea);
                        channel.BasicAck(ea.DeliveryTag, false);
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "Error happening in dlx queue: {0}", ex.Message);
                        channel.BasicNack(ea.DeliveryTag, false, false);
                    }
                };
                channel.BasicConsume(
                    queue: dlxqName,
                    autoAck: false,
                    consumerTag: $"{consumerTag ?? Assembly.GetExecutingAssembly().ToString()} - dlx",
                    consumer: consumer);
            }
        }

        protected void Dispose(bool disposing)
        {
            if (!disposed)
            {
                if (disposing)
                {
                    channel?.Dispose();
                    connection?.Dispose();
                }

                disposed = true;
            }
        }

        public void Dispose()
        {
            logger.LogInformation("Disposing Rabbit Connections and Model...");
            Dispose(true);
        }
    }
}