using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMqWrapper.Models;

namespace RabbitMqWrapper.Subscribers
{
    public class RabbitSubscriber : IRabbitSubscriber, IDisposable
    {
        private readonly IConnectionFactory rabbitConnFactory;
        private readonly ILogger<RabbitSubscriber> logger;
        private readonly string retryExchangeSuffix = ".retryx";
        private readonly string retryQueueSuffix = ".retryq";
        private readonly string dlxSuffix = ".dlx";
        private readonly string dlqSuffix = ".dlq";

        private bool disposed = false;
        private IConnection connection;
        private List<IModel> channelsToDispose;

        public RabbitSubscriber(IConnectionFactory rabbitConnFactory, ILogger<RabbitSubscriber> logger)
        {
            this.rabbitConnFactory = rabbitConnFactory;
            this.logger = logger;
            channelsToDispose = new List<IModel>();
        }

        public Task StartProcess<T>(
            RabbitSubscriberConfiguration option,
            Func<T, Task> queueProcessor,
            Func<DlxMessage<T>, Task> dlxProcessor) where T : class
        {
            var (queueName, exchangeName, consumerTag, maxRetry, backoffRateInSeconds, exchangeType, routingKey) =
                (option.QueueName, option.ExchangeName, option.ConsumerTag, option.MaxRetry, option.BackoffRateInSeconds, option.ExchangeType, option.RoutingKey);

            if (maxRetry.HasValue && !backoffRateInSeconds.HasValue)
            {
                throw new ArgumentException("Retry count should be accompanied by back off rate");
            }

            if (exchangeType.ToLower() != "fanout" && string.IsNullOrWhiteSpace(routingKey))
            {
                throw new ArgumentException("non-fanout exchange type must have routing key");
            }

            connection = rabbitConnFactory.CreateConnection();
            var channel = connection.CreateModel();
            channelsToDispose.Add(channel);

            CreateDlx(dlxProcessor, channel, exchangeName, queueName, consumerTag);

            channel.ExchangeDeclare(exchange: exchangeName, type: exchangeType.ToLower());

            var qName = channel.QueueDeclare(queueName, true, false, false, null).QueueName;

            channel.QueueBind(qName, exchangeName, routingKey ?? "", null);

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

                    await queueProcessor(deserializedObject);
                    channel.BasicAck(ea.DeliveryTag, false);
                }
                catch (Exception e)
                {
                    // Get routing keys from (first (latest)) headers to determine the current retry backoff
                    var routingKeysInMillis = ea.BasicProperties?.Headers != null ? ((ea.BasicProperties.Headers["x-death"] as List<object>).FirstOrDefault() as Dictionary<string, object>)?["routing-keys"] : null;

                    try
                    {
                        if (!maxRetry.HasValue || maxRetry <= 0)
                        {
                            // No retry, just dead letter directly
                            SendToDlx(channel, exchangeName, deserializedObject, e, ea);
                            return;
                        }

                        // try convert routingkeys backoff and convert to retry count below
                        if (routingKeysInMillis != null && UInt32.TryParse(Encoding.UTF8.GetString((routingKeysInMillis as List<object>)[0] as byte[]), out uint retryCount))
                        {
                            retryCount = (retryCount / (backoffRateInSeconds.Value * 1000)) + 1; // when it's in here, means it has gone through first round
                            if (retryCount <= maxRetry)
                            {
                                SendToRetryExchange(channel, retryCount, backoffRateInSeconds.Value, exchangeName, queueName, routingKey, ea);
                                return;
                            }

                            SendToDlx(channel, exchangeName, deserializedObject, e, ea);

                            return;
                        }

                        SendToRetryExchange(channel, 1, backoffRateInSeconds.Value, exchangeName, queueName, routingKey, ea);
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

        private void SendToDlx<T>(IModel channel, string exchangeName, T payload, Exception exception, BasicDeliverEventArgs ea) where T : class
        {
            var dlxName = $"{exchangeName}{dlxSuffix}";

            var dlxMessage = new DlxMessage<T>
            {
                Error = exception.Message,
                Message = payload
            };
            var payloadString = JsonSerializer.Serialize(dlxMessage, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            });

            channel.BasicPublish(dlxName, "", basicProperties: ea.BasicProperties, body: Encoding.UTF8.GetBytes(payloadString));
            channel.BasicAck(ea.DeliveryTag, false);
        }

        private void SendToRetryExchange(IModel channel, uint currentRetryCount, uint backoffRateInSeconds, string exchangeName, string queueName, string originalQueueRoutingKey, BasicDeliverEventArgs ea)
        {
            var retryExchangeName = $"{exchangeName}{retryExchangeSuffix}";
            var delayInMilliseconds = (currentRetryCount * backoffRateInSeconds * 1000);
            CreateRetryQueue(channel, delayInMilliseconds, exchangeName, queueName, originalQueueRoutingKey);
            channel.BasicPublish(retryExchangeName, delayInMilliseconds.ToString(), basicProperties: ea.BasicProperties, body: ea.Body.ToArray());
            channel.BasicAck(ea.DeliveryTag, false);
        }

        private void CreateRetryQueue(IModel channel, uint delayInMilliseconds, string exchangeName, string queueName, string originalQueueRoutingKey)
        {
            var retryExchangeName = $"{exchangeName}{retryExchangeSuffix}";
            var retryQueueName = $"{queueName}{retryQueueSuffix}";
            channel.ExchangeDeclare(exchange: retryExchangeName, type: ExchangeType.Direct);
            var args = new Dictionary<string, object>()
                                {
                                    {"x-dead-letter-exchange", exchangeName},
                                    {"x-message-ttl", delayInMilliseconds}
                                };
            if (!string.IsNullOrWhiteSpace(originalQueueRoutingKey))
            {
                args.Add("x-dead-letter-routing-key", originalQueueRoutingKey);
            }

            var retryQName = channel.QueueDeclare($"{retryQueueName}.{delayInMilliseconds}", false, true, true, args).QueueName;
            channel.QueueBind(retryQName, retryExchangeName, delayInMilliseconds.ToString(), null);
        }

        private void CreateDlx<T>(Func<DlxMessage<T>, Task> dlxProcessor, IModel channel, string exchangeName, string queueName, string consumerTag) where T : class
        {
            var dlxName = $"{exchangeName}{dlxSuffix}";
            var dlqName = $"{queueName}{dlqSuffix}";
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

                        await dlxProcessor(deserializedObject);
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
                    consumerTag: $"{consumerTag ?? Assembly.GetExecutingAssembly().ToString()} - dlq",
                    consumer: consumer);
            }
        }

        protected void Dispose(bool disposing)
        {
            if (!disposed)
            {
                if (disposing)
                {
                    channelsToDispose.ForEach(channel => channel?.Dispose());
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