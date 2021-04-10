namespace RabbitMqWrapper.Models
{
    public class RabbitSubscriberConfiguration
    {
        public string ExchangeName { get; init; }
        public string QueueName { get; init; }
        public string ConsumerTag { get; init; }
        public string ExchangeType { get; init; }
        public string RoutingKey { get; init; }
        public uint? MaxRetry { get; init; }
        public uint? BackoffRateInSeconds { get; init; }
    }
}