# RabbitMq Wrapper Extensions for Dotnet Core DI

The setup requires settings with the following format:

```json
"RabbitMqWrapper": {
    "Connection": {
        "Hostname": "localhost",
        "Username": "guest",
        "Password": "guest"
    },
    "Configurations": [
        {
        "ExchangeName": "hello",
        "QueueName": "hello.queue",
        "ExchangeType": "Fanout",
        "RoutingKey": "abc", // Required for Direct or Topic Exchange Type
        "ConsumerTag": "SampleConsumer", // Optional; default: host application namespace
        "MaxRetry": 2, // Optional
        "BackoffRateInSeconds": 3 // Optional; Required when MaxRetry is specified
        }
    ]
}
```
