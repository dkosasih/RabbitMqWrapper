using System;
using System.Threading.Tasks;
using RabbitMqWrapper.Models;

namespace RabbitMqWrapper.Subscribers
{
    public interface IRabbitSubscriber
    {
        Task StartProcess<T>(
            RabbitSubscriberConfiguration option,
            Func<T, Task> queueProcessor,
            Func<DlxMessage<T>, Task> dlxProcessor) where T : class;
    }

}