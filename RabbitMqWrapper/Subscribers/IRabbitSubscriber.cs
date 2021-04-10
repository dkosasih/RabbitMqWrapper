using System;
using System.Threading.Tasks;
using RabbitMQ.Client.Events;
using RabbitMqWrapper.Models;

namespace RabbitMqWrapper.Subscribers
{ 
    public interface IRabbitSubscriber
    {
        Task StartProcess<T>(
            RabbitSubscriberConfiguration option,
            Func<T, BasicDeliverEventArgs, Task> queueProcessor,
            Func<DlxMessage<T>, BasicDeliverEventArgs, Task> dlxProcessor) where T : class;
    }

}