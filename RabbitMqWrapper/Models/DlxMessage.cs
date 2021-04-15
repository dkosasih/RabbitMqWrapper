namespace RabbitMqWrapper.Models
{
    public class DlxMessage<T> where T : class
    {
        public string Error { get; set; }
        public T Message { get; set; }
    }

}