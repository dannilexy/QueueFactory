using Confluent.Kafka;
using Microsoft.Azure.ServiceBus;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
namespace QueueFactory
{
    // Install NuGet packages:
    // Confluent.Kafka
    // Microsoft.Azure.ServiceBus
    // RabbitMQ.Client


    // Define a common interface for messaging systems
    public interface IMessageQueue
    {
        void ProduceMessage(string message);
        void ConsumeMessages(Action<string> messageHandler);
    }

    // Implementation for Kafka
    public class KafkaMessageQueue : IMessageQueue
    {
        private readonly IProducer<Null, string> producer;
        private readonly IConsumer<Null, string> consumer;

        public KafkaMessageQueue(string bootstrapServers, string groupId, string topic)
        {
            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
            producer = new ProducerBuilder<Null, string>(producerConfig).Build();

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
            consumer = new ConsumerBuilder<Null, string>(consumerConfig).Build();
            consumer.Subscribe(topic);
        }

        public void ProduceMessage(string message)
        {
            producer.Produce("your_kafka_topic", new Message<Null, string> { Value = message });
        }

        public void ConsumeMessages(Action<string> messageHandler)
        {
            while (true)
            {
                var consumeResult = consumer.Consume();
                if (consumeResult != null)
                {
                    var message = consumeResult.Message.Value;
                    messageHandler(message);
                }

                Thread.Sleep(1000); // Adjust as needed
            }
        }
    }

    // Implementation for Azure Service Bus
    public class AzureServiceBusMessageQueue : IMessageQueue
    {
        private readonly IQueueClient client;

        public AzureServiceBusMessageQueue(string connectionString, string queueName)
        {
            client = new QueueClient(connectionString, queueName);
        }

        public void ProduceMessage(string message)
        {
            var serviceBusMessage = new Message(Encoding.UTF8.GetBytes(message));
            client.SendAsync(serviceBusMessage).Wait();
        }

        public void ConsumeMessages(Action<string> messageHandler)
        {
            var messageHandlerOptions = new MessageHandlerOptions(ExceptionReceivedHandler)
            {
                MaxConcurrentCalls = 1,
                AutoComplete = false
            };

            client.RegisterMessageHandler(async (message, token) =>
            {
                var messageBody = Encoding.UTF8.GetString(message.Body);
                messageHandler(messageBody);
                await client.CompleteAsync(message.SystemProperties.LockToken);
            }, messageHandlerOptions);

            Console.ReadLine(); // Keep the application running
        }

        private Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            Console.WriteLine($"Message handler encountered an exception {exceptionReceivedEventArgs.Exception}.");
            return Task.CompletedTask;
        }
    }

    // Implementation for RabbitMQ
    public class RabbitMQMessageQueue : IMessageQueue
    {
        private readonly IModel channel;

        public RabbitMQMessageQueue(string hostName, string queueName)
        {
            var connectionFactory = new ConnectionFactory { HostName = hostName };
            var connection = connectionFactory.CreateConnection();
            channel = connection.CreateModel();
            channel.QueueDeclare(queue: queueName, durable: false, exclusive: false, autoDelete: false, arguments: null);
        }

        public void ProduceMessage(string message)
        {
            var body = Encoding.UTF8.GetBytes(message);
            channel.BasicPublish(exchange: "", routingKey: "your_rabbitmq_queue", basicProperties: null, body: body);
        }

        public void ConsumeMessages(Action<string> messageHandler)
        {
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                messageHandler(message);
            };

            channel.BasicConsume(queue: "your_rabbitmq_queue", autoAck: true, consumer: consumer);
            Console.ReadLine(); // Keep the application running
        }
    }

    // Factory class for creating instances of IMessageQueue
    public static class MessageQueueFactory
    {
        public static IMessageQueue CreateMessageQueue(QueueType queueType, string[] parameters)
        {
            switch (queueType)
            {
                case QueueType.Kafka:
                    return new KafkaMessageQueue(parameters[0], parameters[1], parameters[2]);

                case QueueType.AzureServiceBus:
                    return new AzureServiceBusMessageQueue(parameters[0], parameters[1]);

                case QueueType.RabbitMQ:
                    return new RabbitMQMessageQueue(parameters[0], parameters[1]);

                default:
                    throw new ArgumentException("Invalid queue type");
            }
        }
    }

    // Enum for specifying the type of queue
    public enum QueueType
    {
        Kafka,
        AzureServiceBus,
        RabbitMQ
    }

    class Program
    {
        static void Main()
        {
            // Example of using the factory to create instances of IMessageQueue
            var kafkaQueue = MessageQueueFactory.CreateMessageQueue(QueueType.Kafka, new[] { "your_kafka_broker", "your_group_id", "your_kafka_topic" });
            var azureServiceBusQueue = MessageQueueFactory.CreateMessageQueue(QueueType.AzureServiceBus, new[] { "your_service_bus_connection_string", "your_service_bus_queue" });
            var rabbitMQQueue = MessageQueueFactory.CreateMessageQueue(QueueType.RabbitMQ, new[] { "your_rabbitmq_host", "your_rabbitmq_queue" });

            // Example of producing and consuming messages
            kafkaQueue.ProduceMessage("Hello Kafka!");
            azureServiceBusQueue.ProduceMessage("Hello Azure Service Bus!");
            rabbitMQQueue.ProduceMessage("Hello RabbitMQ!");

            kafkaQueue.ConsumeMessages(message => Console.WriteLine($"Kafka - Consumed message: {message}"));
            azureServiceBusQueue.ConsumeMessages(message => Console.WriteLine($"Azure Service Bus - Consumed message: {message}"));
            rabbitMQQueue.ConsumeMessages(message => Console.WriteLine($"RabbitMQ - Consumed message: {message}"));

            Console.ReadLine(); // Keep the application running
        }
    }
}
