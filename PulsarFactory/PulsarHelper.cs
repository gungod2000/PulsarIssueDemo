using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PulsarFactory
{
    public class PulsarHelper
    {

        private static readonly int pulsarClientCount = 5;
        private static readonly int producerCount = 5;
        private static readonly int consumerCount = 5;

        private static readonly List<IPulsarClient> pulsarClientPool = new List<IPulsarClient>();
        private static readonly List<IProducer<string>> producerPool = new List<IProducer<string>>();

        //issue1 pool
        private static readonly List<IConsumer<string>> consumerPool = new List<IConsumer<string>>();

        //issue2 pool
        private static readonly ConcurrentDictionary<string, IConsumer<string>> consumerPool2 = new ConcurrentDictionary<string, IConsumer<string>>();
        /// <summary>
        /// create pulsar client
        /// </summary>
        /// <returns></returns>
        private static IPulsarClient CreatePulsarClient()
        {
            var client = PulsarClient.Builder().Build();

            return client;
        }

        /// <summary>
        /// init pulsar client pool
        /// </summary>
        public static void InitPulsarClientPool()
        {
            for (int i = 0; i < pulsarClientCount; i++)
            {
                pulsarClientPool.Add(CreatePulsarClient());
            }
        }

        /// <summary>
        /// get random client
        /// </summary>
        /// <returns></returns>
        private static IPulsarClient GetRondomPulsarClient()
        {
            Random random = new Random();
            int index = random.Next(0, pulsarClientCount);
            IPulsarClient client = pulsarClientPool[index];
            return client;
        }

        private static IProducer<string> CreateProducer()
        {
            var client = GetRondomPulsarClient().NewProducer(Schema.String)
            .StateChangedHandler(MonitorProducer)
            .Topic("persistent://public/default/mytopic")
            .Create();

            return client;
        }

        private static IConsumer<string> CreateConsumer()
        {
            var client = GetRondomPulsarClient().NewConsumer(Schema.String)
            .StateChangedHandler(MonitorConsumer)
            .SubscriptionName("mySub1")
            .SubscriptionType(SubscriptionType.Shared)
            .Topic("persistent://public/default/mytopic")
            .Create();

            return client;
        }

        private static IProducer<string> GetRondomProducer()
        {
            Random random = new Random();
            int index = random.Next(0, producerCount);
            IProducer<string> producer = producerPool[index];
            return producer;
        }

        /// <summary>
        /// every thread create one consumer
        /// </summary>
        /// <returns></returns>
        private static IConsumer<string> GetThreadConsumer()
        {
            string threadId = Thread.CurrentThread.ManagedThreadId.ToString();

            IConsumer<string> consumer = null; ;

            if (!consumerPool2.ContainsKey(threadId))
            {
                consumer = GetRondomPulsarClient().NewConsumer(Schema.String)
                .StateChangedHandler(MonitorConsumer)
                .SubscriptionName("mySub1")
                .SubscriptionType(SubscriptionType.Shared)
                .Topic("persistent://public/default/mytopic")
                .Create();

                consumerPool2.TryAdd(threadId, consumer);
            }
            else
            {
                consumer = consumerPool2[threadId];

            }
            return consumer;
        }

        /// <summary>
        /// send message
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        public static MessageId SendMessage(string message)
        {
            CancellationTokenSource source = new CancellationTokenSource(TimeSpan.FromSeconds(5));

            MessageId messageId = GetRondomProducer().Send(message, source.Token).GetAwaiter().GetResult();

            return messageId;

        }

        public static (MessageId, string) ReceiveMessage()
        {
            CancellationTokenSource source = new CancellationTokenSource(TimeSpan.FromSeconds(5));

            var consumer = GetRondomConsumer();

            var message = consumer.Receive(source.Token).Result;
            consumer.Acknowledge(message.MessageId, source.Token).GetAwaiter().GetResult();

            return (message.MessageId, message.Value());

        }

        public static (MessageId, string) ReceiveMessage1()
        {
            CancellationTokenSource source = new CancellationTokenSource(TimeSpan.FromSeconds(5));

            var consumer = GetThreadConsumer();

            var message = consumer.Receive(source.Token).Result;
            consumer.Acknowledge(message.MessageId, source.Token).GetAwaiter().GetResult();

            return (message.MessageId, message.Value());

        }

        public static async ValueTask<(MessageId, string)> ReceiveMessageAsync()
        {
            CancellationTokenSource source = new CancellationTokenSource(TimeSpan.FromSeconds(5));

            var message = await GetRondomConsumer().Receive();

            return (message.MessageId, message.Value());

        }

        private static IConsumer<string> GetRondomConsumer()
        {
            Random random = new Random();
            int index = random.Next(0, consumerCount);
            IConsumer<string> consumer = consumerPool[index];
            return consumer;
        }

        public static void InitConsumerPool()
        {
            for (int i = 0; i < consumerCount; i++)
            {
                consumerPool.Add(CreateConsumer());
            }
        }

        public static void InitProcedurePool()
        {
            for (int i = 0; i < producerCount; i++)
            {
                producerPool.Add(CreateProducer());
            }
        }

        private static void MonitorProducer(ProducerStateChanged stateChanged)
        {
            var topic = stateChanged.Producer.Topic;
            var state = stateChanged.ProducerState;
            Trace.WriteLine($"The producer for topic '{topic}' changed state to '{state}'");
        }


        private static void MonitorConsumer(ConsumerStateChanged stateChanged)
        {
            var topic = stateChanged.Consumer.Topic;
            var state = stateChanged.ConsumerState;
            Trace.WriteLine($"The consumer for topic '{topic}' changed state to '{state}'");
        }
    }
}
