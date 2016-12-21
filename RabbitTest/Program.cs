using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using P = System.Collections.Generic.Dictionary<string, object>;

namespace RabbitTest
{
    class Program
    {
        private static readonly ConnectionFactory _factory = new ConnectionFactory()
        {
            HostName = "192.168.11.169",
            UserName = "test",
            Password = "test",
            Port = AmqpTcpEndpoint.UseDefaultPort
        };

        private static readonly TaskCompletionSource<object> _globalCmpl = new TaskCompletionSource<object>();
        

        static void Main(string[] args)
        {

            string version = Guid.NewGuid().ToString();

            string exchangeFront = version + ".exchange-front";
            string queueA1 = version + ".queue-A1";
            string queueA2 = version + ".queue-A2";
            string queueA3 = version + ".queue-A3";
            string deadLetterExchange = version + ".dead-letter";
            string deadLetterQueue = version + ".dead-letter-queue";

            var DEAD_LETTER_ENABLED = new P()
            {
                { "x-dead-letter-exchange", deadLetterExchange },
                { "x-message-ttl", 5000 }
            };

            Config((c, model) =>
            {
                model.ExchangeDeclare(deadLetterExchange, "fanout", true, false, null);
                model.QueueDeclare(deadLetterQueue, true, false, false, null);
                model.QueueBind(deadLetterQueue, deadLetterExchange, "", null);

                model.ExchangeDeclare(exchangeFront, "direct", true, false, null);

                model.QueueDeclare(queueA1, true, false, false, DEAD_LETTER_ENABLED);
                model.QueueDeclare(queueA2, true, false, false, DEAD_LETTER_ENABLED);
                model.QueueDeclare(queueA3, true, false, false, DEAD_LETTER_ENABLED);

                model.QueueBind(queueA1, exchangeFront, "route-1", null);
                model.QueueBind(queueA2, exchangeFront, "route-2", null);
                model.QueueBind(queueA3, exchangeFront, "route-3", null);
            });

            Func<object, BasicDeliverEventArgs, bool> handler = (o, m) =>
            {
                Console.WriteLine($"Received normal: {Encoding.ASCII.GetString(m.Body)}");
                return false;
            };

            var l1 = ListenQueue(queueA1, handler, 1);
            var l2 = ListenQueue(queueA2, handler, 1);
            var l3 = ListenQueue(queueA3, handler, 1);

            var dl = ListenQueue(deadLetterQueue, (o, m) =>
            {
                Console.WriteLine("Received dead-letter");
                return false;
            }, 3);

            Send((c, model) =>
            {
                model.BasicPublish(exchangeFront, "route-1", null, Encoding.ASCII.GetBytes("abc1"));
                model.BasicPublish(exchangeFront, "route-2", null, Encoding.ASCII.GetBytes("abc2"));
                model.BasicPublish(exchangeFront, "route-3", null, Encoding.ASCII.GetBytes("abc3"));
            });

            try
            {
                Task.WaitAll(l1, l2, l3, dl);
            }
            catch (AggregateException e) when (e.Flatten().InnerExceptions.All(q => q is TaskCanceledException))
            {

            }

            dl = ListenQueue(deadLetterQueue, (o, m) =>
            {
                Console.WriteLine("Received dead-letter");
                return false;
            }, 3);

            dl.Wait();

            Console.WriteLine("All done");
            Console.ReadKey(true);
        }

        private static void Config(Action<IConnection, IModel> configAction)
        {
            using (var connection = _factory.CreateConnection())
            using (var model = connection.CreateModel())
            {
                configAction(connection, model);
            }
        }

        private static async Task ListenQueue(string queue, Func<object, BasicDeliverEventArgs, bool> callback, int receiveCount)
        {
            Console.WriteLine($"Listening \"{queue}\"");
            await Listen((con, model) =>
            {
                var cmpl = new TaskCompletionSource<object>();
                try
                {
                    var received = 0;
                    var consumer = new EventingBasicConsumer(model);
                    consumer.Received += (sender, message) =>
                    {
                        var shouldAck = callback(sender, message);
                        if (shouldAck)
                        {
                            model.BasicAck(message.DeliveryTag, false);
                        }
                        if (++received == receiveCount)
                        {
                            cmpl.SetResult(null);
                        }
                    };
                    consumer.Shutdown += (sender, message) =>
                    {
                        cmpl.SetCanceled();
                    };
                    model.BasicConsume(queue, false, consumer);
                }
                catch (Exception e)
                {
                    cmpl.SetException(e);
                }
                
                return Task.WhenAny(_globalCmpl.Task, cmpl.Task);
            });
            Console.WriteLine($"Closing listener for \"{queue}\"");
        }

        private static async Task Listen(Func<IConnection, IModel, Task> configAction)
        {
            using (var connection = _factory.CreateConnection())
            using (var model = connection.CreateModel())
            {
                await configAction(connection, model);
            }
        }

        private static void Send(Action<IConnection, IModel> action)
        {
            using (var connection = _factory.CreateConnection())
            using (var model = connection.CreateModel())
            {
                Console.WriteLine("Send started");
                action(connection, model);
                Console.WriteLine("Send completed");
            }
        }
    }
}
