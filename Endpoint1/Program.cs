using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NServiceBus;

class Program
{

    static void Main()
    {
        AsyncMain().GetAwaiter().GetResult();
    }

    static async Task AsyncMain()
    {
        Console.Title = "Samples.Azure.StorageQueues.Endpoint1";
        var endpointConfiguration = new EndpointConfiguration("Samples.Azure.StorageQueues.Endpoint1");

        #region config

        endpointConfiguration.UseTransport<AzureStorageQueueTransport>();

        #endregion

        endpointConfiguration.UsePersistence<InMemoryPersistence>();
        endpointConfiguration.UseSerialization<JsonSerializer>();
        endpointConfiguration.EnableInstallers();
        endpointConfiguration.SendFailedMessagesTo("error");

        var count = 0;
        var endpointInstance = await Endpoint.Start(endpointConfiguration)
            .ConfigureAwait(false);
        try
        {
            Console.WriteLine("Press any key to send a message");
            Console.WriteLine("Press ESC to exit");

            while (true)
            {
                var key = Console.ReadKey();
                Console.WriteLine();

                if (key.Key == ConsoleKey.Escape)
                {
                    return;
                }

                var message = new Message1
                {
                    Property = "Hello from Endpoint1"
                };

                var tasks = new List<Task>();

                var messageId = "DedupeSample#" + ++count;

                for (int i = 0; i < 3; i++)
                {
                    var options = new SendOptions();
                    options.SetDestination("Samples.Azure.StorageQueues.Endpoint2");
                    options.SetMessageId(messageId);
                    tasks.Add(endpointInstance.Send(message, options));
                }

                await Task.WhenAll(tasks).ConfigureAwait(false);

                Console.WriteLine("Message1 sent");
            }
        }
        finally
        {
            await endpointInstance.Stop()
                .ConfigureAwait(false);
        }
    }
}