using System;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using static System.Console;

namespace SubscriptionReceiver
{
    class Program
    {
        // connection string to your Service Bus namespace
        static string connectionString = "Endpoint=sb://hornegsdevmsg01.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=bC2o1GygY2qHq9Sk7XYG3UahkfSh4gwdgCTr3JvCkJg=";
        // name of the Service Bus topic
        static string topicName = "messagingservice";

        // name of the subscription to the topic
        static string subscriptionName = "TextMessage";

        // the client that owns the connection and can be used to create senders and receivers
        static ServiceBusClient client;

        // the processor that reads and processes messages from the subscription
        static ServiceBusProcessor processor;

        // handle received messages
        static async Task MessageHandler(ProcessMessageEventArgs args)
        {
            string body = args.Message.Body.ToString();
            Console.WriteLine($"Received: {body} from subscription: {subscriptionName}");
            //throw new InvalidOperationException();

            // complete the message. messages is deleted from the subscription. 
             await args.CompleteMessageAsync(args.Message);
        }

        // handle any errors when receiving messages
        private static async Task ErrorHandler(ProcessErrorEventArgs errorEventArgs)
        {
            Console.WriteLine(errorEventArgs.Exception.ToString());
            
            await Out.WriteLineAsync($"Exception: {errorEventArgs.Exception}");
            await Out.WriteLineAsync($"FullyQualifiedNamespace: {errorEventArgs.FullyQualifiedNamespace}");
            await Out.WriteLineAsync($"ErrorSource: {errorEventArgs.ErrorSource}");
            await Out.WriteLineAsync($"EntityPath: {errorEventArgs.EntityPath}");
            //return Task.CompletedTask;
        }

        static async Task Main()
        {
            // The Service Bus client types are safe to cache and use as a singleton for the lifetime
            // of the application, which is best practice when messages are being published or read
            // regularly.
            //
            // Create the clients that we'll use for sending and processing messages.
            client = new ServiceBusClient(connectionString);
            var options = new ServiceBusProcessorOptions
            {

                AutoCompleteMessages = false,
                MaxConcurrentCalls = 20,
                MaxAutoLockRenewalDuration = TimeSpan.FromMinutes(10),
                ReceiveMode = ServiceBusReceiveMode.PeekLock,
                PrefetchCount = 20
            };

            // create a processor that we can use to process the messages
            processor = client.CreateProcessor(topicName, subscriptionName, options);

            try
            {
                // add handler to process messages
                processor.ProcessMessageAsync += MessageHandler;

                // add handler to process any errors
                processor.ProcessErrorAsync += ErrorHandler;

                // start processing 
                await processor.StartProcessingAsync();

                Console.WriteLine("Wait for a minute and then press any key to end the processing");
                Console.ReadKey();

                // stop processing 
                Console.WriteLine("\nStopping the receiver...");
                await processor.StopProcessingAsync();
                Console.WriteLine("Stopped receiving messages");
            }
            finally
            {
                // Calling DisposeAsync on client types is required to ensure that network
                // resources and other unmanaged objects are properly cleaned up.
                await processor.DisposeAsync();
                await client.DisposeAsync();
            }
        }
    }
}