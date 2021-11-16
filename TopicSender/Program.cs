using System;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Newtonsoft.Json.Linq;

namespace TopicSender
{
    class Program
    {
        // connection string to your Service Bus namespace
        static string connectionString = "Endpoint=sb://hornegsdevmsg01.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=bC2o1GygY2qHq9Sk7XYG3UahkfSh4gwdgCTr3JvCkJg=";

        // name of your Service Bus topic
        static string topicName = "messagingservice";

        // the client that owns the connection and can be used to create senders and receivers
        static ServiceBusClient client;

        // the sender used to publish messages to the topic
        static ServiceBusSender sender;

        // number of messages to be sent to the topic
        private const int numOfMessages = 100;
        static async Task Main()
            {
            // The Service Bus client types are safe to cache and use as a singleton for the lifetime
            // of the application, which is best practice when messages are being published or read
            // regularly.
            //
            // Create the clients that we'll use for sending and processing messages.
            try
            {
                client = new ServiceBusClient(connectionString);
                sender = client.CreateSender(topicName);

                // create a batch 
                ServiceBusMessageBatch messageBatch = await sender.CreateMessageBatchAsync();
                int start = 1;
                int end = numOfMessages;
                for (var i = 1; i <= numOfMessages; i++)
                {
                    var message = new ServiceBusMessage("{'account_id':'" + i + ",'message':'static text message'}")
                    {
                        Subject = "Voice"
                    };
                    // if number is odd send message with SMS subject to be consumed by S1
                    if (i % 2 != 0)
                    {
                        message = new ServiceBusMessage("{'account_id':'" + i + ",'message':'static text message'}")
                        {
                            Subject = "SMS"
                        };
                    }
                      

                    // Can add additinal properties
                   // message = new ServiceBusMessage("I'm rich! I have 1000");
                   // message.ApplicationProperties.Add("currency", "CHF");

                    // try adding a message to the batch
                    if (!messageBatch.TryAddMessage(message))
                    {
                        if (messageBatch.Count == 0)
                        {
                            Console.WriteLine($"Failed to fit message number in a batch {i}");
                            break;
                        }
                        end = --i;
                        

                        Console.WriteLine($"Batch sent - {start} to {end} messages");

                        start = end+1;
                        // Decrement counter so that message number i can get another chance in a new batch
                        
                        // Use the producer client to send the batch of messages to the Service Bus topic
                        await sender.SendMessagesAsync(messageBatch);
                        messageBatch.Dispose();
                        Console.WriteLine($"A batch of {numOfMessages} messages has been published to the topic.");

                        //create a new batch

                        messageBatch = await sender.CreateMessageBatchAsync();
                    }
                    else if (i == numOfMessages)
                    {

                        // send the final batch
                        await sender.SendMessagesAsync(messageBatch);
                        Console.WriteLine($"Batch sent - {start} to {i} messages");
                        messageBatch.Dispose();
                    }
                }
            }
            finally
            {
                // Calling DisposeAsync on client types is required to ensure that network
                // resources and other unmanaged objects are properly cleaned up.
                await sender.DisposeAsync();
                await client.DisposeAsync();
            }
            Console.WriteLine("Press any key to end the application");
            Console.ReadKey();

        }
       
    }
    
}
