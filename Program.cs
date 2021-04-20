using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Producer;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Azure_EventHubs
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var connectionString = "Endpoint=sb://mptesteventhub.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=ytQawEtQdv90Z3QSP+CSN301iQ2fWiXkb0AI7WWMlyI="; // Connection string for the event hubs namespace
            var eventHubName = "mptesteventhub"; // Name of the event hub
            var consumerGroup = EventHubConsumerClient.DefaultConsumerGroupName;

            var producer = new EventHubProducerClient(connectionString, eventHubName);
            var consumer = new EventHubConsumerClient(consumerGroup, connectionString, eventHubName);

            // Sending events
            //try
            //{
            //    string firstPartition = (await producer.GetPartitionIdsAsync()).First();

            //    var batchOptions = new CreateBatchOptions
            //    {
            //        PartitionId = firstPartition
            //    };

            //    using var eventBatch = await producer.CreateBatchAsync();

            //    var eventBody = new BinaryData("This is an event body");
            //    var eventData = new EventData(eventBody);

            //    if (!eventBatch.TryAdd(eventData))
            //    {
            //        throw new Exception($"The event could not be added.");
            //    }

            //    await producer.SendAsync(eventBatch);
            //}
            //finally
            //{
            //    await producer.CloseAsync();
            //}

            // Receiving events
            try
            {
                //using CancellationTokenSource cancellationSource = new CancellationTokenSource();
                //cancellationSource.CancelAfter(TimeSpan.FromSeconds(45));

                int eventsRead = 0;
                int maximumEvents = 5;

                var options = new ReadEventOptions
                {
                    MaximumWaitTime = TimeSpan.FromSeconds(1)
                };

                await foreach (PartitionEvent partitionEvent in consumer.ReadEventsAsync(options))
                {
                    if (partitionEvent.Data != null)
                    {
                        string readFromPartition = partitionEvent.Partition.PartitionId;
                        string eventBody = partitionEvent.Data.EventBody.ToString();

                        Console.WriteLine($"Read event body: '{ eventBody }' from '{ readFromPartition }' partition.");
                    }
                    else
                    {
                        Console.WriteLine($"Wait time elapsed; no event was available.");
                    }

                    eventsRead++;

                    if (eventsRead >= maximumEvents)
                    {
                        break;
                    }
                }
            }
            catch (TaskCanceledException)
            {
                // This is expected if the cancellation token is
                // signaled.
            }
            finally
            {
                await consumer.CloseAsync();
            }
        }
    }
}
