using System;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;

namespace EventHubViewer
{
    class EventHubDetails
    {
        // Display details from the Event Hub and Partitions and then exit
        public static async Task getDetails(CommandLineOptions clOptions)
        {
            using CancellationTokenSource cancellationSource = new CancellationTokenSource();
            var consumer = new EventHubConsumerClient(clOptions.consumerGroup, clOptions.connectionString, clOptions.eventHubName);
            EventHubProperties eventHubProperties = await consumer.GetEventHubPropertiesAsync(cancellationToken: cancellationSource.Token);
            
            Console.WriteLine(
                $"Event Hub Namespace: { consumer.FullyQualifiedNamespace }\n" + 
                $"Event Hub Name: { eventHubProperties.Name }\n" + 
                $"Created On: { eventHubProperties.CreatedOn }\n" + 
                $"Total Partitions: { eventHubProperties.PartitionIds.Length }\n"
            );

            foreach (var partitionId in eventHubProperties.PartitionIds)
            {
                PartitionProperties partitionProperties = await consumer.GetPartitionPropertiesAsync(partitionId, cancellationToken: cancellationSource.Token);
                Console.WriteLine(
                    $"Partition: { partitionId }\n" +
                    $"\tHas Messages: { (partitionProperties.IsEmpty ? "No" : "Yes") }\n" +
                    $"\tFirst Sequence Number: { partitionProperties.BeginningSequenceNumber }\n" +
                    $"\tLast Sequence Number: { partitionProperties.LastEnqueuedSequenceNumber }\n" +
                    $"\tLast Offset Number: { partitionProperties.LastEnqueuedOffset }\n" +
                    $"\tLast Enqueued Time: { partitionProperties.LastEnqueuedTime }\n"
                );
            }
        }
    }
}
