using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Spectre.Console;

namespace EventHubViewer
{
    class EventHubMetrics
    {
        public class PartitionStatistics {
            public long LastSequenceNumber { get; set; }
            public long LastOffset { get; set; }
            public DateTimeOffset LastEnqueuedTime { get; set; }
        }

        public static List<PartitionStatistics> Statistics = new List<PartitionStatistics>();

        // Display details from the Event Hub and Partitions and then exit
        public static async Task getLiveMetrics(CommandLineOptions clOptions)
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

            var table = new Table();
            table.AddColumn("Partition");
            table.AddColumn("Last Sequence Number");
            table.AddColumn("Last Offset");
            table.AddColumn("Last Enqueued Time");
            table.AddColumn("Messages per Second");
            table.AddColumn("Bytes per Second");

            foreach (var partitionId in eventHubProperties.PartitionIds)
            {

                PartitionProperties partitionProperties = await consumer.GetPartitionPropertiesAsync(partitionId, cancellationToken: cancellationSource.Token);
                PartitionStatistics partitionStatistics = new PartitionStatistics();
                partitionStatistics.LastSequenceNumber = partitionProperties.LastEnqueuedSequenceNumber;
                partitionStatistics.LastOffset = partitionProperties.LastEnqueuedOffset;
                Statistics.Add(partitionStatistics);

                table.AddRow(partitionId);
            }

            AnsiConsole.Live(table)
                .Start(ctx => 
                {
                    do
                    {
                        while (!Console.KeyAvailable)
                        {
                            renderTable(consumer, eventHubProperties, cancellationSource, table);
                            ctx.Refresh();
                            Thread.Sleep(1000);
                        }
                    } while (Console.ReadKey(true).Key != ConsoleKey.Escape);
                });

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

        private static async void renderTable(EventHubConsumerClient consumer, EventHubProperties eventHubProperties, CancellationTokenSource cancellationSource, Table table)
        {
            foreach (var partitionId in eventHubProperties.PartitionIds)
            {
                PartitionProperties partitionProperties = await consumer.GetPartitionPropertiesAsync(partitionId, cancellationToken: cancellationSource.Token);
                table.UpdateCell(Int32.Parse(partitionId), 1, partitionProperties.LastEnqueuedSequenceNumber.ToString());
                table.UpdateCell(Int32.Parse(partitionId), 2, partitionProperties.LastEnqueuedOffset.ToString());
                table.UpdateCell(Int32.Parse(partitionId), 3, partitionProperties.LastEnqueuedTime.ToString("hh:mm.ss:ffff"));

                var messagesPerSecond = partitionProperties.LastEnqueuedSequenceNumber - Statistics[Int32.Parse(partitionId)].LastSequenceNumber;
                table.UpdateCell(Int32.Parse(partitionId), 4, $"[green]{messagesPerSecond}[/]");
                Statistics[Int32.Parse(partitionId)].LastSequenceNumber = partitionProperties.LastEnqueuedSequenceNumber;

                var bytesPerSecond = (partitionProperties.LastEnqueuedOffset - Statistics[Int32.Parse(partitionId)].LastOffset) / 1024.00;
                table.UpdateCell(Int32.Parse(partitionId), 5, $"[green]{bytesPerSecond}[/]");
                Statistics[Int32.Parse(partitionId)].LastOffset = partitionProperties.LastEnqueuedOffset;
            }
        }
    }
}
