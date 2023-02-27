using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Spectre.Console;

namespace EventHubViewer
{
    class EventHubMetrics
    {
        // Maintain partition statistics to calculate differences
        public class PartitionStatistics {
            public long LastSequenceNumber { get; set; }
            public long LastOffset { get; set; }
            public DateTimeOffset LastEnqueuedTime { get; set; }
        }

        public static List<PartitionStatistics> Statistics = new List<PartitionStatistics>();

        // Build the Live Metrics table and refresh every second
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

            var metricsTable = new Table();
            metricsTable.Border = TableBorder.SimpleHeavy;
            metricsTable.AddColumn("[bold]Partition[/]");
            metricsTable.AddColumn("[bold]Last Sequence Number[/]");
            metricsTable.AddColumn("[bold]Last Offset[/]");
            metricsTable.AddColumn("[bold]Last Enqueued Time[/]");
            metricsTable.AddColumn("[bold]Messages per Second[/]");
            metricsTable.AddColumn("[bold]Throughput[/]");

            foreach (var partitionId in eventHubProperties.PartitionIds)
            {
                // Get the initial partition-level statistics
                PartitionProperties partitionProperties = await consumer.GetPartitionPropertiesAsync(partitionId, cancellationToken: cancellationSource.Token);
                PartitionStatistics partitionStatistics = new PartitionStatistics();
                partitionStatistics.LastSequenceNumber = partitionProperties.LastEnqueuedSequenceNumber;
                partitionStatistics.LastOffset = partitionProperties.LastEnqueuedOffset;
                Statistics.Add(partitionStatistics);

                // Add a table row for each partition
                metricsTable.AddRow(partitionId);
            }
            
            
            metricsTable.AddRow("");

            AnsiConsole.Live(metricsTable)
                .Start(ctx => 
                {
                    do
                    {
                        while (!Console.KeyAvailable)
                        {
                            renderTable(consumer, eventHubProperties, cancellationSource, metricsTable);
                            ctx.Refresh();
                            Thread.Sleep(1000);
                        }
                    } while (Console.ReadKey(true).Key != ConsoleKey.Escape);
                });
        }

        // Pull the partition level properties and render the updates
        private static async void renderTable(EventHubConsumerClient consumer, EventHubProperties eventHubProperties, CancellationTokenSource cancellationSource, Table metricsTable)
        {
            long totalMessagesPerSecond = 0;
            long totalKbPerSecond = 0;
            DateTime date = DateTime.UtcNow;

            foreach (var partitionId in eventHubProperties.PartitionIds)
            {
                PartitionProperties partitionProperties = await consumer.GetPartitionPropertiesAsync(partitionId, cancellationToken: cancellationSource.Token);
                metricsTable.UpdateCell(Int32.Parse(partitionId), 1, partitionProperties.LastEnqueuedSequenceNumber.ToString());
                metricsTable.UpdateCell(Int32.Parse(partitionId), 2, partitionProperties.LastEnqueuedOffset.ToString());
                metricsTable.UpdateCell(Int32.Parse(partitionId), 3, partitionProperties.LastEnqueuedTime.ToString("hh:mm:ss.ffff"));

                // Calculate messages per second. Need to update to calculate based on LastEnqueuedTime
                var messagesPerSecond = partitionProperties.LastEnqueuedSequenceNumber - Statistics[Int32.Parse(partitionId)].LastSequenceNumber;
                metricsTable.UpdateCell(Int32.Parse(partitionId), 4, $"[green]{messagesPerSecond}[/]");
                Statistics[Int32.Parse(partitionId)].LastSequenceNumber = partitionProperties.LastEnqueuedSequenceNumber;
                totalMessagesPerSecond += messagesPerSecond;

                // Calculate the throughput
                var kbPerSecond = Math.Round((partitionProperties.LastEnqueuedOffset - Statistics[Int32.Parse(partitionId)].LastOffset) / 1024.00, 2);
                metricsTable.UpdateCell(Int32.Parse(partitionId), 5, $"[green]{kbPerSecond} KB/sec[/]");
                totalKbPerSecond += (partitionProperties.LastEnqueuedOffset - Statistics[Int32.Parse(partitionId)].LastOffset);
                Statistics[Int32.Parse(partitionId)].LastOffset = partitionProperties.LastEnqueuedOffset;
            }

            // Render the totals
            metricsTable.UpdateCell(eventHubProperties.PartitionIds.Length, 3, $"[bold green]{date.ToString("hh:mm:ss.ffff")}[/]");
            metricsTable.UpdateCell(eventHubProperties.PartitionIds.Length, 4, $"[bold green]{totalMessagesPerSecond}[/]");
            var totalThroughput = Math.Round(totalKbPerSecond / 1024.00, 2);
            metricsTable.UpdateCell(eventHubProperties.PartitionIds.Length, 5, $"[bold green]{Math.Round(totalKbPerSecond / 1024.00, 2)} KB/sec[/]");
        }
    }
}
