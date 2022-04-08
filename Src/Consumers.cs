using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using CommandLine;

namespace EventHubViewer
{
    class Consumers
    {
        // Consume messages from all partitions within the Event Hub from either the beginning or the end
        public static async Task<int> AllPartitions(CommandLineOptions clOptions)
        {
            var consumerOptions = new EventHubConsumerClientOptions();
            consumerOptions.RetryOptions.Mode = EventHubsRetryMode.Fixed;
            consumerOptions.RetryOptions.MaximumRetries = 5;

            var consumer = new EventHubConsumerClient(clOptions.consumerGroup, clOptions.connectionString, clOptions.eventHubName, consumerOptions);
            string namespaceName = consumer.FullyQualifiedNamespace.Substring(0, consumer.FullyQualifiedNamespace.IndexOf("."));

            Console.WriteLine(
                $"Event Hub Namespace: { consumer.FullyQualifiedNamespace }\n" +
                $"Event Hub Name: { consumer.EventHubName }\n" +
                $"Consuming from Partition: All\n" +
                $"Consuming from: {((clOptions.fromStart == true) ? "Start" : "End")}\n" +
                $"Messages to Consume: {((clOptions.messageCount == -1) ? "All" : clOptions.messageCount)}\n"
            );

            try
            {
                // Create a cancellation token and configure the timeout for when we should stop consuming message (default: 300 seconds)
                using CancellationTokenSource cancellationSource = new CancellationTokenSource();
                cancellationSource.CancelAfter(TimeSpan.FromSeconds(clOptions.timeout));
                int receivedMessages = 0;

                // Start consuming messages from all partitions
                await foreach (PartitionEvent partitionEvent in consumer.ReadEventsAsync(
                    startReadingAtEarliestEvent: clOptions.fromStart,
                    cancellationToken: cancellationSource.Token))
                {
                    // If we reached the provided --messageCount, then stop consuming messages
                    if (clOptions.messageCount != -1 && receivedMessages++ >= clOptions.messageCount) {
                        cancellationSource.Cancel();
                    }

                    Output.outputMessage(namespaceName, consumer.EventHubName, partitionEvent, clOptions);
                }
            }
            catch (TaskCanceledException)
            {
                Console.WriteLine($"Terminated");
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
            finally
            {
                await consumer.CloseAsync();
            }

            return 0;
        }

        // Consume messages as single partition within the Event Hub
        public static async Task<int> Partition(CommandLineOptions clOptions)
        {
            var consumerOptions = new EventHubConsumerClientOptions();
            consumerOptions.RetryOptions.Mode = EventHubsRetryMode.Fixed;
            consumerOptions.RetryOptions.MaximumRetries = 5;

            var consumer = new EventHubConsumerClient(clOptions.consumerGroup, clOptions.connectionString, clOptions.eventHubName, consumerOptions);
            string namespaceName = consumer.FullyQualifiedNamespace.Substring(0, consumer.FullyQualifiedNamespace.IndexOf("."));

            Console.WriteLine(
                $"Event Hub Namespace: { consumer.FullyQualifiedNamespace }\n" +
                $"Event Hub Name: { consumer.EventHubName }\n" +
                $"Consuming from Partition: {clOptions.partitionId }\n"
            );

            try
            {
                // Create a cancellation token and configure the timeout for when we should stop consuming message (default: 300 seconds)
                using CancellationTokenSource cancellationSource = new CancellationTokenSource();
                cancellationSource.CancelAfter(TimeSpan.FromSeconds(clOptions.timeout));
                int receivedMessages = 0;
                PartitionProperties properties = await consumer.GetPartitionPropertiesAsync(clOptions.partitionId.ToString(), cancellationSource.Token);
                EventPosition startingPosition;

                // Determine where and when the user wants to consume messages from
                if (clOptions.fromStart) {
                    startingPosition = EventPosition.Earliest;
                    Console.WriteLine($"Consuming from: Start");
                } else if (clOptions.fromTime != null) {
                    if (DateTime.TryParse(clOptions.fromTime, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime time)) {
                        startingPosition = EventPosition.FromEnqueuedTime(time);
                        Console.WriteLine($"Consuming from Time: {time.ToString()}");
                    } else {
                        throw new FormatException("Error: Invalid --formTime timestamp. Needs to be in ISO 8601 (i.e. 2022-03-10T14:59:59+00:00).");
                    }
                } else if (clOptions.fromOffset > -1) {
                    startingPosition = EventPosition.FromOffset(clOptions.fromOffset);
                    Console.WriteLine($"Consuming from Offset: {clOptions.fromOffset}");
                } else if (clOptions.fromSequence > -1) {
                    startingPosition = EventPosition.FromSequenceNumber(clOptions.fromSequence);
                    Console.WriteLine($"Consuming from Sequence Number: {clOptions.fromSequence}");
                } else {
                    startingPosition = EventPosition.Latest;
                    Console.WriteLine($"Consuming from: End");
                }

                Console.WriteLine($"Messages to Consume: {((clOptions.messageCount == -1) ? "All" : clOptions.messageCount)}\r\n");

                // Start consuming messages from the provided partition id
                await foreach (PartitionEvent partitionEvent in consumer.ReadEventsFromPartitionAsync(
                    clOptions.partitionId.ToString(),
                    startingPosition,
                    cancellationSource.Token))
                {
                    // If we reached the provided --messageCount, then stop consuming messages
                    if (clOptions.messageCount != -1 && receivedMessages++ >= clOptions.messageCount) {
                        cancellationSource.Cancel();
                    }
    
                    Output.outputMessage(namespaceName, consumer.EventHubName, partitionEvent, clOptions);
                }
            }
            catch (TaskCanceledException)
            {
                Console.WriteLine($"Terminated");
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
            finally
            {
                await consumer.CloseAsync();
            }

            return 0;
        }
    }
}
