using System;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using CommandLine;

namespace EventHubViewer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            // Parse the input arguments provided by the user
            var parser = new CommandLine.Parser(with=> with.HelpWriter=null);
		    var parserResult = await parser.ParseArguments<CommandLineOptions>(args).WithParsedAsync<CommandLineOptions>(async clOptions => 
            {
                if (clOptions.getDetails) {
                    await EventHubDetails(clOptions);
                } else if (clOptions.partitionId == -1) {
                    await ConsumeAllPartitions(clOptions);
                } else if (clOptions.partitionId > -1) {
                    await ConsumePartition(clOptions);
                }
            });
            
            parserResult.WithNotParsed(errs=> CommandLineOptions.DisplayHelp(parserResult, errs));
        }

        // Display details from the Event Hub and Partitions and then exit
        private static async Task EventHubDetails(CommandLineOptions clOptions)
        {
            using CancellationTokenSource cancellationSource = new CancellationTokenSource();
            var consumer = new EventHubConsumerClient(clOptions.consumerGroup, clOptions.connectionString, clOptions.eventHubName);
            EventHubProperties eventHubProperties = await consumer.GetEventHubPropertiesAsync(cancellationToken: cancellationSource.Token);
            
            Console.WriteLine($"Event Hub Namespace: { consumer.FullyQualifiedNamespace }");
            Console.WriteLine($"Event Hub Name: { eventHubProperties.Name }");
            Console.WriteLine($"Created On: { eventHubProperties.CreatedOn }");
            Console.WriteLine($"Total Partitions: { eventHubProperties.PartitionIds.Length }\r\n");

            foreach (var partitionId in eventHubProperties.PartitionIds)
            {
                PartitionProperties partitionProperties = await consumer.GetPartitionPropertiesAsync(partitionId, cancellationToken: cancellationSource.Token);
                Console.WriteLine($"Partition: { partitionId }");
                Console.WriteLine($"\tHas Messages: { (partitionProperties.IsEmpty ? "No" : "Yes") }");
                Console.WriteLine($"\tFirst Sequence Number: { partitionProperties.BeginningSequenceNumber }");
                Console.WriteLine($"\tLast Sequence Number: { partitionProperties.LastEnqueuedSequenceNumber }");
                Console.WriteLine($"\tLast Offset Number: { partitionProperties.LastEnqueuedOffset }");
                Console.WriteLine($"\tLast Enqueued Time: { partitionProperties.LastEnqueuedTime }\r\n");
            }
        }

        // Consume messages from all partitions within the Event Hub from either the beginning or the end
        private static async Task<int> ConsumeAllPartitions(CommandLineOptions clOptions)
        {
            var consumerOptions = new EventHubConsumerClientOptions();
            consumerOptions.RetryOptions.Mode = EventHubsRetryMode.Fixed;
            consumerOptions.RetryOptions.MaximumRetries = 5;

            var consumer = new EventHubConsumerClient(clOptions.consumerGroup, clOptions.connectionString, clOptions.eventHubName, consumerOptions);
            string namespaceName = consumer.FullyQualifiedNamespace.Substring(0, consumer.FullyQualifiedNamespace.IndexOf("."));

            Console.WriteLine($"Event Hub Namespace: { consumer.FullyQualifiedNamespace }");
            Console.WriteLine($"Event Hub Name: { consumer.EventHubName }");
            Console.WriteLine($"Consuming from Partition: All");
            Console.WriteLine($"Consuming from: {((clOptions.fromStart == true) ? "Start" : "End")}");
            Console.WriteLine($"Messages to Consume: {((clOptions.messageCount == -1) ? "All" : clOptions.messageCount)}\r\n");

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

                    await outputMessage(namespaceName, consumer.EventHubName, partitionEvent, clOptions.outputPath, clOptions.quiet);
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
        private static async Task<int> ConsumePartition(CommandLineOptions clOptions)
        {
            var consumerOptions = new EventHubConsumerClientOptions();
            consumerOptions.RetryOptions.Mode = EventHubsRetryMode.Fixed;
            consumerOptions.RetryOptions.MaximumRetries = 5;

            var consumer = new EventHubConsumerClient(clOptions.consumerGroup, clOptions.connectionString, clOptions.eventHubName, consumerOptions);
            string namespaceName = consumer.FullyQualifiedNamespace.Substring(0, consumer.FullyQualifiedNamespace.IndexOf("."));

            Console.WriteLine($"Event Hub Namespace: { consumer.FullyQualifiedNamespace }");
            Console.WriteLine($"Event Hub Name: { consumer.EventHubName }");
            Console.WriteLine($"Consuming from Partition: {clOptions.partitionId }");

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
    
                    await outputMessage(namespaceName, consumer.EventHubName, partitionEvent, clOptions.outputPath, clOptions.quiet);
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

        private static async Task outputMessage(string namespaceName, string eventHubName, PartitionEvent partitionEvent, string outputPath, bool quiet)
        {
            if (!quiet) {
                Console.WriteLine($"Enqueued: { partitionEvent.Data.EnqueuedTime.ToString() }");
                Console.WriteLine($"Partition: { partitionEvent.Partition.PartitionId }");
                Console.WriteLine($"Sequence: { partitionEvent.Data.SequenceNumber }");
                Console.WriteLine($"Offset: { partitionEvent.Data.Offset }");
                Console.WriteLine($"Bytes: { partitionEvent.Data.EventBody.ToArray().Length }");
                Console.WriteLine("Body:");
                Console.WriteLine(partitionEvent.Data.EventBody.ToString());
                Console.WriteLine("");
            } 
            
            if (outputPath != "") {
                await File.WriteAllTextAsync($"{outputPath}/{namespaceName}-{eventHubName}-partition-{partitionEvent.Partition.PartitionId}-seq-{partitionEvent.Data.SequenceNumber}-offset-{partitionEvent.Data.Offset}", partitionEvent.Data.EventBody.ToString());
            }
        }
    }
}
