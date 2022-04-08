using System;
using System.Collections.Generic;
using System.IO;
using Azure.Messaging.EventHubs.Consumer;

namespace EventHubViewer
{
    class Output
    {
        public static async void outputMessage(string namespaceName, string eventHubName, PartitionEvent partitionEvent, CommandLineOptions clOptions)
        {
            if (!clOptions.quiet) {
                if (clOptions.properties) {
                    Console.WriteLine(
                        $"Enqueued: { partitionEvent.Data.EnqueuedTime.ToString() }\n" +
                        $"Partition: { partitionEvent.Partition.PartitionId }\n" +
                        $"Sequence: { partitionEvent.Data.SequenceNumber }\n" +
                        $"Offset: { partitionEvent.Data.Offset }\n" +
                        $"Bytes: { partitionEvent.Data.EventBody.ToArray().Length }\n" +
                        $"ContentType: { partitionEvent.Data.ContentType }\n" +
                        $"CorrelationId: { partitionEvent.Data.CorrelationId }\n" +
                        $"MessageId: { partitionEvent.Data.MessageId }"
                    );
                }
                
                if (clOptions.appProperties) {
                    Console.WriteLine("Application Properties:");
                    foreach (KeyValuePair<string, object> appProperty in partitionEvent.Data.Properties)
                    {
                        Console.WriteLine($"\t{appProperty.Key}: {appProperty.Value}");
                    }
                }
                
                if (clOptions.systemProperties) {
                    Console.WriteLine("System Properties:");
                    foreach (KeyValuePair<string, object> systemProperty in partitionEvent.Data.SystemProperties)
                    {
                        Console.WriteLine($"\t{systemProperty.Key}: {systemProperty.Value}");
                    }
                }

                Console.WriteLine($"Body:\n{partitionEvent.Data.EventBody.ToString()}\n");

                if (clOptions.step) {
                    Console.WriteLine("Enter for next message...");
                    while (Console.ReadKey().Key != ConsoleKey.Enter) {}
                }
            } 
            
            if (clOptions.outputPath != null) {
                await File.WriteAllTextAsync(
                    $"{clOptions.outputPath}/" + 
                    $"{namespaceName}-{eventHubName}-partition-{partitionEvent.Partition.PartitionId}-seq-{partitionEvent.Data.SequenceNumber}-offset-{partitionEvent.Data.Offset}", 
                    partitionEvent.Data.EventBody.ToString()
                );
            }
        }
    }
}
