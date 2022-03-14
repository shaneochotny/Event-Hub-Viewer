using CommandLine;
using CommandLine.Text;
using System;
using System.Collections.Generic;

namespace EventHubViewer
{    public class CommandLineOptions
    {
        [Option("connectionString", Required = true, HelpText = "Connection String for the Event Hub Namespace.")]
        public string connectionString { get; set; }

        [Option("eventHubName", Required = true, HelpText = "Name of the Event Hub.")]
        public string eventHubName { get; set; }
            
        [Option("consumerGroup", Required = true, HelpText = "Consumer Group to use.")]
        public string consumerGroup { get; set; }

        [Option ("getDetails", Default = false, HelpText = "Display information about the specified Event Hub and exit.")]
        public bool getDetails { get; set; }

        [Option ("fromStart", SetName = "fromStart", HelpText = "Start consuming messages from the first sequence number.")]
        public bool fromStart { get; set; }

        [Option ("fromEnd", SetName = "fromEnd", Default = true, HelpText = "Start consuming messages from the last sequence number.")]
        public bool fromEnd { get; set; }

        [Option ("messageCount", Default = -1, HelpText = "Consume a specific amount of messages and terminate. No limit is set by default. The timeout flag will always override and terminate the client regardless of messageCount.")]
        public int messageCount { get; set; }

        [Option ("timeout", Default = 300, HelpText = "Timeout in seconds before terminating.")]
        public int timeout { get; set; }

        [Option ("partitionId", Default = -1, HelpText = "Specify the partition to consume messages from.")]
        public int partitionId { get; set; }

        [Option ("fromTime", SetName = "fromTime", HelpText = "Start consuming messages from the specified time up until --messageCount or --timeout, whichever comes first. You must also specify a --partitionId.")]
        public string fromTime { get; set; }

        [Option ("fromOffset", Default = -1, SetName = "fromOffset", HelpText = "Start consuming messages from the specified offset up until --messageCount or --timeout, whichever comes first. You must also specify a --partitionId.")]
        public long fromOffset { get; set; }

        [Option ("fromSequence", Default = -1, SetName = "fromSequence", HelpText = "Start consuming messages from the specified sequence number up until --messageCount or --timeout, whichever comes first. You must also specify a --partitionId.")]
        public long fromSequence { get; set; }

        [Option ("outputPath", HelpText = "Output messages to individual files the specified directory path.")]
        public string outputPath { get; set; }

        [Option ("quiet", Default = false, HelpText = "Don't output messages to the console.")]
        public bool quiet { get; set; }

        [Usage(ApplicationAlias = "ehviewer")]
        public static IEnumerable<Example> Examples
        {
            get
            {
                yield return new Example("Consume messages from all partitions starting at the end", new CommandLineOptions { connectionString = "connString", eventHubName = "events", consumerGroup = "console_viewer" });
                yield return new Example("Consume messages from partition 2 starting at sequence number 3823", new CommandLineOptions { connectionString = "connString", eventHubName = "events", consumerGroup = "console_viewer", partitionId = 2, fromSequence = 3823 });
                yield return new Example("Consume messages from partition 2 for sequence range 3823-3833", new CommandLineOptions { connectionString = "connString", eventHubName = "events", consumerGroup = "console_viewer", partitionId = 2, fromSequence = 3823, messageCount = 10 });
            }
        }

        public static void DisplayHelp<T>(ParserResult<T> result, IEnumerable< Error> errs)
        {
            var helpText= HelpText.AutoBuild(result, h =>
            {
                h.AdditionalNewLineAfterOption = true;
                h.Heading = "Event Hub Viewer 0.1";
                h.Copyright = "";   
                return HelpText.DefaultParsingErrorsHandler(result, h);
            } , e => e);	
            
            Console.WriteLine(helpText);
        }
    }
}