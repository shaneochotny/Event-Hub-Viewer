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
    class Core
    {
        static async Task Main(string[] args)
        {
            // Parse the input arguments provided by the user
            var parser = new CommandLine.Parser(with=> with.HelpWriter=null);
		    var parserResult = await parser.ParseArguments<CommandLineOptions>(args).WithParsedAsync<CommandLineOptions>(async clOptions => 
            {
                if (clOptions.getLiveMetrics) {
                    await EventHubMetrics.getLiveMetrics(clOptions);
                } else if (clOptions.getDetails) {
                    await EventHubDetails.getDetails(clOptions);
                } else if (clOptions.partitionId == -1) {
                    await Consumers.AllPartitions(clOptions);
                } else if (clOptions.partitionId > -1) {
                    await Consumers.Partition(clOptions);
                }
            });

            parserResult.WithNotParsed(errs=> CommandLineOptions.DisplayHelp(parserResult, errs));
        }
    }
}
