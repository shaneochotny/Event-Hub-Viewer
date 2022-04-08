# Event Hub Viewer

This is a quick console application that acts as an Event Hub consumer client for use in various testing and debugging scenarios. It allows you to specify partition id's and start/end parameters so you can selectively consume messages of interest, or just consume all messages.

# Feature Overview

- Specify Read Location: Specify the offset, sequence number, time, and/or partitions to fetch exactly the messages you want.
- Step Through Messages: Use the keyboard to step through and read messages one at a time.
- Metadata: View system and custom application message data from the header.
- Output: View messages in the console or output to files.

# How to Run

```
dotnet run [parameters]
```

## Parameters
```
--connectionString    Required. Connection String for the Event Hub Namespace.

--eventHubName        Required. Name of the Event Hub.

--consumerGroup       Required. Consumer Group to use.

--getDetails          (Default: false) Display information about the specified
                      Event Hub and exit.

--fromStart           Start consuming messages from the first sequence number.

--fromEnd             (Default: true) Start consuming messages from the last
                      sequence number.

--messageCount        (Default: -1) Consume a specific amount of messages and
                      terminate. No limit is set by default. The timeout flag
                      will always override and terminate the client regardless
                      of messageCount.

--timeout             (Default: 300) Timeout in seconds before terminating.

--partitionId         (Default: -1) Specify the partition to consume messages
                      from.

--fromTime            Start consuming messages from the specified time up
                      until --messageCount or --timeout, whichever comes
                      first. You must also specify a --partitionId.

--fromOffset          (Default: -1) Start consuming messages from the
                      specified offset up until --messageCount or --timeout,
                      whichever comes first. You must also specify a
                      --partitionId.

--fromSequence        (Default: -1) Start consuming messages from the
                      specified sequence number up until --messageCount or
                      --timeout, whichever comes first. You must also specify
                      a --partitionId.

--outputPath          Output messages to individual files the specified
                      directory path.

--quiet               (Default: false) Don't output messages to the console.

--properties          (Default: false) Display property metadata such as
                      offset, sequence, and enqueued time.

--appProperties       (Default: false) Display any free-form properties and
                      metadata added to the event header.

--systemProperties    (Default: false) Display any system properties and
                      metadata added to the event header.

--step                (Default: false) Step through messages using the enter
                      key.

--help                Display this help screen.

--version             Display version information.
```

# Examples

### Consume messages from all partitions starting at the end
```
--connectionString "Endpoint=sb://[namespace].servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=[...]" --consumerGroup console_viewer --eventHubName events
```

### Consume messages from partition 2 starting at sequence number 3823
```
--connectionString "Endpoint=sb://[namespace].servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=[...]" --consumerGroup console_viewer --eventHubName events --partitionId 2 --fromSequence 3823
```

### Consume messages from partition 2 for sequence range 3823-3833
```
--connectionString "Endpoint=sb://[namespace].servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=[...]" --consumerGroup console_viewer --eventHubName events --partitionId 2 --fromSequence 3823 --messageCount 10
```

### Consume messages from partition 4 that were queued after a specified time
```
--connectionString "Endpoint=sb://[namespace].servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=[...]" --consumerGroup console_viewer --eventHubName events --partitionId 2 --fromTime 2022-03-10T14:59:59+00:00
```

### Consume messages from partition 0 for sequence range 3823-3833 and write them to files
```
--connectionString "Endpoint=sb://[namespace].servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=[...]" --consumerGroup console_viewer --eventHubName events --partitionId 2  --fromSequence 3823 --messageCount 10 --outputPath output_messages/
```