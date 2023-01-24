# Minimal Reproducible for Apache Flink 

## What is that?
This minimal reproducible is for demonstrating a problem with the newly introduced `KinesisStreamsSink` in Apache Flink [1.15](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/connectors/datastream/kinesis/#kinesis-streams-sink).
When a custom `SerializationSchema` implementation is used the overwritten `open()` method is not called. 

The job creates random events from a collection and sources it. As sink an AWS Kinesis sink is used.  

## What's the problem?
When implementing a custom `SerializationSchema` for `KinesisStreamsSink` the `open()` method is not called. Hence, any initializations done there are not considered. 
In this minimum example a `com.fasterxml.jackson.databind.ObjectMapper` is used and initialized to deserialize a POJO to JSON for sinking it to AWS Kinesis. 
The initialization and set up of the `ObjectMapper` is skipped. 

This results in NPE on running the job as the `ObjectMapper` is not initialized:
```
Caused by: java.lang.NullPointerException
	at flink.sink.open.StreamingJob$CustomSchema.serialize(StreamingJob.java:77)
	at flink.sink.open.StreamingJob$CustomSchema.serialize(StreamingJob.java:61)
	at org.apache.flink.connector.kinesis.sink.KinesisStreamsSinkElementConverter.apply(KinesisStreamsSinkElementConverter.java:56)
	at org.apache.flink.connector.kinesis.sink.KinesisStreamsSinkElementConverter.apply(KinesisStreamsSinkElementConverter.java:34)
	at org.apache.flink.connector.base.sink.writer.AsyncSinkWriter.write(AsyncSinkWriter.java:330)
	at org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperator.processElement(SinkWriterOperator.java:158)
	at org.apache.flink.streaming.runtime.tasks.CopyingChainingOutput.pushToOperator(CopyingChainingOutput.java:82)
	at org.apache.flink.streaming.runtime.tasks.CopyingChainingOutput.collect(CopyingChainingOutput.java:57)
	at org.apache.flink.streaming.runtime.tasks.CopyingChainingOutput.collect(CopyingChainingOutput.java:29)
	at org.apache.flink.streaming.api.operators.CountingOutput.collect(CountingOutput.java:56)
	at org.apache.flink.streaming.api.operators.CountingOutput.collect(CountingOutput.java:29)
	at org.apache.flink.streaming.api.operators.StreamMap.processElement(StreamMap.java:38)
	at org.apache.flink.streaming.runtime.tasks.OneInputStreamTask$StreamTaskNetworkOutput.emitRecord(OneInputStreamTask.java:233)
	at org.apache.flink.streaming.runtime.io.AbstractStreamTaskNetworkInput.processElement(AbstractStreamTaskNetworkInput.java:134)
	at org.apache.flink.streaming.runtime.io.AbstractStreamTaskNetworkInput.emitNext(AbstractStreamTaskNetworkInput.java:105)
	at org.apache.flink.streaming.runtime.io.StreamOneInputProcessor.processInput(StreamOneInputProcessor.java:65)
	at org.apache.flink.streaming.runtime.tasks.StreamTask.processInput(StreamTask.java:519)
	at org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor.runMailboxLoop(MailboxProcessor.java:203)
	at org.apache.flink.streaming.runtime.tasks.StreamTask.runMailboxLoop(StreamTask.java:804)
	at org.apache.flink.streaming.runtime.tasks.StreamTask.invoke(StreamTask.java:753)
	at org.apache.flink.runtime.taskmanager.Task.runWithSystemExitMonitoring(Task.java:948)
	at org.apache.flink.runtime.taskmanager.Task.restoreAndInvoke(Task.java:927)
	at org.apache.flink.runtime.taskmanager.Task.doRun(Task.java:741)
	at org.apache.flink.runtime.taskmanager.Task.run(Task.java:563)
	at java.base/java.lang.Thread.run(Thread.java:829)
```

## How to run it?
### Pre:
The job needs AWS Kinesis to sink to. `kinesalite` is a local representation of Kinesis. It can be used according to [Flink documentation](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/connectors/datastream/kinesis/#using-custom-kinesis-endpoints).

Install local Kineses - kinesalite:
* Clone the [kinesalite repository](https://github.com/mhart/kinesalite):
    ```zsh
    git clone https://github.com/mhart/kinesalite.git
    ```
* Install kinesalite:
    ```zsh
    npm i
    ```
* Execute the `kinesalite`:
    ```zsh
    node cli.js --shardLimit 1 # or any shard limit you want
    ```

### Reproduce:

This is a fully runnable Apache Flink job. 

But the problem can also be reproduced by running the jUnit tests:
```
mvn clean test
```
