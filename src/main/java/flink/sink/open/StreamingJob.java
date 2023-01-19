package flink.sink.open;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import static org.apache.flink.kinesis.shaded.org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_ENDPOINT;
import static org.apache.flink.kinesis.shaded.org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_REGION;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.STREAM_INITIAL_POSITION;

public class StreamingJob {

    private final static Logger LOGGER = LoggerFactory.getLogger(StreamingJob.class);

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        var input = new ArrayList<EventUnderTest>();
        for (int i = 1; i < 100; i++) {
            input.add(new EventUnderTest(i));
        }
        var source = env.fromCollection(input);

        var stream = source.map(e -> {
            LOGGER.info("processing element: " + e);
            return e;
        });

        var kdsSink = createKinesisStreamSink();

        stream.sinkTo(kdsSink);

        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }

    private static KinesisStreamsSink<EventUnderTest> createKinesisStreamSink() {
        var sinkProperties = new Properties();
        sinkProperties.put(AWS_REGION, "eu-central-1");
        sinkProperties.put(AWS_ENDPOINT, "http://localhost:4567");
        sinkProperties.put(STREAM_INITIAL_POSITION, "TRIM_HORIZON");

        return KinesisStreamsSink.<EventUnderTest>builder()
                .setKinesisClientProperties(sinkProperties)
                .setSerializationSchema(new CustomSchema())
                .setPartitionKeyGenerator(element -> String.valueOf(element.hashCode()))
                .setStreamName("my-stream-name")
                .build();
    }

    private static class CustomSchema implements SerializationSchema<EventUnderTest> {

        private final static Logger LOGGER = LoggerFactory.getLogger(CustomSchema.class);

        private transient ObjectMapper mapper;

        @Override
        public void open(InitializationContext context) throws Exception {
            SerializationSchema.super.open(context);
            LOGGER.info("Open called!");
            this.mapper = new ObjectMapper();
        }

        @Override
        public byte[] serialize(EventUnderTest element) {
            try {
                return mapper.writeValueAsBytes(element);
            } catch (IOException e) {
                LOGGER.error("Cannot deserialize the incoming object to json: {}", element);
                throw new RuntimeException(e.getMessage());
            }
        }
    }

    private static class EventUnderTest {
        private final Integer attr;

        public EventUnderTest(Integer attr) {
            this.attr = attr;
        }
    }
}
