package flink.sink.open;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StreamingJobTest {

    @ClassRule
    public static final MiniClusterWithClientResource flinkCluster = new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder().setNumberSlotsPerTaskManager(1)
                    .setNumberTaskManagers(1)
                    .build());
    protected StreamExecutionEnvironment env;

    @BeforeEach
    public void beforeEach() {
        this.env = StreamExecutionEnvironment.getExecutionEnvironment();
        this.env.setParallelism(1);
    }

    @AfterEach
    public void afterEach() {
        flinkCluster.after();
    }

    @Test
    public void should_callOpen_when_usingCustomSerializationSchema() throws Exception {
        StreamingJob.setupStreamExecutionEnvironment(this.env);
    }
}
