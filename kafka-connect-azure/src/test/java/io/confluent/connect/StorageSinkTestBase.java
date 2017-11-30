package io.confluent.connect;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.After;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class StorageSinkTestBase {
    protected static final String TOPIC = "test-topic";
    protected static final int PARTITION = 12;
    protected static final int PARTITION2 = 13;
    protected static final int PARTITION3 = 14;
    protected static final TopicPartition TOPIC_PARTITION = new TopicPartition("test-topic", 12);
    protected static final TopicPartition TOPIC_PARTITION2 = new TopicPartition("test-topic", 13);
    protected static final TopicPartition TOPIC_PARTITION3 = new TopicPartition("test-topic", 14);
    protected Map<String, String> properties;
    protected String url;

    public MockSinkTaskContext getContext() {
        return context;
    }

    protected StorageSinkTestBase.MockSinkTaskContext context;

    public StorageSinkTestBase() {
    }

    protected Map<String, String> createProps() {
        HashMap<String,String> props = new HashMap<>();
        props.put("store.url", this.url);
        props.put("flush.size", "3");
        return props;
    }

    public void setUp() throws Exception {
        this.properties = this.createProps();
        HashSet<TopicPartition> assignment = new HashSet<>();
        assignment.add(TOPIC_PARTITION);
        assignment.add(TOPIC_PARTITION2);
        this.context = new MockSinkTaskContext(assignment);
    }

    @After
    public void tearDown() throws Exception {
    }

    protected static class MockSinkTaskContext implements SinkTaskContext {
        private final Map<TopicPartition, Long> offsets = new HashMap<>();
        private long timeoutMs = -1L;
        private Set<TopicPartition> assignment;

        public MockSinkTaskContext(Set<TopicPartition> assignment) {
            this.assignment = assignment;
        }

        public void offset(Map<TopicPartition, Long> offsets) {
            this.offsets.putAll(offsets);
        }

        public void offset(TopicPartition tp, long offset) {
            this.offsets.put(tp, Long.valueOf(offset));
        }

        public Map<TopicPartition, Long> offsets() {
            return this.offsets;
        }

        public void timeout(long timeoutMs) {
            this.timeoutMs = timeoutMs;
        }

        public long timeout() {
            return this.timeoutMs;
        }

        public Set<TopicPartition> assignment() {
            return this.assignment;
        }

        public void setAssignment(Set<TopicPartition> nextAssignment) {
            this.assignment = nextAssignment;
        }

        public void pause(TopicPartition... partitions) {
        }

        public void resume(TopicPartition... partitions) {
        }

        public void requestCommit() {
        }
    }
}
