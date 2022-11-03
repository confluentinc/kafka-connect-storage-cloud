package io.confluent.connect.s3.hooks;

import io.confluent.connect.s3.S3SinkConnectorConfig;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Set;

/**
 * An interface represent an action to be performed before offsets are committed back to kafka connect.
 */
public interface PreCommitHook {

    void init(S3SinkConnectorConfig config);

    /**
     * @param filesToCommit Files already uploaded to S3 which offsets we can commit
     * @return files which we already ran the hook on
     */
    Map<TopicPartition, Set<String>> execute(Map<TopicPartition, Set<String>> filesToCommit);

    void close();
}
