package io.confluent.connect.s3.hooks;

import io.confluent.connect.s3.S3SinkConnectorConfig;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Set;

public class NoopPreCommitHook implements PreCommitHook {

    @Override
    public void init(S3SinkConnectorConfig config) {

    }

    @Override
    public Map<TopicPartition, Set<String>> execute(Map<TopicPartition, Set<String>> filesToCommit) {
        return filesToCommit;
    }

    @Override
    public void close() {

    }
}
