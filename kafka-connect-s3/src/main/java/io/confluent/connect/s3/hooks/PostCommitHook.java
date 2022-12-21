/*
 * Add here
 */

package io.confluent.connect.s3.hooks;

import io.confluent.connect.s3.S3SinkConnectorConfig;

import java.util.List;

/**
 * An interface represent an action to be performed after a file is commited to S3.
 */
public interface PostCommitHook {

  void init(S3SinkConnectorConfig config);

  void put(List<String> s3ObjectPaths, List<Long> s3ObjectToBaseRecordTimestamp);

  void close();
}
