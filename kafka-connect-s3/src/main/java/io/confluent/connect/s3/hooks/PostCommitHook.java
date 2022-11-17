/*
 * Add here
 */

package io.confluent.connect.s3.hooks;

import io.confluent.connect.s3.S3SinkConnectorConfig;

import java.util.Set;

/**
 * An interface represent an action to be performed after a file is commited to S3.
 */
public interface PostCommitHook {

  void init(S3SinkConnectorConfig config);

  void put(Set<String> s3ObjectPath);

  void close();
}
