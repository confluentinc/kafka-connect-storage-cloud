/*
 * Add here
 */

package io.confluent.connect.s3.hooks;

import io.confluent.connect.s3.S3SinkConnectorConfig;

import java.util.Set;

public class NoopPostCommitHook implements PostCommitHook {

  @Override
  public void init(S3SinkConnectorConfig config) {

  }

  @Override
  public void put(Set<String> s3ObjectPaths, Long baseRecordTimestamp) {

  }

  @Override
  public void close() {

  }
}
