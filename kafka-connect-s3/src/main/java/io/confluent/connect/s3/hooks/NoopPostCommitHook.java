/*
 * Add here
 */

package io.confluent.connect.s3.hooks;

import io.confluent.connect.s3.S3SinkConnectorConfig;

import java.util.List;

public class NoopPostCommitHook implements PostCommitHook {

  @Override
  public void init(S3SinkConnectorConfig config) {

  }

  @Override
  public void put(List<String> s3ObjectPaths, List<Long> s3ObjectToBaseRecordTimestamp) {

  }

  @Override
  public void close() {

  }
}
