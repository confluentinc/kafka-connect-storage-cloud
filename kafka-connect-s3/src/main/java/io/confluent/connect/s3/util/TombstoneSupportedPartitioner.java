/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.s3.util;

import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.Partitioner;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;

public class TombstoneSupportedPartitioner<T> extends DefaultPartitioner<T> {

  private final Partitioner<T> delegatePartitioner;
  private final String tombstonePartition;

  public TombstoneSupportedPartitioner(Partitioner<T> delegatePartitioner,
      String tombstonePartition) {
    this.delegatePartitioner = delegatePartitioner;
    this.tombstonePartition = tombstonePartition;
  }

  @Override
  public void configure(Map<String, Object> map) {
    delegatePartitioner.configure(map);
  }

  @Override
  public String encodePartition(SinkRecord sinkRecord) {
    return sinkRecord.value() == null ? this.tombstonePartition
        : delegatePartitioner.encodePartition(sinkRecord);
  }

  @Override
  public String generatePartitionedPath(String s, String s1) {
    return delegatePartitioner.generatePartitionedPath(s, s1);
  }

  @Override
  public List<T> partitionFields() {
    return delegatePartitioner.partitionFields();
  }
}
