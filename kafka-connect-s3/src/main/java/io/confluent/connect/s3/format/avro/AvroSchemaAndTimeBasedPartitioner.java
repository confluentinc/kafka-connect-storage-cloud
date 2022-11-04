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

package io.confluent.connect.s3.format.avro;

import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;
import org.apache.kafka.connect.sink.SinkRecord;

public class AvroSchemaAndTimeBasedPartitioner<T> extends TimeBasedPartitioner<T> {

  @Override
  public String encodePartition(SinkRecord sinkRecord, long nowInMillis) {
    return sinkRecord.valueSchema().name() + getDelim()
      + super.encodePartition(sinkRecord, nowInMillis);
  }

  @Override
  public String encodePartition(SinkRecord sinkRecord) {
    return sinkRecord.valueSchema().version() + getDelim() + super.encodePartition(sinkRecord);
  }

  public boolean shouldRotateOnTime(String encodedPartition, String currentEncodedPartition) {
    return !timePart(encodedPartition).equals(timePart(currentEncodedPartition));
  }

  private String timePart(String encodedPartition) {
    return encodedPartition.split(getDelim(), 2)[1]; // TODO more validation here
  }

  private String getDelim() {
    return (String) config.get(StorageCommonConfig.DIRECTORY_DELIM_CONFIG);
  }

  @Override
  public String generatePartitionedPath(String topic, String encodedPartition) {
    // remove first schema_name
    return topic + getDelim() + timePart(encodedPartition);
  }

}
