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

import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.partitioner.Partitioner;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

public class SchemaPartitioner<T> implements Partitioner<T> {

  private final Partitioner<T> delegatePartitioner;
  private final S3SinkConnectorConfig.AffixType schemaAffixType;
  protected String delim;

  public SchemaPartitioner(Map<String, Object> config,
      S3SinkConnectorConfig.AffixType schemaAffixType, Partitioner<T> delegatePartitioner) {
    this.delim = (String)config.get(StorageCommonConfig.DIRECTORY_DELIM_CONFIG);
    this.delegatePartitioner = delegatePartitioner;
    this.schemaAffixType = schemaAffixType;
  }

  @Override
  public void configure(Map<String, Object> map) {
    delegatePartitioner.configure(map);
  }

  @Override
  public String encodePartition(SinkRecord sinkRecord) {
    String encodePartition = this.delegatePartitioner.encodePartition(sinkRecord);
    Schema valueSchema = sinkRecord.valueSchema();
    if (valueSchema != null) {
      encodePartition = generateSchemaBasedPath(encodePartition, valueSchema);
    }
    return encodePartition;
  }

  @Override
  public String encodePartition(SinkRecord sinkRecord, long nowInMillis) {
    String encodePartition = this.delegatePartitioner.encodePartition(sinkRecord, nowInMillis);
    Schema valueSchema = sinkRecord.valueSchema();
    if (valueSchema != null) {
      encodePartition = generateSchemaBasedPath(encodePartition, valueSchema);
    }
    return encodePartition;
  }

  private String generateSchemaBasedPath(String encodedPartition, Schema valueSchema) {
    if (schemaAffixType == S3SinkConnectorConfig.AffixType.PREFIX) {
      return "schema_name=" + valueSchema.name() + this.delim + encodedPartition;
    } else {
      return encodedPartition + this.delim + "schema_name=" + valueSchema.name();
    }
  }

  @Override
  public String generatePartitionedPath(String topic, String encodedPartition) {
    return delegatePartitioner.generatePartitionedPath(topic, encodedPartition);
  }

  @Override
  public List<T> partitionFields() {
    return delegatePartitioner.partitionFields();
  }
}