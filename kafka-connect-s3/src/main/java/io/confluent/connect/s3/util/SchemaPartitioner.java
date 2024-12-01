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

import static io.confluent.connect.s3.S3SinkConnectorConfig.SCHEMA_PARTITION_AFFIX_TYPE_CONFIG;

public class SchemaPartitioner<T> implements Partitioner<T> {

  private final Partitioner<T> delegatePartitioner;
  private S3SinkConnectorConfig.AffixType schemaAffixType;
  private String delim;

  public SchemaPartitioner(Partitioner<T> delegatePartitioner) {
    this.delegatePartitioner = delegatePartitioner;
  }

  @Override
  public void configure(Map<String, Object> config) {
    this.delim = (String) config.get(StorageCommonConfig.DIRECTORY_DELIM_CONFIG);
    this.schemaAffixType = S3SinkConnectorConfig.AffixType.valueOf(
        (String) config.get(SCHEMA_PARTITION_AFFIX_TYPE_CONFIG));
    delegatePartitioner.configure(config);
  }

  @Override
  public String encodePartition(SinkRecord sinkRecord) {
    String encodePartition = this.delegatePartitioner.encodePartition(sinkRecord);
    Schema valueSchema = sinkRecord.valueSchema();
    String valueSchemaName = valueSchema != null ? valueSchema.name() : null;
    return generateSchemaBasedPath(encodePartition, valueSchemaName);
  }

  @Override
  public String encodePartition(SinkRecord sinkRecord, long nowInMillis) {
    String encodePartition = this.delegatePartitioner.encodePartition(sinkRecord, nowInMillis);
    Schema valueSchema = sinkRecord.valueSchema();
    String valueSchemaName = valueSchema != null ? valueSchema.name() : null;
    return generateSchemaBasedPath(encodePartition, valueSchemaName);
  }

  private String generateSchemaBasedPath(String encodedPartition, String schemaName) {
    if (schemaAffixType == S3SinkConnectorConfig.AffixType.PREFIX) {
      return "schema_name=" + schemaName + this.delim + encodedPartition;
    } else {
      return encodedPartition + this.delim + "schema_name=" + schemaName;
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