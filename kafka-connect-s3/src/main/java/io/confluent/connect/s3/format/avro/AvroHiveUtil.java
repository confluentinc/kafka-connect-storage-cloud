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

import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import java.util.List;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.errors.HiveMetaStoreException;
import io.confluent.connect.storage.hive.HiveMetaStore;
import io.confluent.connect.storage.hive.HiveSchemaConverter;
import io.confluent.connect.storage.hive.HiveUtil;
import io.confluent.connect.storage.partitioner.Partitioner;
import org.apache.kafka.connect.data.SchemaBuilder;

public class AvroHiveUtil extends HiveUtil {

  private static final String AVRO_SERDE = "org.apache.hadoop.hive.serde2.avro.AvroSerDe";
  private static final String AVRO_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.avro"
      + ".AvroContainerInputFormat";
  private static final String AVRO_OUTPUT_FORMAT = "org.apache.hadoop.hive.ql.io.avro"
      + ".AvroContainerOutputFormat";
  private static final String AVRO_SCHEMA_LITERAL = "avro.schema.literal";
  private final AvroData avroData;
  private final S3SinkConnectorConfig config;
  private final String s3Protocol;

  public AvroHiveUtil(
      S3SinkConnectorConfig conf, AvroData avroData, HiveMetaStore
      hiveMetaStore
  ) {
    super(conf, hiveMetaStore);
    this.avroData = avroData;
    this.config = conf;
    this.s3Protocol = conf.getHiveS3Protocol();
  }

  @Override
  public void createTable(String database,
                          String tableName,
                          Schema schema,
                          Partitioner partitioner,
                          String topic)
          throws HiveMetaStoreException {
    Table table = constructAvroTable(database, tableName, schema, partitioner, topic);
    hiveMetaStore.createTable(table);
  }

  @Override
  public void alterSchema(
      String database,
      String tableName,
      Schema schema
  ) throws HiveMetaStoreException {
    Table table = hiveMetaStore.getTable(database, tableName);
    Schema filteredSchema = excludePartitionFieldsFromSchema(schema, table.getPartitionKeys());
    table.getParameters().put(AVRO_SCHEMA_LITERAL,
        avroData.fromConnectSchema(filteredSchema).toString());
    hiveMetaStore.alterTable(table);
  }

  private Table constructAvroTable(
      String database,
      String tableName,
      Schema schema,
      Partitioner partitioner,
      String topic
  )
      throws HiveMetaStoreException {
    Table table = newTable(database, tableName);
    table.setTableType(TableType.EXTERNAL_TABLE);
    table.getParameters().put("EXTERNAL", "TRUE");

    final String s3Protocol = config.getHiveS3Protocol();
    final String s3BucketName = config.getBucketName();
    final String topicsDir = config.getString(StorageCommonConfig.TOPICS_DIR_CONFIG);
    String tablePath = hiveDirectoryName(
            String.format("%s://%s", s3Protocol, s3BucketName),
            topicsDir,
            topic);
    table.setDataLocation(new Path(tablePath));
    table.setSerializationLib(AVRO_SERDE);
    try {
      table.setInputFormatClass(AVRO_INPUT_FORMAT);
      table.setOutputFormatClass(AVRO_OUTPUT_FORMAT);
    } catch (HiveException e) {
      throw new HiveMetaStoreException("Cannot find input/output format:", e);
    }
    Schema filteredSchema = excludePartitionFieldsFromSchema(schema, partitioner.partitionFields());
    List<FieldSchema> columns = HiveSchemaConverter.convertSchema(filteredSchema);
    table.setFields(columns);
    table.setPartCols(partitioner.partitionFields());
    table.getParameters().put(AVRO_SCHEMA_LITERAL,
        avroData.fromConnectSchema(filteredSchema).toString());
    return table;
  }

  /**
   * Remove the column(s) that is later re-created by Hive when using the
   * {@code partition.field.name} config.
   *
   * @param originalSchema the old schema to remove fields from
   * @param partitionFields the fields used for partitioning
   * @return the new schema without the fields used for partitioning
   */
  private Schema excludePartitionFieldsFromSchema(
      Schema originalSchema,
      List<FieldSchema> partitionFields
  ) {
    Set<String> partitions = partitionFields.stream()
        .map(FieldSchema::getName).collect(Collectors.toSet());

    SchemaBuilder newSchema = SchemaBuilder.struct();
    for (Field field : originalSchema.fields()) {
      if (!partitions.contains(field.name())) {
        newSchema.field(field.name(), field.schema());
      }
    }
    return newSchema;
  }
}
