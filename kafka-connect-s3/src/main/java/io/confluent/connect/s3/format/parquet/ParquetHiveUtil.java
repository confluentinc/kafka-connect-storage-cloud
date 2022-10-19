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

package io.confluent.connect.s3.format.parquet;

import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.storage.common.StorageCommonConfig;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.kafka.connect.data.Schema;

import java.util.List;

import io.confluent.connect.storage.errors.HiveMetaStoreException;
import io.confluent.connect.storage.hive.HiveMetaStore;
import io.confluent.connect.storage.hive.HiveSchemaConverter;
import io.confluent.connect.storage.hive.HiveUtil;
import io.confluent.connect.storage.partitioner.Partitioner;

public class ParquetHiveUtil extends HiveUtil {
  private final S3SinkConnectorConfig config;

  public ParquetHiveUtil(S3SinkConnectorConfig conf, HiveMetaStore hiveMetaStore) {
    super(conf, hiveMetaStore);
    this.config = conf;
  }

  @Override
  public void createTable(
      String database,
      String tableName,
      Schema schema,
      Partitioner partitioner,
      String topic
  ) throws HiveMetaStoreException {
    Table table = constructParquetTable(database, tableName, schema, partitioner, topic);
    hiveMetaStore.createTable(table);
  }

  @Override
  public void alterSchema(String database, String tableName, Schema schema) {
    Table table = hiveMetaStore.getTable(database, tableName);
    List<FieldSchema> columns = HiveSchemaConverter.convertSchema(schema);
    removeFieldPartitionColumn(columns, table.getPartitionKeys());
    table.setFields(columns);
    hiveMetaStore.alterTable(table);
  }

  private Table constructParquetTable(
      String database,
      String tableName,
      Schema schema,
      Partitioner partitioner,
      String topic
  ) throws HiveMetaStoreException {
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
    table.setSerializationLib(getHiveParquetSerde());
    try {
      table.setInputFormatClass(getHiveParquetInputFormat());
      table.setOutputFormatClass(getHiveParquetOutputFormat());
    } catch (HiveException e) {
      throw new HiveMetaStoreException("Cannot find input/output format:", e);
    }
    // convert Connect schema schema to Hive columns
    List<FieldSchema> columns = HiveSchemaConverter.convertSchema(schema);
    removeFieldPartitionColumn(columns, partitioner.partitionFields());
    table.setFields(columns);
    table.setPartCols(partitioner.partitionFields());
    return table;
  }

  private String getHiveParquetInputFormat() {
    String newClass = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
    String oldClass = "parquet.hive.DeprecatedParquetInputFormat";

    try {
      Class.forName(newClass);
      return newClass;
    } catch (ClassNotFoundException ex) {
      return oldClass;
    }
  }

  private String getHiveParquetOutputFormat() {
    String newClass = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";
    String oldClass = "parquet.hive.DeprecatedParquetOutputFormat";

    try {
      Class.forName(newClass);
      return newClass;
    } catch (ClassNotFoundException ex) {
      return oldClass;
    }
  }

  private String getHiveParquetSerde() {
    String newClass = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
    String oldClass = "parquet.hive.serde.ParquetHiveSerDe";

    try {
      Class.forName(newClass);
      return newClass;
    } catch (ClassNotFoundException ex) {
      return oldClass;
    }
  }

  /**
   * Remove the column that is later re-created by Hive when using the
   * {@code partition.field.name} config.
   *
   * @param columns the hive columns from
   *                {@link HiveSchemaConverter#convertSchema(Schema) convertSchema}.
   * @param partitionFields the fields used for partitioning
   */
  private void removeFieldPartitionColumn(
      List<FieldSchema> columns,
      List<FieldSchema> partitionFields
  ) {
    Set<String> partitions = partitionFields.stream()
        .map(FieldSchema::getName).collect(Collectors.toSet());

    columns.removeIf(column -> partitions.contains(column.getName()));
  }
}
