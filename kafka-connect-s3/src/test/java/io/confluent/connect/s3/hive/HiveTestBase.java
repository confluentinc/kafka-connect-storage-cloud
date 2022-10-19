/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.connect.s3.hive;

import com.amazonaws.services.s3.AmazonS3;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.S3SinkTask;
import io.confluent.connect.s3.TestWithMockedS3;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.common.util.StringUtils;
import io.confluent.connect.storage.format.Format;
import org.junit.After;

import java.util.Map;

import io.confluent.connect.storage.hive.HiveConfig;
import io.confluent.connect.storage.hive.HiveMetaStore;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.Partitioner;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.powermock.api.mockito.PowerMockito;

public class HiveTestBase<
    FORMAT extends Format<S3SinkConnectorConfig, String>
  > extends TestWithMockedS3 {

  protected S3Storage storage;
  protected AvroData avroData;
  protected AmazonS3 s3;
  protected Partitioner<?> partitioner;
  protected FORMAT format;
  private final Class<FORMAT> clazz;

  protected String hiveDatabase;
  protected HiveMetaStore hiveMetaStore;
  protected HiveExec hiveExec;

  @Rule
  public TemporaryFolder hiveMetaStoreWarehouseDir = new TemporaryFolder();


  /**
   * Constructor
   *
   * @param clazz  A Class<Format type> object for the purpose of creating
   *               a new format object.
   */
  protected HiveTestBase(final Class<FORMAT> clazz) {
    this.clazz = clazz;
  }

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.put(S3SinkConnectorConfig.HIVE_S3_PROTOCOL_CONFIG, "s3a");
    props.put(S3SinkConnectorConfig.S3_PATH_STYLE_ACCESS_ENABLED_CONFIG, "true");
    props.put(StorageCommonConfig.DIRECTORY_DELIM_CONFIG, "/");
    props.put(StorageCommonConfig.FILE_DELIM_CONFIG, "-");
    return props;
  }

  //@Before should be omitted in order to be able to add properties per test.
  public void setUp() throws Exception {
    super.setUp();
    s3 = PowerMockito.spy(newS3Client(connectorConfig));
    storage = new S3Storage(connectorConfig, url, S3_TEST_BUCKET_NAME, s3);

    avroData = new AvroData(connectorConfig.avroDataConfig());

    partitioner = new DefaultPartitioner<>();
    partitioner.configure(parsedConfig);
    format = clazz.getDeclaredConstructor(S3Storage.class).newInstance(storage);
    assertEquals(format.getClass().getName(), clazz.getName());

    s3.createBucket(S3_TEST_BUCKET_NAME);
    assertTrue(s3.doesBucketExistV2(S3_TEST_BUCKET_NAME));

    hiveDatabase = connectorConfig.getString(HiveConfig.HIVE_DATABASE_CONFIG);

    Configuration hadoopConf = new Configuration();
    hadoopConf.set("fs.s3a.endpoint", url);
    hadoopConf.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider");
    hadoopConf.set("fs.s3a.path.style.access", "true");
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false");
    hadoopConf.set("fs.s3a.change.detection.version.required", "false");
    hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

    HiveConf hiveConf = new HiveConf(hadoopConf, HiveConf.class);

    hiveConf.set("hive.metastore.schema.verification", "false");
    hiveConf.set("datanucleus.schema.autoCreateAll", "true");

    hiveConf.set(
      "hive.metastore.warehouse.dir",
      hiveMetaStoreWarehouseDir.newFolder(
        "hive-tests-metastore-warehouse-" +
        UUID.randomUUID().toString()
      ).getPath()
    );

    hiveMetaStore = new HiveMetaStore(hiveConf, connectorConfig);
    HiveConf hiveExecConf = new HiveConf(hiveConf);
    hiveExec = new HiveExec(hiveExecConf);
  }

  @After
  public void tearDown() throws Exception {
    cleanHive();
    super.tearDown();
  }

  private void cleanHive() {
    if (hiveMetaStore == null) {
      return;
    }
    // ensures all tables are removed
    for (String database : hiveMetaStore.getAllDatabases()) {
      for (String table : hiveMetaStore.getAllTables(database)) {
        hiveMetaStore.dropTable(database, table);
      }
      if (!"default".equals(database)) {
        hiveMetaStore.dropDatabase(database, false);
      }
    }
  }

  /**
   * Return a list of new records starting at zero offset.
   *
   * @param size the number of records to return.
   * @return the list of records.
   */
  protected List<SinkRecord> createRecords(int size) {
    return createRecords(size, 0);
  }

  /**
   * Return a list of new records starting at the given offset.
   *
   * @param size the number of records to return.
   * @param startOffset the starting offset.
   * @return the list of records.
   */
  protected List<SinkRecord> createRecords(int size, long startOffset) {
    return createRecords(size, startOffset, Collections.singleton(new TopicPartition(TOPIC, PARTITION)));
  }

  protected List<SinkRecord> createRecords(int size, long startOffset, Set<TopicPartition> partitions) {
    String key = "key";
    Schema schema = createSchema();
    Struct record = createRecord(schema);

    List<SinkRecord> sinkRecords = new ArrayList<>();
    for (TopicPartition tp : partitions) {
      for (long offset = startOffset; offset < startOffset + size; ++offset) {
        sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), Schema.STRING_SCHEMA, key, schema, record, offset));
      }
    }
    return sinkRecords;
  }

  protected Struct createRecord(Schema schema, int ibase, float fbase) {
    return new Struct(schema)
        .put("boolean", true)
        .put("int", ibase)
        .put("long", (long) ibase)
        .put("float", fbase)
        .put("double", (double) fbase);
  }

  // Create a batch of records with incremental numeric field values. Total number of records is
  // given by 'size'.
  protected List<Struct> createRecordBatch(Schema schema, int size) {
    ArrayList<Struct> records = new ArrayList<>(size);
    int ibase = 16;
    float fbase = 12.2f;

    for (int i = 0; i < size; ++i) {
      records.add(createRecord(schema, ibase + i, fbase + i));
    }
    return records;
  }

  // Create a list of records by repeating the same record batch. Total number of records: 'batchesNum' x 'batchSize'
  protected List<Struct> createRecordBatches(Schema schema, int batchSize, int batchesNum) {
    ArrayList<Struct> records = new ArrayList<>();
    for (int i = 0; i < batchesNum; ++i) {
      records.addAll(createRecordBatch(schema, batchSize));
    }
    return records;
  }

  protected List<SinkRecord> createSinkRecords(List<Struct> records, Schema schema) {
    return createSinkRecords(records, schema, 0, Collections.singleton(new TopicPartition(TOPIC, PARTITION)));
  }

  protected List<SinkRecord> createSinkRecords(List<Struct> records, Schema schema, long startOffset,
                                               Set<TopicPartition> partitions) {
    String key = "key";
    List<SinkRecord> sinkRecords = new ArrayList<>();
    for (TopicPartition tp : partitions) {
      long offset = startOffset;
      for (Struct record : records) {
        sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), Schema.STRING_SCHEMA, key, schema, record, offset++));
      }
    }
    return sinkRecords;
  }

  protected String s3aFileKey(
      String keyPrefix,
      String name) {

    StringJoiner sj = new StringJoiner("/")
      .add(connectorConfig.getHiveS3Protocol() + ":/")
      .add(connectorConfig.getBucketName());

    if (StringUtils.isNotBlank(topicsDir)) {
      sj.add(topicsDir);
    }

    sj.add(keyPrefix)
      .add(name);

    return sj.toString();
  }

  protected void writeToS3(final List<SinkRecord> sinkRecords, final Partitioner partitioner) throws Exception {
    S3SinkTask task = new S3SinkTask(
      connectorConfig,
      context,
      storage,
      partitioner,
      format,
      new HiveMetaStoreUpdaterImpl(connectorConfig, format),
      SYSTEM_TIME
    );
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();
  }

}
