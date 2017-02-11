/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.connect.s3.format.avro.AvroFormat;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.s3.util.FileUtils;
import io.confluent.connect.s3.util.TimeUtils;
import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.hive.schema.TimeBasedSchemaGenerator;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.FieldPartitioner;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class TopicPartitionWriterTest extends TestWithMockedS3 {
  // The default
  private static final String ZERO_PAD_FMT = "%010d";

  private RecordWriterProvider<S3SinkConnectorConfig> writerProvider;
  private S3Storage storage;
  private static String extension;

  private AmazonS3 s3;
  Map<String, String> localProps = new HashMap<>();

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.putAll(localProps);
    return props;
  }

  public void setUp() throws Exception {
    super.setUp();

    s3 = newS3Client(connectorConfig);
    storage = new S3Storage(connectorConfig, url, S3_TEST_BUCKET_NAME, s3);

    Format<S3SinkConnectorConfig, String> format = new AvroFormat(storage, avroData);
    writerProvider = format.getRecordWriterProvider();
    extension = writerProvider.getExtension();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    localProps.clear();
  }

  @Test
  public void testWriteRecordDefaultWithPaddingSeq() throws Exception {
    localProps.put(S3SinkConnectorConfig.FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG, "2");
    setUp();

    // Define the partitioner
    Partitioner<FieldSchema> partitioner = new DefaultPartitioner<>();
    partitioner.configure(parsedConfig);
    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION, storage, writerProvider, partitioner,  connectorConfig, context);

    String key = "key";
    Schema schema = createSchema();
    List<Struct> records = createRecordBatch(schema, 10);

    Collection<SinkRecord> sinkRecords = createSinkRecords(records, key, schema);

    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    // Test actual write
    topicPartitionWriter.write();
    topicPartitionWriter.close();

    String dirPrefix = partitioner.generatePartitionedPath(TOPIC, "partition=" + PARTITION);
    List<String> expectedFiles = new ArrayList<>();
    expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 0, extension, "%02d"));
    expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 3, extension, "%02d"));
    expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 6, extension, "%02d"));
    verify(expectedFiles, 3, schema, records);
  }

  @Test
  public void testWriteRecordDefaultWithPadding() throws Exception {
    localProps.put(S3SinkConnectorConfig.FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG, "2");
    setUp();

    // Define the partitioner
    Partitioner<FieldSchema> partitioner = new DefaultPartitioner<>();
    partitioner.configure(parsedConfig);
    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION, storage, writerProvider, partitioner,  connectorConfig, context);

    String key = "key";
    Schema schema = createSchema();
    List<Struct> records = createRecordBatches(schema, 3, 3);

    Collection<SinkRecord> sinkRecords = createSinkRecords(records, key, schema);

    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    // Test actual write
    topicPartitionWriter.write();
    topicPartitionWriter.close();

    String dirPrefix = partitioner.generatePartitionedPath(TOPIC, "partition=" + PARTITION);
    List<String> expectedFiles = new ArrayList<>();
    expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 0, extension, "%02d"));
    expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 3, extension, "%02d"));
    expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 6, extension, "%02d"));
    verify(expectedFiles, 3, schema, records);
  }

  @Test
  public void testWriteRecordFieldPartitioner() throws Exception {
    localProps.put(S3SinkConnectorConfig.FLUSH_SIZE_CONFIG, "9");
    setUp();

    // Define the partitioner
    Partitioner<FieldSchema> partitioner = new FieldPartitioner<>();
    partitioner.configure(parsedConfig);

    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION, storage, writerProvider, partitioner, connectorConfig, context);

    String key = "key";
    Schema schema = createSchema();
    List<Struct> records = createRecordBatches(schema, 3, 6);

    Collection<SinkRecord> sinkRecords = createSinkRecords(records, key, schema);

    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    // Test actual write
    topicPartitionWriter.write();
    topicPartitionWriter.close();

    String partitionField = (String) parsedConfig.get(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG);
    String dirPrefix1 = partitioner.generatePartitionedPath(TOPIC, partitionField + "=" + String.valueOf(16));
    String dirPrefix2 = partitioner.generatePartitionedPath(TOPIC, partitionField + "=" + String.valueOf(17));
    String dirPrefix3 = partitioner.generatePartitionedPath(TOPIC, partitionField + "=" + String.valueOf(18));

    List<Struct> expectedRecords = new ArrayList<>();
    long lbase = 16;
    double dbase = 12.2;
    // The expected sequence of records is constructed taking into account that sorting of files occurs in verify
    for (int i = 0; i < 3; ++i) {
      for (int j = 0; j < 6; ++j) {
        expectedRecords.add(createRecord(schema, lbase + i, dbase + i));
      }
    }

    List<String> expectedFiles = new ArrayList<>();
    for(int i = 0; i < 18; i += 9) {
      expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix1, TOPIC_PARTITION, i, extension, ZERO_PAD_FMT));
      expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix2, TOPIC_PARTITION, i + 1, extension, ZERO_PAD_FMT));
      expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix3, TOPIC_PARTITION, i + 2, extension, ZERO_PAD_FMT));
    }

    verify(expectedFiles, 3, schema, expectedRecords);
  }

  @Test
  public void testWriteRecordTimeBasedPartition() throws Exception {
    setUp();

    // Define the partitioner
    Partitioner<FieldSchema> partitioner = new TimeBasedPartitioner<>();
    parsedConfig.put(PartitionerConfig.SCHEMA_GENERATOR_CLASS_CONFIG, TimeBasedSchemaGenerator.class);
    partitioner.configure(parsedConfig);

    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION, storage, writerProvider, partitioner, connectorConfig, context);

    String key = "key";
    Schema schema = createSchema();
    List<Struct> records = createRecordBatches(schema, 3, 3);

    Collection<SinkRecord> sinkRecords = createSinkRecords(records, key, schema);

    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    // Test actual write
    topicPartitionWriter.write();
    topicPartitionWriter.close();

    long partitionDurationMs = (Long) parsedConfig.get(PartitionerConfig.PARTITION_DURATION_MS_CONFIG);
    String pathFormat = (String) parsedConfig.get(PartitionerConfig.PATH_FORMAT_CONFIG);
    String timeZoneString = (String) parsedConfig.get(PartitionerConfig.TIMEZONE_CONFIG);
    long timestamp = System.currentTimeMillis();
    String encodedPartition = TimeUtils.encodeTimestamp(partitionDurationMs, pathFormat, timeZoneString, timestamp);

    String dirPrefix = partitioner.generatePartitionedPath(TOPIC, encodedPartition);
    List<String> expectedFiles = new ArrayList<>();
    for (int i : new int[]{0, 3, 6}) {
      expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, i, extension, ZERO_PAD_FMT));
    }
    verify(expectedFiles, 3, schema, records);
  }

  @Test
  public void testNoFilesWrittenWithoutCommit() throws Exception {
    // Setting size-based rollup to 10 but will produce fewer records. Commit should not happen.
    localProps.put(S3SinkConnectorConfig.FLUSH_SIZE_CONFIG, "10");
    setUp();

    // Define the partitioner
    Partitioner<FieldSchema> partitioner = new DefaultPartitioner<>();
    partitioner.configure(parsedConfig);
    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION, storage, writerProvider, partitioner,  connectorConfig, context);

    String key = "key";
    Schema schema = createSchema();
    List<Struct> records = createRecordBatches(schema, 3, 3);

    Collection<SinkRecord> sinkRecords = createSinkRecords(records, key, schema);

    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    // Test actual write
    topicPartitionWriter.write();
    topicPartitionWriter.close();

    String dirPrefix = partitioner.generatePartitionedPath(TOPIC, "partition=" + PARTITION);
    List<String> expectedFiles = new ArrayList<>();
    expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 0, extension, "%02d"));
    expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 3, extension, "%02d"));
    expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 6, extension, "%02d"));
    // Record size argument does not matter.
    verify(Collections.<String>emptyList(), -1, schema, records);
  }

  // Create a batch of records with incremental numeric field values. Total number of records is given by 'size'.
  private Struct createRecord(Schema schema, long lbase, double dbase) {
    return new Struct(schema)
               .put("boolean", true)
               .put("int", (int) lbase)
               .put("long", lbase)
               .put("float", (float) dbase)
               .put("double", dbase);
  }

  // Create a batch of records with incremental numeric field values. Total number of records is given by 'size'.
  private List<Struct> createRecordBatch(Schema schema, int size) {
    ArrayList<Struct> records = new ArrayList<>(size);
    long lbase = 16;
    double dbase = 12.2;

    for (int i = 0; i < size; ++i) {
      records.add(createRecord(schema, lbase + i, dbase + i));
    }
    return records;
  }

  // Create a list of records by repeating the same record batch. Total number of records: 'batchesNum' x 'batchSize'
  private List<Struct> createRecordBatches(Schema schema, int batchSize, int batchesNum) {
    ArrayList<Struct> records = new ArrayList<>();
    for (int i = 0; i < batchesNum; ++i) {
      records.addAll(createRecordBatch(schema, batchSize));
    }
    return records;
  }

  // Given a list of records, create a list of sink records with contiguous offsets.
  private ArrayList<SinkRecord> createSinkRecords(List<Struct> records, String key, Schema schema) {
    ArrayList<SinkRecord> sinkRecords = new ArrayList<>();
    for (int i = 0; i < records.size(); ++i) {
      sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, records.get(i), i));
    }
    return sinkRecords;
  }

  private void verify(List<String> expectedFileKeys, int expectedSize, Schema schema, List<Struct> records)
      throws IOException {
    List<S3ObjectSummary> summaries = listObjects(S3_TEST_BUCKET_NAME, null, s3);
    List<String> actualFiles = new ArrayList<>();
    for (S3ObjectSummary summary : summaries) {
      String fileKey = summary.getKey();
      actualFiles.add(fileKey);
    }

    Collections.sort(actualFiles);
    Collections.sort(expectedFileKeys);
    assertThat(actualFiles, is(expectedFileKeys));

    int index = 0;
    for (String fileKey : actualFiles) {
      Collection<Object> actualRecords = readRecords(S3_TEST_BUCKET_NAME, fileKey, s3);
      assertEquals(expectedSize, actualRecords.size());
      for (Object avroRecord : actualRecords) {
        Object expectedRecord = avroData.fromConnectData(schema, records.get(index++));
        assertEquals(expectedRecord, avroRecord);
      }
    }
  }

}
