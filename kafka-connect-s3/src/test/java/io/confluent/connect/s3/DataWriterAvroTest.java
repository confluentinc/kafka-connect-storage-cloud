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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaProjector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.confluent.connect.s3.format.avro.AvroUtils;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.s3.util.FileUtils;
import io.confluent.connect.storage.hive.HiveConfig;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.Partitioner;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class DataWriterAvroTest extends TestWithMockedS3 {

  private static final String ZERO_PAD_FMT = "%010d";

  private final String extension = ".avro";
  protected S3Storage storage;
  protected AmazonS3 s3;
  Partitioner<FieldSchema> partitioner;
  S3SinkTask task;
  Map<String, String> localProps = new HashMap<>();

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.putAll(localProps);
    return props;
  }

  //@Before should be ommitted in order to be able to add properties per test.
  public void setUp() throws Exception {
    super.setUp();

    s3 = newS3Client(connectorConfig);

    storage = new S3Storage(connectorConfig, url, S3_TEST_BUCKET_NAME, s3);

    partitioner = new DefaultPartitioner<>();
    partitioner.configure(parsedConfig);
    s3.createBucket(S3_TEST_BUCKET_NAME);
    assertTrue(s3.doesBucketExist(S3_TEST_BUCKET_NAME));
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    localProps.clear();
  }

  @Test
  public void testWriteRecords() throws Exception {
    setUp();
    task = new S3SinkTask(connectorConfig, context, storage, partitioner, avroData);

    List<SinkRecord> sinkRecords = createRecords(7);
    // Perform write
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long[] validOffsets = {0, 3, 6};
    verify(sinkRecords, validOffsets);
  }


  @Test
  public void testRecoveryWithPartialFile() throws Exception {
    setUp();

    // Upload partial file.
    List<SinkRecord> sinkRecords = createRecords(2);
    byte[] partialData = AvroUtils.putRecords(sinkRecords, avroData);
    String fileKey = FileUtils.fileKeyToCommit(topicsDir, getDirectory(), TOPIC_PARTITION, 0, extension, ZERO_PAD_FMT);
    s3.putObject(S3_TEST_BUCKET_NAME, fileKey, new ByteArrayInputStream(partialData), null);

    // Accumulate rest of the records.
    sinkRecords.addAll(createRecords(5, 2));

    task = new S3SinkTask(connectorConfig, context, storage, partitioner, avroData);
    // Perform write
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long[] validOffsets = {0, 3, 6};
    verify(sinkRecords, validOffsets);
  }

  @Test
  public void testWriteRecordsSpanningMultipleParts() throws Exception {
    localProps.put(S3SinkConnectorConfig.FLUSH_SIZE_CONFIG, "10000");
    setUp();

    task = new S3SinkTask(connectorConfig, context, storage, partitioner, avroData);

    List<SinkRecord> sinkRecords = createRecords(11000);

    // Perform write
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long[] validOffsets = {0, 10000};
    verify(sinkRecords, validOffsets);
  }

  @Test
  public void testWriteRecordsInMultiplePartitions() throws Exception {
    setUp();
    task = new S3SinkTask(connectorConfig, context, storage, partitioner, avroData);

    List<SinkRecord> sinkRecords = createRecords(7, 0, context.assignment());
    // Perform write
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long[] validOffsets = {0, 3, 6};
    verify(sinkRecords, validOffsets, context.assignment());
  }

  @Test
  public void testWriteInterleavedRecordsInMultiplePartitions() throws Exception {
    setUp();
    task = new S3SinkTask(connectorConfig, context, storage, partitioner, avroData);

    List<SinkRecord> sinkRecords = createRecordsInterleaved(7 * context.assignment().size(), 0, context.assignment());
    // Perform write
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long[] validOffsets = {0, 3, 6};
    verify(sinkRecords, validOffsets, context.assignment());
  }

  @Test
  public void testWriteInterleavedRecordsInMultiplePartitionsNonZeroInitialOffset() throws Exception {
    setUp();
    task = new S3SinkTask(connectorConfig, context, storage, partitioner, avroData);

    List<SinkRecord> sinkRecords = createRecordsInterleaved(7 * context.assignment().size(), 9, context.assignment());
    // Perform write
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long[] validOffsets = {9, 12, 15};
    verify(sinkRecords, validOffsets, context.assignment());
  }

  @Test
  public void testRebalance() throws Exception {
    setUp();
  }

  @Test
  public void testProjectBackward() throws Exception {
    localProps.put(S3SinkConnectorConfig.FLUSH_SIZE_CONFIG, "2");
    localProps.put(HiveConfig.SCHEMA_COMPATIBILITY_CONFIG, "BACKWARD");
    setUp();

    task = new S3SinkTask(connectorConfig, context, storage, partitioner, avroData);
    List<SinkRecord> sinkRecords = createRecordsWithAlteringSchemas(7, 0);

    // Perform write
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long[] validOffsets = {0, 1, 3, 5, 7};
    verify(sinkRecords, validOffsets);
  }

  @Test
  public void testProjectNone() throws Exception {
    localProps.put(S3SinkConnectorConfig.FLUSH_SIZE_CONFIG, "2");
    setUp();

    task = new S3SinkTask(connectorConfig, context, storage, partitioner, avroData);
    List<SinkRecord> sinkRecords = createRecordsWithAlteringSchemas(7, 0);

    // Perform write
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long[] validOffsets = {0, 1, 2, 3, 4, 5, 6};
    verify(sinkRecords, validOffsets);
  }

  @Test
  public void testProjectForward() throws Exception {
    localProps.put(S3SinkConnectorConfig.FLUSH_SIZE_CONFIG, "2");
    localProps.put(HiveConfig.SCHEMA_COMPATIBILITY_CONFIG, "FORWARD");
    setUp();

    task = new S3SinkTask(connectorConfig, context, storage, partitioner, avroData);
    // By excluding the first element we get a list starting with record having the new schema.
    List<SinkRecord> sinkRecords = createRecordsWithAlteringSchemas(8, 0).subList(1, 8);

    // Perform write
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long[] validOffsets = {1, 2, 4, 6, 8};
    verify(sinkRecords, validOffsets);
  }

  @Test
  public void testProjectNoVersion() throws Exception {
    setUp();
  }

  @Test
  public void testFlushPartialFile() throws Exception {
    setUp();
  }

  /**
   * Return a list of new records starting at zero offset.
   *
   * @param size the number of records to return.
   * @return
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

  protected List<SinkRecord> createRecordsWithAlteringSchemas(int size, long startOffset) {
    String key = "key";
    Schema schema = createSchema();
    Struct record = createRecord(schema);
    Schema newSchema = createNewSchema();
    Struct newRecord = createNewRecord(newSchema);

    int limit = (size / 2) * 2;
    boolean remainder = size % 2 > 0;
    List<SinkRecord> sinkRecords = new ArrayList<>();
    for (long offset = startOffset; offset < startOffset + limit; ++offset) {
      sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, offset));
      sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, newSchema, newRecord, ++offset));
    }
    if (remainder) {
      sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record,
                                     startOffset + size - 1));
    }
    return sinkRecords;
  }

  protected List<SinkRecord> createRecordsInterleaved(int size, long startOffset, Set<TopicPartition> partitions) {
    String key = "key";
    Schema schema = createSchema();
    Struct record = createRecord(schema);

    List<SinkRecord> sinkRecords = new ArrayList<>();
    for (long offset = startOffset, total = 0; total < size; ++offset) {
      for (TopicPartition tp : partitions) {
        sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), Schema.STRING_SCHEMA, key, schema, record, offset));
        if (++total >= size) {
          break;
        }
      }
    }
    return sinkRecords;
  }

  protected String getDirectory() {
    return getDirectory(TOPIC, PARTITION);
  }

  protected String getDirectory(String topic, int partition) {
    String encodedPartition = "partition=" + String.valueOf(partition);
    return partitioner.generatePartitionedPath(topic, encodedPartition);
  }

  protected void verifyFileListing(long[] validOffsets, Set<TopicPartition> partitions)
      throws IOException {
    List<String> expectedFiles = new ArrayList<>();
    for (TopicPartition tp : partitions) {
      for (int i = 1, j = 0; i < validOffsets.length; ++i) {
        long startOffset = validOffsets[i - 1];
        expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, getDirectory(tp.topic(), tp.partition()), tp,
                                                    startOffset, extension, ZERO_PAD_FMT));
      }
    }

    List<S3ObjectSummary> summaries = listObjects(S3_TEST_BUCKET_NAME, null, s3);
    List<String> actualFiles = new ArrayList<>();
    for (S3ObjectSummary summary : summaries) {
      String fileKey = summary.getKey();
      actualFiles.add(fileKey);
    }

    Collections.sort(actualFiles);
    Collections.sort(expectedFiles);
    assertThat(actualFiles, is(expectedFiles));
  }

  protected void verifyContents(List<SinkRecord> expectedRecords, int startIndex, Collection<Object> records) {
    Schema expectedSchema = null;
    for (Object avroRecord : records) {
      if (expectedSchema == null) {
        expectedSchema = expectedRecords.get(startIndex).valueSchema();
      }
      Object expectedValue = SchemaProjector.project(expectedRecords.get(startIndex).valueSchema(),
                                                     expectedRecords.get(startIndex++).value(),
                                                     expectedSchema);
      assertEquals(avroData.fromConnectData(expectedSchema, expectedValue), avroRecord);
    }
  }

  protected void verify(List<SinkRecord> sinkRecords, long[] validOffsets) throws IOException {
    verify(sinkRecords, validOffsets, Collections.singleton(new TopicPartition(TOPIC, PARTITION)));
  }

  /**
   * Verify files and records are uploaded appropriately.
   * @param sinkRecords a flat list of the records that need to appear in potentially several files in S3.
   * @param validOffsets an array containing the offsets that map to uploaded files for a topic-partition.
   *                     Offsets appear in ascending order, the difference between two consecutive offsets
   *                     equals the expected size of the file, and last offset in exclusive.
   * @throws IOException
   */
  protected void verify(List<SinkRecord> sinkRecords, long[] validOffsets, Set<TopicPartition> partitions)
      throws IOException {
    verifyFileListing(validOffsets, partitions);
    for (TopicPartition tp : partitions) {
      for (int i = 1, j = 0; i < validOffsets.length; ++i) {
        long startOffset = validOffsets[i - 1];
        long size = validOffsets[i] - startOffset;

        FileUtils.fileKeyToCommit(topicsDir, getDirectory(tp.topic(), tp.partition()), tp, startOffset, extension, ZERO_PAD_FMT);
        Collection<Object> records = readRecords(topicsDir, getDirectory(tp.topic(), tp.partition()), tp, startOffset,
                                                 extension, ZERO_PAD_FMT, S3_TEST_BUCKET_NAME, s3);
        assertEquals(size, records.size());
        verifyContents(sinkRecords, j, records);
        j += size;
      }
    }
  }
}

