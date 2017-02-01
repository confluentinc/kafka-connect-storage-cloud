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
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.connect.s3.format.avro.AvroUtils;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.s3.storage.S3StorageConfig;
import io.confluent.connect.s3.util.FileUtils;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.Partitioner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DataWriterAvroTest extends TestWithMockedS3 {

  private static final String ZERO_PAD_FMT = "%010d";

  private final String extension = ".avro";
  private AWSCredentials credentials;
  protected S3StorageConfig storageConfig;
  protected S3Storage storage;
  protected AmazonS3Client s3;
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
    credentials = new AnonymousAWSCredentials();
    storageConfig = new S3StorageConfig(connectorConfig, credentials);

    s3 = new AmazonS3Client(storageConfig.provider(), storageConfig.clientConfig(), storageConfig.collector());
    s3.setEndpoint(S3_TEST_URL);

    storage = new S3Storage(storageConfig, url, S3_TEST_BUCKET_NAME, s3);

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
  public void testWriteRecord() throws Exception {
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
  public void testWriteRecordMultiplePartitions() throws Exception {
    setUp();
  }

  @Test
  public void testGetPreviousOffsets() throws Exception {
    setUp();
  }

  @Test
  public void testWriteRecordNonZeroInitialOffset() throws Exception {
    setUp();
  }

  @Test
  public void testRebalance() throws Exception {
    setUp();
  }

  @Test
  public void testProjectBackward() throws Exception {
    setUp();
  }

  @Test
  public void testProjectNone() throws Exception {
    setUp();
  }

  @Test
  public void testProjectForward() throws Exception {
    setUp();
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
    String key = "key";
    Schema schema = createSchema();
    Struct record = createRecord(schema);

    List<SinkRecord> sinkRecords = new ArrayList<>();
    for (long offset = startOffset; offset < startOffset + size; ++offset) {
      sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, offset));
    }
    return sinkRecords;
  }

  protected String getDirectory() {
    String encodedPartition = "partition=" + String.valueOf(PARTITION);
    return partitioner.generatePartitionedPath(TOPIC, encodedPartition);
  }

  protected void verifyContents(List<SinkRecord> expectedRecords, Collection<Object> records) {
    int i = 0;
    for (Object avroRecord : records) {
      Schema schema = expectedRecords.get(i).valueSchema();
      Struct record = (Struct) expectedRecords.get(i++).value();
      assertEquals(avroData.fromConnectData(schema, record), avroRecord);
    }
  }

  protected void verify(List<SinkRecord> sinkRecords, long[] validOffsets) throws IOException {
    // Last file doesn't satisfy size requirement and gets discarded on close
    for (int i = 1; i < validOffsets.length; ++i) {
      long startOffset = validOffsets[i - 1];
      long size = validOffsets[i] - startOffset;

      Collection<Object> records = readRecords(topicsDir, getDirectory(), TOPIC_PARTITION, startOffset, extension,
                                               ZERO_PAD_FMT, S3_TEST_BUCKET_NAME, s3);
      assertEquals(size, records.size());
      verifyContents(sinkRecords, records);

    }
  }
}

