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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
  private S3Storage storage;

  private S3StorageConfig storageConfig;
  private AWSCredentials credentials;
  private AmazonS3Client s3;
  Partitioner<FieldSchema> partitioner;
  S3SinkTask task;
  int flushSize;
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
    partitioner = new DefaultPartitioner<>();
    partitioner.configure(rawConfig);
    s3.createBucket(S3_TEST_BUCKET_NAME);
    assertTrue(s3.doesBucketExist(S3_TEST_BUCKET_NAME));
    task = new S3SinkTask(connectorConfig, context, storage, partitioner, avroData);

    String encodedPartition = "partition=" + String.valueOf(PARTITION);
    String directory = partitioner.generatePartitionedPath(TOPIC, encodedPartition);

    String key = "key";
    Schema schema = createSchema();
    Struct record = createRecord(schema);

    Collection<SinkRecord> sinkRecords = new ArrayList<>();
    for (long offset = 0; offset < 7; ++offset) {
      sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, offset));
    }

    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    flushSize = connectorConfig.getInt(S3SinkConnectorConfig.FLUSH_SIZE_CONFIG);
    // Last file doesn't satisfy size requirement and gets discarded on close
    long[] validOffsets = {0, 3, 6};
    for (int i = 1; i < validOffsets.length; ++i) {
      long startOffset = validOffsets[i - 1];
      String fileKey = FileUtils.fileKeyToCommit(topicsDir, directory, TOPIC_PARTITION, startOffset, extension, ZERO_PAD_FMT);
      long size = validOffsets[i] - startOffset;

      System.out.println(fileKey);
      InputStream in = s3.getObject(S3_TEST_BUCKET_NAME, fileKey).getObjectContent();

      Collection<Object> records = AvroUtils.getRecords(in);

      assertEquals(size, records.size());
      for (Object avroRecord : records) {
        assertEquals(avroData.fromConnectData(schema, record), avroRecord);
      }
    }
  }

  @Test
  public void testRecoveryWithPartialFile() throws Exception {
    setUp();
    partitioner = new DefaultPartitioner<>();
    partitioner.configure(rawConfig);
    s3.createBucket(S3_TEST_BUCKET_NAME);
    assertTrue(s3.doesBucketExist(S3_TEST_BUCKET_NAME));

    String encodedPartition = "partition=" + String.valueOf(PARTITION);
    String directory = partitioner.generatePartitionedPath(TOPIC, encodedPartition);

    String key = "key";
    Schema schema = createSchema();
    Struct record = createRecord(schema);

    Collection<SinkRecord> sinkRecords = new ArrayList<>();
    for (long offset = 0; offset < 2; ++offset) {
      sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, offset));
    }

    byte[] partialData = AvroUtils.putRecords(sinkRecords, avroData);
    String fileKey = FileUtils.fileKeyToCommit(topicsDir, directory, TOPIC_PARTITION, 0, extension, ZERO_PAD_FMT);
    s3.putObject(S3_TEST_BUCKET_NAME, fileKey, new ByteArrayInputStream(partialData), null);

    task = new S3SinkTask(connectorConfig, context, storage, partitioner, avroData);

   sinkRecords.clear();
    for (long offset = 0; offset < 7; ++offset) {
      sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, offset));
    }

    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    flushSize = connectorConfig.getInt(S3SinkConnectorConfig.FLUSH_SIZE_CONFIG);
    // Last file doesn't satisfy size requirement and gets discarded on close
    long[] validOffsets = {0, 3, 6};
    for (int i = 1; i < validOffsets.length; ++i) {
      long startOffset = validOffsets[i - 1];
      fileKey = FileUtils.fileKeyToCommit(topicsDir, directory, TOPIC_PARTITION, startOffset, extension, ZERO_PAD_FMT);
      long size = validOffsets[i] - startOffset;

      System.out.println(fileKey);
      InputStream in = s3.getObject(S3_TEST_BUCKET_NAME, fileKey).getObjectContent();

      Collection<Object> records = AvroUtils.getRecords(in);

      assertEquals(size, records.size());
      for (Object avroRecord : records) {
        assertEquals(avroData.fromConnectData(schema, record), avroRecord);
      }
    }
  }

  @Test
  public void testWriteRecordsSpanningMultipleParts() throws Exception {
    localProps.put(S3SinkConnectorConfig.FLUSH_SIZE_CONFIG, "10000");
    setUp();
    partitioner = new DefaultPartitioner<>();
    partitioner.configure(rawConfig);
    s3.createBucket(S3_TEST_BUCKET_NAME);
    assertTrue(s3.doesBucketExist(S3_TEST_BUCKET_NAME));
    task = new S3SinkTask(connectorConfig, context, storage, partitioner, avroData);

    String encodedPartition = "partition=" + String.valueOf(PARTITION);
    String directory = partitioner.generatePartitionedPath(TOPIC, encodedPartition);

    String key = "key";
    Schema schema = createSchema();
    Struct record = createRecord(schema);

    Collection<SinkRecord> sinkRecords = new ArrayList<>();
    for (long offset = 0; offset < 11000; ++offset) {
      sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, offset));
    }

    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    flushSize = connectorConfig.getInt(S3SinkConnectorConfig.FLUSH_SIZE_CONFIG);
    // Last file doesn't satisfy size requirement and gets discarded on close
    long[] validOffsets = {0, 10000};
    for (int i = 1; i < validOffsets.length; ++i) {
      long startOffset = validOffsets[i - 1];
      String fileKey = FileUtils.fileKeyToCommit(topicsDir, directory, TOPIC_PARTITION, startOffset, extension, ZERO_PAD_FMT);
      long size = validOffsets[i] - startOffset;

      System.out.println(fileKey);
      InputStream in = s3.getObject(S3_TEST_BUCKET_NAME, fileKey).getObjectContent();

      Collection<Object> records = AvroUtils.getRecords(in);

      assertEquals(size, records.size());
      for (Object avroRecord : records) {
        assertEquals(avroData.fromConnectData(schema, record), avroRecord);
      }
    }
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
  public void testWriteRecordNonZeroInitailOffset() throws Exception {
    setUp();
  }

  @Test
  public void testRebalance() throws Exception {
    setUp();
  }

  @Test
  public void testProjectBackWard() throws Exception {
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

}

