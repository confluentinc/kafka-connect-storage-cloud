/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/

package io.confluent.connect.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import io.confluent.connect.s3.format.avro.AvroFormat;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.s3.storage.S3StorageConfig;
import io.confluent.connect.s3.util.FileUtils;
import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.FieldPartitioner;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class TopicPartitionWriterTest extends TestWithMockedS3 {
  // The default based on default configuration of 10
  private static final String ZERO_PAD_FMT = "%010d";

  private RecordWriterProvider<S3StorageConfig> writerProvider;
  private S3Storage storage;
  private static String extension;

  private S3StorageConfig storageConfig;
  private AWSCredentials credentials;
  private AmazonS3Client s3;
  Map<String, String> localProps = new HashMap<>();

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.putAll(localProps);
    return props;
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    credentials = new AnonymousAWSCredentials();
    storageConfig = new S3StorageConfig(connectorConfig, credentials);

    s3 = new AmazonS3Client(storageConfig.provider(), storageConfig.clientConfig(), storageConfig.collector());
    s3.setEndpoint(S3_TEST_URL);

    storage = new S3Storage(storageConfig, url, S3_TEST_BUCKET_NAME, s3);

    Format<S3StorageConfig, String> format = new AvroFormat(storage, avroData);
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
  public void testFirstThingsFirst() throws Exception {
    s3.createBucket(S3_TEST_BUCKET_NAME);
    assertTrue(s3.doesBucketExist(S3_TEST_BUCKET_NAME));
  }

  @Test
  public void testWriteRecordDefaultWithPadding() throws Exception {
    tearDown();
    localProps.put(S3SinkConnectorConfig.FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG, "2");
    setUp();
    Partitioner partitioner = new DefaultPartitioner();
    partitioner.configure(Collections.<String, Object>emptyMap());
    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION, storage, writerProvider, partitioner,  connectorConfig, context, avroData);

    String key = "key";
    Schema schema = createSchema();
    Struct[] records = createRecords(schema);

    Collection<SinkRecord> sinkRecords = createSinkRecords(records, key, schema);

    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    topicPartitionWriter.write();
    topicPartitionWriter.close();

    String dirPrefix = partitioner.generatePartitionedPath(TOPIC, "partition=" + PARTITION);
    Set<String> expectedFiles = new HashSet<>();
    expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 0, extension, "%02d"));
    expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 3, extension, "%02d"));
    expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 6, extension, "%02d"));
    verify(expectedFiles, records, schema);
  }

  public String getExpectedPath(String bucket, String topic, int partition, String offset) {
    return s3.getEndpointPrefix() + "://" + bucket + "/" + DM + topic + DM + "partition=" + partition + DM + topic
               + FM + partition + FM + offset + extension;
  }

  @Test
  public void testWriteRecordFieldPartitioner() throws Exception {
    Map<String, Object> config = createConfig();
    Partitioner partitioner = new FieldPartitioner();
    partitioner.configure(config);

    String partitionField = (String) config.get(S3SinkConnectorConfig.PARTITION_FIELD_NAME_CONFIG);

    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION, storage, writerProvider, partitioner, connectorConfig, context, avroData);

    String key = "key";
    Schema schema = createSchema();
    Struct[] records = createRecords(schema);

    Collection<SinkRecord> sinkRecords = createSinkRecords(records, key, schema);

    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    topicPartitionWriter.write();
    topicPartitionWriter.close();


    String dirPrefix1 = partitioner.generatePartitionedPath(TOPIC, partitionField + "=" + String.valueOf(16));
    String dirPrefix2 = partitioner.generatePartitionedPath(TOPIC, partitionField + "=" + String.valueOf(17));
    String dirPrefix3 = partitioner.generatePartitionedPath(TOPIC, partitionField + "=" + String.valueOf(18));

    Set<String> expectedFiles = new HashSet<>();
    expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix1, TOPIC_PARTITION, 0, extension, ZERO_PAD_FMT));
    expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix2, TOPIC_PARTITION, 3, extension, ZERO_PAD_FMT));
    expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix3, TOPIC_PARTITION, 6, extension, ZERO_PAD_FMT));

    verify(expectedFiles, records, schema);
    verify(expectedFiles, records, schema);
  }

  @Test
  public void testWriteRecordTimeBasedPartition() throws Exception {
    Map<String, Object> config = createConfig();
    Partitioner partitioner = new TimeBasedPartitioner();
    partitioner.configure(config);

    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION, storage, writerProvider, partitioner, connectorConfig, context, avroData);

    String key = "key";
    Schema schema = createSchema();
    Struct[] records = createRecords(schema);

    Collection<SinkRecord> sinkRecords = createSinkRecords(records, key, schema);

    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    topicPartitionWriter.write();
    topicPartitionWriter.close();


    long partitionDurationMs = (Long) config.get(S3SinkConnectorConfig.PARTITION_DURATION_MS_CONFIG);
    String pathFormat = (String) config.get(S3SinkConnectorConfig.PATH_FORMAT_CONFIG);
    String timeZoneString = (String) config.get(S3SinkConnectorConfig.TIMEZONE_CONFIG);
    long timestamp = System.currentTimeMillis();

    String encodedPartition = TimeUtils.encodeTimestamp(partitionDurationMs, pathFormat, timeZoneString, timestamp);

    String dirPrefix = partitioner.generatePartitionedPath(TOPIC, encodedPartition);

    Set<String> expectedFiles = new HashSet<>();
    expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 0, extension, ZERO_PAD_FMT));
    expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 3, extension, ZERO_PAD_FMT));
    expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 6, extension, ZERO_PAD_FMT));

    verify(expectedFiles, records, schema);
  }

  private Map<String, Object> createConfig() {
    Map<String, Object> config = new HashMap<>();
    config.put(S3SinkConnectorConfig.PARTITION_FIELD_NAME_CONFIG, "int");
    config.put(S3SinkConnectorConfig.PARTITION_DURATION_MS_CONFIG, TimeUnit.HOURS.toMillis(1));
    config.put(S3SinkConnectorConfig.PATH_FORMAT_CONFIG, "'year'=YYYY_'month'=MM_'day'=dd_'hour'=HH_");
    config.put(S3SinkConnectorConfig.LOCALE_CONFIG, "en");
    config.put(S3SinkConnectorConfig.TIMEZONE_CONFIG, "America/Los_Angeles");
    return config;
  }

  private Struct[] createRecords(Schema schema) {
    Struct record1 = new Struct(schema)
        .put("boolean", true)
        .put("int", 16)
        .put("long", 12L)
        .put("float", 12.2f)
        .put("double", 12.2);

    Struct record2 = new Struct(schema)
        .put("boolean", true)
        .put("int", 17)
        .put("long", 12L)
        .put("float", 12.2f)
        .put("double", 12.2);

    Struct record3 = new Struct(schema)
        .put("boolean", true)
        .put("int", 18)
        .put("long", 12L)
        .put("float", 12.2f)
        .put("double", 12.2);

    ArrayList<Struct> records = new ArrayList<>();
    records.add(record1);
    records.add(record2);
    records.add(record3);
    return records.toArray(new Struct[records.size()]);
  }


  private ArrayList<SinkRecord> createSinkRecords(Struct[] records, String key, Schema schema) {
    ArrayList<SinkRecord> sinkRecords = new ArrayList<>();
    long offset = 0;
    for (Struct record : records) {
      for (long count = 0; count < 3; count++) {
        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record,
                                               offset + count);
        sinkRecords.add(sinkRecord);
      }
      offset = offset + 3;
    }
    return sinkRecords;
  }

  private void verify(Set<String> expectedKeys, Struct[] records, Schema schema) throws IOException {
    List<S3ObjectSummary> summaries = listObjects(S3_TEST_BUCKET_NAME, null);
    int index = 0;
    for (S3ObjectSummary summary : summaries) {
      String bucket = summary.getBucketName();
      String endpoint = s3.getEndpointPrefix();
      String key = summary.getKey();
      System.out.println("Object bucket: " + summary.getBucketName());
      System.out.println("Object name: " + summary.getKey());
      String fullKey = key;
      System.out.println("Full : " + fullKey);
      assertTrue(expectedKeys.contains(fullKey));
      // s3.getObject(S3_TEST_BUCKET_NAME, key);
      //Collection<Object> avroRecords = schemaFileReader.readData(conf, filePath);
      System.out.println("Object size for now: " + summary.getSize());
      /*
      assertEquals(3, avroRecords.size());
      for (Object avroRecord: avroRecords) {
        assertEquals(avroData.fromConnectData(schema, records[index]), avroRecord);
      }
      */
      index++;
    }
    assertEquals(expectedKeys.size(), summaries.size());
  }

  private List<S3ObjectSummary> listObjects(String bucket, String prefix) {
    List<S3ObjectSummary> objects = new ArrayList<>();
    ObjectListing listing;
    if (prefix == null) {
      listing = s3.listObjects(bucket);
    } else {
      listing = s3.listObjects(bucket, prefix);
    }
    objects.addAll(listing.getObjectSummaries());
    while (listing.isTruncated()) {
      listing = s3.listNextBatchOfObjects(listing);
      objects.addAll(listing.getObjectSummaries());
    }

    return objects;
  }
}
