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

package io.confluent.connect.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.Tag;
import io.confluent.common.utils.SystemTime;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.confluent.common.utils.MockTime;
import io.confluent.common.utils.SystemTime;
import io.confluent.common.utils.Time;
import io.confluent.connect.s3.format.avro.AvroFormat;
import io.confluent.connect.s3.storage.S3OutputStream;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.s3.util.FileUtils;
import io.confluent.connect.s3.util.TimeUtils;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.partitioner.DailyPartitioner;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.FieldPartitioner;
import io.confluent.connect.storage.partitioner.HourlyPartitioner;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;
import io.confluent.connect.storage.partitioner.TimestampExtractor;

import static org.apache.kafka.common.utils.Time.SYSTEM;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TopicPartitionWriterTest extends TestWithMockedS3 {
  // The default
  private static final String ZERO_PAD_FMT = "%010d";

  private RecordWriterProvider<S3SinkConnectorConfig> writerProvider;
  private S3Storage storage;
  private AvroFormat format;
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
    format = new AvroFormat(storage);

    s3.createBucket(S3_TEST_BUCKET_NAME);
    assertTrue(s3.doesBucketExist(S3_TEST_BUCKET_NAME));

    Format<S3SinkConnectorConfig, String> format = new AvroFormat(storage);
    writerProvider = format.getRecordWriterProvider();
    extension = writerProvider.getExtension();
  }

  public void setUpWithCommitException() throws Exception {
    super.setUp();

    s3 = newS3Client(connectorConfig);
    storage = new S3Storage(connectorConfig, url, S3_TEST_BUCKET_NAME, s3) {
      private final AtomicInteger retries = new AtomicInteger(0);

      @Override
      public S3OutputStream create(String path, boolean overwrite) {
        return new S3OutputStreamFlaky(path, this.conf(), s3, retries);
      }
    };

    format = new AvroFormat(storage);

    Format<S3SinkConnectorConfig, String> format = new AvroFormat(storage);
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
    Partitioner<?> partitioner = new DefaultPartitioner<>();
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
    Partitioner<?> partitioner = new DefaultPartitioner<>();
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
    Partitioner<?> partitioner = new FieldPartitioner<>();
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

    @SuppressWarnings("unchecked")
    List<String> partitionFields = (List<String>) parsedConfig.get(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG);
    String partitionField = partitionFields.get(0);
    String dirPrefix1 = partitioner.generatePartitionedPath(TOPIC, partitionField + "=" + String.valueOf(16));
    String dirPrefix2 = partitioner.generatePartitionedPath(TOPIC, partitionField + "=" + String.valueOf(17));
    String dirPrefix3 = partitioner.generatePartitionedPath(TOPIC, partitionField + "=" + String.valueOf(18));

    List<Struct> expectedRecords = new ArrayList<>();
    int ibase = 16;
    float fbase = 12.2f;
    // The expected sequence of records is constructed taking into account that sorting of files occurs in verify
    for (int i = 0; i < 3; ++i) {
      for (int j = 0; j < 6; ++j) {
        expectedRecords.add(createRecord(schema, ibase + i, fbase + i));
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
  public void testWriteRecordTimeBasedPartitionWallclockRealtime() throws Exception {
    setUp();

    long timeBucketMs = TimeUnit.SECONDS.toMillis(5);
    long partitionDurationMs = TimeUnit.MINUTES.toMillis(1);
    // Define the partitioner
    Partitioner<?> partitioner = new TimeBasedPartitioner<>();
    parsedConfig.put(PartitionerConfig.PARTITION_DURATION_MS_CONFIG, partitionDurationMs);
    parsedConfig.put(
        PartitionerConfig.PATH_FORMAT_CONFIG, "'year'=YYYY_'month'=MM_'day'=dd_'hour'=HH_'min'=mm_'sec'=ss");
    partitioner.configure(parsedConfig);

    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION, storage, writerProvider, partitioner, connectorConfig, context);

    String key = "key";
    Schema schema = createSchema();
    List<Struct> records = createRecordBatches(schema, 3, 6);
    Collection<SinkRecord> sinkRecords = createSinkRecords(records.subList(0, 9), key, schema);
    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    long timestampFirst = SYSTEM.milliseconds();
    topicPartitionWriter.write();

    SYSTEM.sleep(timeBucketMs);

    sinkRecords = createSinkRecords(records.subList(9, 18), key, schema, 9);
    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    long timestampLater = SYSTEM.milliseconds();
    topicPartitionWriter.write();
    topicPartitionWriter.close();

    String encodedPartitionFirst = getTimebasedEncodedPartition(timestampFirst);
    String encodedPartitionLater = getTimebasedEncodedPartition(timestampLater);

    String dirPrefixFirst = partitioner.generatePartitionedPath(TOPIC, encodedPartitionFirst);
    List<String> expectedFiles = new ArrayList<>();
    for (int i : new int[]{0, 3, 6}) {
      expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefixFirst, TOPIC_PARTITION, i, extension,
                                                  ZERO_PAD_FMT));
    }

    String dirPrefixLater = partitioner.generatePartitionedPath(TOPIC, encodedPartitionLater);
    for (int i : new int[]{9, 12, 15}) {
      expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefixLater, TOPIC_PARTITION, i, extension,
                                                  ZERO_PAD_FMT));
    }
    verify(expectedFiles, 3, schema, records);
  }

  @Test
  public void testWriteRecordTimeBasedPartitionWallclockMocked() throws Exception {
    localProps.put(S3SinkConnectorConfig.FLUSH_SIZE_CONFIG, "1000");
    localProps.put(
        S3SinkConnectorConfig.ROTATE_INTERVAL_MS_CONFIG,
        String.valueOf(TimeUnit.HOURS.toMillis(1))
    );
    setUp();

    // Define the partitioner
    TimeBasedPartitioner<?> partitioner = new TimeBasedPartitioner<>();
    parsedConfig.put(PartitionerConfig.PARTITION_DURATION_MS_CONFIG, TimeUnit.DAYS.toMillis(1));
    parsedConfig.put(
        PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MockedWallclockTimestampExtractor.class.getName());
    partitioner.configure(parsedConfig);
    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION, storage, writerProvider, partitioner, connectorConfig, context);

    String key = "key";
    Schema schema = createSchema();
    List<Struct> records = createRecordBatches(schema, 3, 6);
    Collection<SinkRecord> sinkRecords = createSinkRecords(records.subList(0, 9), key, schema);
    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    MockTime time = ((MockedWallclockTimestampExtractor) partitioner.getTimestampExtractor()).time;
    // Bring the clock to present.
    time.sleep(SYSTEM.milliseconds());
    long timestampFirst = time.milliseconds();
    topicPartitionWriter.write();

    // 2 hours
    time.sleep(2 * 3600 * 1000);

    sinkRecords = createSinkRecords(records.subList(9, 18), key, schema, 9);
    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    long timestampLater = time.milliseconds();
    topicPartitionWriter.write();

    // 1 hours and 1 ms, send another record to flush the pending ones.
    time.sleep(3600 * 1000 + 1);

    sinkRecords = createSinkRecords(records.subList(17, 18), key, schema, 1);
    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }
    topicPartitionWriter.write();
    topicPartitionWriter.close();

    String encodedPartitionFirst = getTimebasedEncodedPartition(timestampFirst);
    String encodedPartitionLater = getTimebasedEncodedPartition(timestampLater);

    String dirPrefixFirst = partitioner.generatePartitionedPath(TOPIC, encodedPartitionFirst);
    List<String> expectedFiles = new ArrayList<>();
    for (int i : new int[]{0}) {
      expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefixFirst, TOPIC_PARTITION, i, extension,
                                                  ZERO_PAD_FMT));
    }

    String dirPrefixLater = partitioner.generatePartitionedPath(TOPIC, encodedPartitionLater);
    for (int i : new int[]{9}) {
      expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefixLater, TOPIC_PARTITION, i, extension,
                                                  ZERO_PAD_FMT));
    }
    verify(expectedFiles, 9, schema, records);
  }

  @Test
  public void testWriteRecordTimeBasedPartitionRecordTimestampHours() throws Exception {
    // Do not roll on size, only based on time.
    localProps.put(S3SinkConnectorConfig.FLUSH_SIZE_CONFIG, "1000");
    localProps.put(
        S3SinkConnectorConfig.ROTATE_INTERVAL_MS_CONFIG,
        String.valueOf(TimeUnit.MINUTES.toMillis(1))
    );
    setUp();

    // Define the partitioner
    Partitioner<?> partitioner = new HourlyPartitioner<>();
    parsedConfig.put(S3SinkConnectorConfig.ROTATE_INTERVAL_MS_CONFIG, TimeUnit.MINUTES.toMillis(1));
    parsedConfig.put(PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, "Record");
    partitioner.configure(parsedConfig);

    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION, storage, writerProvider, partitioner, connectorConfig, context);

    String key = "key";
    Schema schema = createSchema();
    List<Struct> records = createRecordBatches(schema, 3, 6);
    DateTime first = new DateTime(2017, 3, 2, 10, 0, DateTimeZone.forID("America/Los_Angeles"));
    // One record every 20 sec, puts 3 records every minute/rotate interval
    long advanceMs = 20000;
    long timestampFirst = first.getMillis();
    Collection<SinkRecord> sinkRecords = createSinkRecordsWithTimestamp(records.subList(0, 9), key, schema, 0,
                                                                        timestampFirst, advanceMs);
    long timestampLater = first.plusHours(2).getMillis();
    sinkRecords.addAll(
        createSinkRecordsWithTimestamp(records.subList(9, 18), key, schema, 9, timestampLater, advanceMs));

    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    topicPartitionWriter.write();
    topicPartitionWriter.close();

    String encodedPartitionFirst = getTimebasedEncodedPartition(timestampFirst);
    String encodedPartitionLater = getTimebasedEncodedPartition(timestampLater);

    String dirPrefixFirst = partitioner.generatePartitionedPath(TOPIC, encodedPartitionFirst);
    List<String> expectedFiles = new ArrayList<>();
    for (int i : new int[]{0, 3, 6}) {
      expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefixFirst, TOPIC_PARTITION, i, extension,
                                                  ZERO_PAD_FMT));
    }

    String dirPrefixLater = partitioner.generatePartitionedPath(TOPIC, encodedPartitionLater);
    // Records 15,16,17 won't be flushed until a record with a higher timestamp arrives.
    for (int i : new int[]{9, 12}) {
      expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefixLater, TOPIC_PARTITION, i, extension,
                                                  ZERO_PAD_FMT));
    }
    verify(expectedFiles, 3, schema, records);
  }

  @Test
  public void testWriteRecordTimeBasedPartitionRecordTimestampDays() throws Exception {
    localProps.put(S3SinkConnectorConfig.FLUSH_SIZE_CONFIG, "1000");
    localProps.put(
        S3SinkConnectorConfig.ROTATE_INTERVAL_MS_CONFIG,
        String.valueOf(TimeUnit.MINUTES.toMillis(1))
    );
    setUp();

    // Define the partitioner
    Partitioner<?> partitioner = new DailyPartitioner<>();
    parsedConfig.put(PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, "Record");
    parsedConfig.put(PartitionerConfig.PATH_FORMAT_CONFIG, "'year'=YYYY_'month'=MM_'day'=dd");
    partitioner.configure(parsedConfig);

    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION, storage, writerProvider, partitioner, connectorConfig, context);

    String key = "key";
    Schema schema = createSchema();
    List<Struct> records = createRecordBatches(schema, 3, 6);
    DateTime first = new DateTime(2017, 3, 2, 10, 0, DateTimeZone.forID("America/Los_Angeles"));
    // One record every 20 sec, puts 3 records every minute/rotate interval
    long advanceMs = 20000;
    long timestampFirst = first.getMillis();
    Collection<SinkRecord> sinkRecords = createSinkRecordsWithTimestamp(records.subList(0, 9), key, schema, 0,
                                                                        timestampFirst, advanceMs);
    long timestampLater = first.plusHours(2).getMillis();
    sinkRecords.addAll(
        createSinkRecordsWithTimestamp(records.subList(9, 18), key, schema, 9, timestampLater, advanceMs));

    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    topicPartitionWriter.write();
    topicPartitionWriter.close();

    String encodedPartitionFirst = getTimebasedEncodedPartition(timestampFirst);
    String encodedPartitionLater = getTimebasedEncodedPartition(timestampLater);

    String dirPrefixFirst = partitioner.generatePartitionedPath(TOPIC, encodedPartitionFirst);
    List<String> expectedFiles = new ArrayList<>();
    for (int i : new int[]{0, 3, 6}) {
      expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefixFirst, TOPIC_PARTITION, i, extension,
                                                  ZERO_PAD_FMT));
    }

    String dirPrefixLater = partitioner.generatePartitionedPath(TOPIC, encodedPartitionLater);
    // Records 15,16,17 won't be flushed until a record with a higher timestamp arrives.
    for (int i : new int[]{9, 12}) {
      expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefixLater, TOPIC_PARTITION, i, extension,
                                                  ZERO_PAD_FMT));
    }
    verify(expectedFiles, 3, schema, records);
  }

  @Test(expected = ConnectException.class)
  public void testWriteRecordTimeBasedPartitionWithNullTimestamp() throws Exception {
    localProps.put(
        S3SinkConnectorConfig.ROTATE_INTERVAL_MS_CONFIG,
        String.valueOf(TimeUnit.MINUTES.toMillis(1))
    );
    setUp();

    // Define the partitioner
    Partitioner<?> partitioner = new TimeBasedPartitioner<>();
    parsedConfig.put(PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, "Record");
    parsedConfig.put(PartitionerConfig.PATH_FORMAT_CONFIG, "'year'=YYYY_'month'=MM_'day'=dd");
    parsedConfig.put(PartitionerConfig.PARTITION_DURATION_MS_CONFIG, TimeUnit.DAYS.toMillis(1));
    partitioner.configure(parsedConfig);

    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION, storage, writerProvider, partitioner, connectorConfig, context);

    String key = "key";
    Schema schema = createSchema();
    List<Struct> records = createRecordBatches(schema, 1, 1);
    // Just one bad record with null timestamp
    Collection<SinkRecord> sinkRecords = createSinkRecords(records, key, schema, 0);

    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    topicPartitionWriter.write();
  }

  @Test
  public void testWallclockUsesBatchTimePartitionBoundary() throws Exception {
    localProps.put(S3SinkConnectorConfig.FLUSH_SIZE_CONFIG, "6");
    setUp();

    // Define the partitioner
    TimeBasedPartitioner<?> partitioner = new TimeBasedPartitioner<>();
    parsedConfig.put(PartitionerConfig.PARTITION_DURATION_MS_CONFIG, TimeUnit.DAYS.toMillis(1));
    parsedConfig.put(
        PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        TimeBasedPartitioner.WallclockTimestampExtractor.class.getName());
    partitioner.configure(parsedConfig);

    Time systemTime = EasyMock.createMock(SystemTime.class);

    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION, storage, writerProvider, partitioner, connectorConfig, context, systemTime);

    // Freeze clock passed into topicPartitionWriter, so we know what time it will use for "now"
    long freezeTime = 3599000L;
    EasyMock.expect(systemTime.milliseconds()).andReturn(freezeTime);
    EasyMock.replay(systemTime);

    String key = "key";
    Schema schema = createSchema();
    List<Struct> records = createRecordBatches(schema, 3, 6);
    Collection<SinkRecord> sinkRecords = createSinkRecords(records.subList(0, 9), key, schema);
    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    // The Wallclock extractor should be passed the frozen time from topicPartitionWriter
    topicPartitionWriter.write();

    List<String> expectedFiles = new ArrayList<>();
    String dirPrefix = partitioner.generatePartitionedPath(TOPIC, getTimebasedEncodedPartition(freezeTime));
    expectedFiles.add(FileUtils.fileKeyToCommit(
        topicsDir, dirPrefix, TOPIC_PARTITION, 0, extension, ZERO_PAD_FMT));
    verify(expectedFiles, 6, schema, records);
  }

  @Test
  public void testWriteRecordTimeBasedPartitionWallclockMockedWithScheduleRotation()
      throws Exception {
    localProps.put(S3SinkConnectorConfig.FLUSH_SIZE_CONFIG, "1000");
    localProps.put(
        S3SinkConnectorConfig.ROTATE_INTERVAL_MS_CONFIG,
        String.valueOf(TimeUnit.HOURS.toMillis(1))
    );
    localProps.put(
        S3SinkConnectorConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG,
        String.valueOf(TimeUnit.MINUTES.toMillis(10))
    );
    setUp();

    // Define the partitioner
    TimeBasedPartitioner<?> partitioner = new TimeBasedPartitioner<>();
    parsedConfig.put(PartitionerConfig.PARTITION_DURATION_MS_CONFIG, TimeUnit.DAYS.toMillis(1));
    parsedConfig.put(
        PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MockedWallclockTimestampExtractor.class.getName());
    partitioner.configure(parsedConfig);

    MockTime time = ((MockedWallclockTimestampExtractor) partitioner.getTimestampExtractor()).time;

    // Bring the clock to present.
    time.sleep(SYSTEM.milliseconds());
    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION, storage, writerProvider, partitioner, connectorConfig, context, time);

    String key = "key";
    Schema schema = createSchema();
    List<Struct> records = createRecordBatches(schema, 3, 6);
    Collection<SinkRecord> sinkRecords = createSinkRecords(records.subList(0, 3), key, schema);
    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    // No records written to S3
    topicPartitionWriter.write();
    long timestampFirst = time.milliseconds();

    // 11 minutes
    time.sleep(TimeUnit.MINUTES.toMillis(11));
    // Records are written due to scheduled rotation
    topicPartitionWriter.write();

    sinkRecords = createSinkRecords(records.subList(3, 6), key, schema, 3);
    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    // More records later
    topicPartitionWriter.write();
    long timestampLater = time.milliseconds();

    // 11 minutes later, another scheduled rotation
    time.sleep(TimeUnit.MINUTES.toMillis(11));

    // Again the records are written due to scheduled rotation
    topicPartitionWriter.write();
    topicPartitionWriter.close();

    String encodedPartitionFirst = getTimebasedEncodedPartition(timestampFirst);
    String encodedPartitionLater = getTimebasedEncodedPartition(timestampLater);

    String dirPrefixFirst = partitioner.generatePartitionedPath(TOPIC, encodedPartitionFirst);
    List<String> expectedFiles = new ArrayList<>();
    for (int i : new int[]{0}) {
      expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefixFirst, TOPIC_PARTITION, i, extension,
                                                  ZERO_PAD_FMT));
    }

    String dirPrefixLater = partitioner.generatePartitionedPath(TOPIC, encodedPartitionLater);
    for (int i : new int[]{3}) {
      expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefixLater, TOPIC_PARTITION, i, extension,
                                                  ZERO_PAD_FMT));
    }
    verify(expectedFiles, 3, schema, records);
  }

  @Test(expected = RetriableException.class)
  public void testPropagateRetriableErrorsDuringTimeBasedCommits() throws Exception {
    localProps.put(S3SinkConnectorConfig.FLUSH_SIZE_CONFIG, "1000");
    localProps.put(
        S3SinkConnectorConfig.ROTATE_INTERVAL_MS_CONFIG,
        String.valueOf(TimeUnit.HOURS.toMillis(1))
    );
    localProps.put(
        S3SinkConnectorConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG,
        String.valueOf(TimeUnit.MINUTES.toMillis(10))
    );
    setUpWithCommitException();

    // Define the partitioner
    TimeBasedPartitioner<?> partitioner = new TimeBasedPartitioner<>();
    parsedConfig.put(PartitionerConfig.PARTITION_DURATION_MS_CONFIG, TimeUnit.DAYS.toMillis(1));
    parsedConfig.put(
        PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MockedWallclockTimestampExtractor.class.getName());
    partitioner.configure(parsedConfig);

    MockTime time = ((MockedWallclockTimestampExtractor) partitioner.getTimestampExtractor()).time;

    // Bring the clock to present.
    time.sleep(SYSTEM.milliseconds());
    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION, storage, writerProvider, partitioner, connectorConfig, context, time);

    String key = "key";
    Schema schema = createSchema();
    List<Struct> records = createRecordBatches(schema, 3, 6);
    Collection<SinkRecord> sinkRecords = createSinkRecords(records.subList(0, 3), key, schema);
    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    // No records written to S3
    topicPartitionWriter.write();
    long timestampFirst = time.milliseconds();

    // 11 minutes
    time.sleep(TimeUnit.MINUTES.toMillis(11));
    // Records are written due to scheduled rotation
    topicPartitionWriter.write();
  }

  @Test
  public void testWriteRecordTimeBasedPartitionFieldTimestampHours() throws Exception {
    // Do not roll on size, only based on time.
    localProps.put(S3SinkConnectorConfig.FLUSH_SIZE_CONFIG, "1000");
    localProps.put(
        S3SinkConnectorConfig.ROTATE_INTERVAL_MS_CONFIG,
        String.valueOf(TimeUnit.MINUTES.toMillis(1))
    );
    setUp();

    // Define the partitioner
    Partitioner<?> partitioner = new HourlyPartitioner<>();
    parsedConfig.put(PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, "RecordField");
    partitioner.configure(parsedConfig);

    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION, storage, writerProvider, partitioner, connectorConfig, context);

    String key = "key";
    Schema schema = createSchemaWithTimestampField();

    DateTime first = new DateTime(2017, 3, 2, 10, 0, DateTimeZone.forID("America/Los_Angeles"));
    // One record every 20 sec, puts 3 records every minute/rotate interval
    long advanceMs = 20000;
    long timestampFirst = first.getMillis();
    int size = 18;

    ArrayList<Struct> records = new ArrayList<>(size);
    for (int i = 0; i < size/2; ++i) {
      records.add(createRecordWithTimestampField(schema, timestampFirst));
      timestampFirst += advanceMs;
    }
    Collection<SinkRecord> sinkRecords = createSinkRecords(records.subList(0, 9), key, schema);

    long timestampLater = first.plusHours(2).getMillis();
    for (int i = size/2; i < size; ++i) {
      records.add(createRecordWithTimestampField(schema, timestampLater));
      timestampLater += advanceMs;
    }
    sinkRecords.addAll(createSinkRecords(records.subList(9, 18), key, schema, 9));

    // And one last record to flush the previous ones.
    long timestampMuchLater = first.plusHours(6).getMillis();
    Struct lastOne = createRecordWithTimestampField(schema, timestampMuchLater);
    sinkRecords.addAll(createSinkRecords(Collections.singletonList(lastOne), key, schema, 19));

    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    topicPartitionWriter.write();
    topicPartitionWriter.close();

    String encodedPartitionFirst = getTimebasedEncodedPartition(timestampFirst);
    String encodedPartitionLater = getTimebasedEncodedPartition(timestampLater);

    String dirPrefixFirst = partitioner.generatePartitionedPath(TOPIC, encodedPartitionFirst);
    List<String> expectedFiles = new ArrayList<>();
    for (int i : new int[]{0, 3, 6}) {
      expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefixFirst, TOPIC_PARTITION, i, extension,
                                                  ZERO_PAD_FMT));
    }

    String dirPrefixLater = partitioner.generatePartitionedPath(TOPIC, encodedPartitionLater);
    for (int i : new int[]{9, 12, 15}) {
      expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefixLater, TOPIC_PARTITION, i, extension,
                                                  ZERO_PAD_FMT));
    }
    verify(expectedFiles, 3, schema, records);
  }

  private String getTimebasedEncodedPartition(long timestamp) {
    long partitionDurationMs = (Long) parsedConfig.get(PartitionerConfig.PARTITION_DURATION_MS_CONFIG);
    String pathFormat = (String) parsedConfig.get(PartitionerConfig.PATH_FORMAT_CONFIG);
    String timeZone = (String) parsedConfig.get(PartitionerConfig.TIMEZONE_CONFIG);
    return TimeUtils.encodeTimestamp(partitionDurationMs, pathFormat, timeZone, timestamp);
  }

  @Test
  public void testNoFilesWrittenWithoutCommit() throws Exception {
    // Setting size-based rollup to 10 but will produce fewer records. Commit should not happen.
    localProps.put(S3SinkConnectorConfig.FLUSH_SIZE_CONFIG, "10");
    setUp();

    // Define the partitioner
    Partitioner<?> partitioner = new DefaultPartitioner<>();
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

    // Record size argument does not matter.
    verify(Collections.<String>emptyList(), -1, schema, records);
  }

  @Test
  public void testRotateIntervalIsIgnoredWhenUsedWithNoTimeBasedPartitioner() throws Exception {
    // Setting size-based rollup to 10 but will produce fewer records. Commit should not happen.
    localProps.put(S3SinkConnectorConfig.FLUSH_SIZE_CONFIG, "10");
    localProps.put(
        S3SinkConnectorConfig.ROTATE_INTERVAL_MS_CONFIG,
        String.valueOf(TimeUnit.MINUTES.toMillis(1))
    );
    setUp();

    // Define the partitioner
    Partitioner<?> partitioner = new DefaultPartitioner<>();
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

    // Record size argument does not matter.
    verify(Collections.<String>emptyList(), -1, schema, records);
  }

  @Test
  public void testWriteRecordDefaultWithEmptyTopicsDir() throws Exception {
    localProps.put(StorageCommonConfig.TOPICS_DIR_CONFIG, "");
    setUp();

    // Define the partitioner
    Partitioner<?> partitioner = new DefaultPartitioner<>();
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
    expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 0, extension, ZERO_PAD_FMT));
    expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 3, extension, ZERO_PAD_FMT));
    expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 6, extension, ZERO_PAD_FMT));
    verify(expectedFiles, 3, schema, records);
  }

  @Test
  public void testAddingS3ObjectTags() throws Exception{
    // Setting size-based rollup to 10 but will produce fewer records. Commit should not happen.
    localProps.put(S3SinkConnectorConfig.S3_OBJECT_TAGGING_CONFIG, "true");
    setUp();

    // Define the partitioner
    Partitioner<?> partitioner = new DefaultPartitioner<>();
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

    // Check expected s3 object tags
    String dirPrefix = partitioner.generatePartitionedPath(TOPIC, "partition=" + PARTITION);
    Map<String, List<Tag>> expectedTaggedFiles = new HashMap<>();
    expectedTaggedFiles.put(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 0, extension, ZERO_PAD_FMT),
            Arrays.asList(new Tag("startOffset", "0"), new Tag("endOffset", "2"), new Tag("recordCount", "3")));
    expectedTaggedFiles.put(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 3, extension, ZERO_PAD_FMT),
            Arrays.asList(new Tag("startOffset", "3"), new Tag("endOffset", "5"), new Tag("recordCount", "3")));
    expectedTaggedFiles.put(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 6, extension, ZERO_PAD_FMT),
            Arrays.asList(new Tag("startOffset", "6"), new Tag("endOffset", "8"), new Tag("recordCount", "3")));
    verifyTags(expectedTaggedFiles);
  }

  private Struct createRecord(Schema schema, int ibase, float fbase) {
    return new Struct(schema)
               .put("boolean", true)
               .put("int", ibase)
               .put("long", (long) ibase)
               .put("float", fbase)
               .put("double", (double) fbase);
  }

  // Create a batch of records with incremental numeric field values. Total number of records is given by 'size'.
  private List<Struct> createRecordBatch(Schema schema, int size) {
    ArrayList<Struct> records = new ArrayList<>(size);
    int ibase = 16;
    float fbase = 12.2f;

    for (int i = 0; i < size; ++i) {
      records.add(createRecord(schema, ibase + i, fbase + i));
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
  private List<SinkRecord> createSinkRecords(List<Struct> records, String key, Schema schema) {
    return createSinkRecords(records, key, schema, 0);
  }

  // Given a list of records, create a list of sink records with contiguous offsets.
  private List<SinkRecord> createSinkRecords(List<Struct> records, String key, Schema schema, int startOffset) {
    ArrayList<SinkRecord> sinkRecords = new ArrayList<>();
    for (int i = 0; i < records.size(); ++i) {
      sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, records.get(i),
                                     i + startOffset));
    }
    return sinkRecords;
  }

  // Given a list of records, create a list of sink records with contiguous offsets.
  private List<SinkRecord> createSinkRecordsWithTimestamp(List<Struct> records, String key, Schema schema,
                                                          int startOffset, long startTime, long timeStep) {
    ArrayList<SinkRecord> sinkRecords = new ArrayList<>();
    for (int i = 0, offset = startOffset; i < records.size(); ++i, ++offset) {
      sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, records.get(i), offset,
                                     startTime + offset * timeStep, TimestampType.CREATE_TIME));
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
      Collection<Object> actualRecords = readRecordsAvro(S3_TEST_BUCKET_NAME, fileKey, s3);
      assertEquals(expectedSize, actualRecords.size());
      for (Object avroRecord : actualRecords) {
        Object expectedRecord = format.getAvroData().fromConnectData(schema, records.get(index++));
        assertEquals(expectedRecord, avroRecord);
      }
    }
  }

  private void verifyTags(Map<String, List<Tag>> expectedTaggedFiles)
          throws IOException {
    List<S3ObjectSummary> summaries = listObjects(S3_TEST_BUCKET_NAME, null, s3);
    List<String> actualFiles = new ArrayList<>();
    for (S3ObjectSummary summary : summaries) {
      String fileKey = summary.getKey();
      actualFiles.add(fileKey);
    }

    List<String> expectedFileKeys = new ArrayList<>(expectedTaggedFiles.keySet());
    Collections.sort(actualFiles);
    Collections.sort(expectedFileKeys);
    assertThat(actualFiles, is(expectedFileKeys));

    for (String fileKey : actualFiles) {
      List<Tag> actualTags = getS3ObjectTags(S3_TEST_BUCKET_NAME, fileKey, s3);
      List<Tag> expectedTags = expectedTaggedFiles.get(fileKey);
      assertTrue(actualTags.containsAll(expectedTags));
    }
  }

  public static class MockedWallclockTimestampExtractor implements TimestampExtractor {
    public final MockTime time;

    public MockedWallclockTimestampExtractor() {
      this.time = new MockTime();
    }

    @Override
    public void configure(Map<String, Object> config) {}

    @Override
    public Long extract(ConnectRecord<?> record) {
      return time.milliseconds();
    }
  }
}
