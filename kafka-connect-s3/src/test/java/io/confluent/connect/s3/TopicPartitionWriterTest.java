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

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.Tag;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.common.utils.SystemTime;
import io.confluent.connect.s3.format.KeyValueHeaderRecordWriterProvider;
import io.confluent.connect.s3.format.RecordViewSetter;
import io.confluent.connect.s3.format.RecordViews.HeaderRecordView;
import io.confluent.connect.s3.format.RecordViews.KeyRecordView;
import io.confluent.connect.s3.format.json.JsonFormat;
import io.confluent.connect.s3.storage.CompressionType;
import io.confluent.connect.s3.util.SchemaPartitioner;
import io.confluent.connect.storage.errors.PartitionException;
import io.confluent.kafka.serializers.NonRecordContainer;

import org.apache.avro.util.Utf8;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.errors.SchemaProjectorException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.confluent.common.utils.MockTime;
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
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static io.confluent.connect.s3.S3SinkConnectorConfig.SCHEMA_PARTITION_AFFIX_TYPE_CONFIG;
import static io.confluent.connect.s3.util.Utils.sinkRecordToLoggableString;
import static io.confluent.connect.storage.StorageSinkConnectorConfig.FLUSH_SIZE_CONFIG;
import static io.confluent.connect.storage.StorageSinkConnectorConfig.FORMAT_CLASS_CONFIG;
import static io.confluent.connect.storage.common.StorageCommonConfig.DIRECTORY_DELIM_CONFIG;
import static io.confluent.connect.storage.partitioner.PartitionerConfig.PARTITION_FIELD_NAME_CONFIG;
import static org.apache.kafka.common.utils.Time.SYSTEM;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

@RunWith(Theories.class)
public class TopicPartitionWriterTest extends TestWithMockedS3 {
  // The default
  private static final String ZERO_PAD_FMT = "%010d";
  private static final String HEADER_JSON_EXT = ".headers.json";
  private static final String HEADER_AVRO_EXT = ".headers.avro";
  private static final String KEYS_AVRO_EXT = ".keys.avro";

  private enum RecordElement {
    KEYS,
    HEADERS,
    VALUES
  }

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
    assertTrue(s3.doesBucketExistV2(S3_TEST_BUCKET_NAME));

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
      public S3OutputStream create(String path, boolean overwrite, Class<?> formatClass) {
        return new S3OutputStreamFlaky(path, this.conf(), s3, retries);
      }
    };

    format = new AvroFormat(storage);

    Format<S3SinkConnectorConfig, String> format = new AvroFormat(storage);
    writerProvider = format.getRecordWriterProvider();
    extension = writerProvider.getExtension();
  }

  public void setUpWithTaggingException(boolean mockSdkClientException) throws Exception {
    super.setUp();

    s3 = newS3Client(connectorConfig);
    storage = new S3Storage(connectorConfig, url, S3_TEST_BUCKET_NAME, s3) {
      @Override
      public void addTags(String fileName, Map<String, String> tags) throws SdkClientException {
        if (mockSdkClientException) {
          throw new SdkClientException("Mock SdkClientException while tagging");
        }
        throw new RuntimeException("Mock RuntimeException while tagging");
      }
    };

    format = new AvroFormat(storage);

    s3.createBucket(S3_TEST_BUCKET_NAME);
    assertTrue(s3.doesBucketExistV2(S3_TEST_BUCKET_NAME));

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
        TOPIC_PARTITION, storage, writerProvider, partitioner,  connectorConfig, context, null);

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
        TOPIC_PARTITION, storage, writerProvider, partitioner,  connectorConfig, context, null);

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
    localProps.put(FLUSH_SIZE_CONFIG, "9");
    setUp();

    // Define the partitioner
    Partitioner<?> partitioner = new FieldPartitioner<>();
    partitioner.configure(parsedConfig);

    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION, storage, writerProvider, partitioner, connectorConfig, context, null);

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
    List<String> partitionFields = (List<String>) parsedConfig.get(PARTITION_FIELD_NAME_CONFIG);
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
        TOPIC_PARTITION, storage, writerProvider, partitioner, connectorConfig, context, null);

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
    localProps.put(FLUSH_SIZE_CONFIG, "1000");
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
        TOPIC_PARTITION, storage, writerProvider, partitioner, connectorConfig, context, null);

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
    localProps.put(FLUSH_SIZE_CONFIG, "1000");
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
        TOPIC_PARTITION, storage, writerProvider, partitioner, connectorConfig, context, null);

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
    localProps.put(FLUSH_SIZE_CONFIG, "1000");
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
        TOPIC_PARTITION, storage, writerProvider, partitioner, connectorConfig, context, null);

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
        TOPIC_PARTITION, storage, writerProvider, partitioner, connectorConfig, context, null);

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
    localProps.put(FLUSH_SIZE_CONFIG, "6");
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
        TOPIC_PARTITION, storage, writerProvider, partitioner, connectorConfig, context, systemTime, null);

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
  @DataPoints("affixType")
  public static S3SinkConnectorConfig.AffixType[] affixTypeValues(){
    return new S3SinkConnectorConfig.AffixType[]{
        S3SinkConnectorConfig.AffixType.PREFIX, S3SinkConnectorConfig.AffixType.SUFFIX
    };
  }

  @DataPoints("testWithSchemaData")
  public static boolean[] testWithSchemaDataValues(){
    return new boolean[]{true, false};
  }

  @Theory
  public void testWriteSchemaPartitionerWithAffix(
      @FromDataPoints("affixType")S3SinkConnectorConfig.AffixType affixType,
      @FromDataPoints("testWithSchemaData") boolean testWithSchemaData
  ) throws Exception {
    testWriteSchemaPartitionerWithAffix(testWithSchemaData, affixType);
  }


  @Test
  public void testWriteRecordsAfterScheduleRotationExpiryButNoResetShouldGoToSameFile()
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
        PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        MockedWallclockTimestampExtractor.class.getName());
    partitioner.configure(parsedConfig);

    MockTime time = ((MockedWallclockTimestampExtractor) partitioner.getTimestampExtractor()).time;

    // Bring the clock to present.
    time.sleep(SYSTEM.milliseconds());

    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION, storage, writerProvider, partitioner, connectorConfig, context, time,
        null);

    // sleep for 11 minutes after startup
    time.sleep(TimeUnit.MINUTES.toMillis(11));

    //send new records after ScheduleRotation is expired but not reset
    String key = "key";
    Schema schema = createSchema();
    List<Struct> records = createRecordBatches(schema, 3, 6);
    Collection<SinkRecord> sinkRecords = createSinkRecords(records.subList(0, 3), key, schema);
    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    // No records written to S3
    topicPartitionWriter.write();
    // 11 minutes
    time.sleep(TimeUnit.MINUTES.toMillis(11));
    // Records are written due to scheduled rotation
    topicPartitionWriter.write();
    topicPartitionWriter.close();
    long timestampFirst = time.milliseconds();

    String encodedPartitionFirst = getTimebasedEncodedPartition(timestampFirst);

    String dirPrefixFirst = partitioner.generatePartitionedPath(TOPIC, encodedPartitionFirst);
    List<String> expectedFiles = new ArrayList<>();
    for (int i : new int[]{0}) {
      expectedFiles
          .add(FileUtils.fileKeyToCommit(topicsDir, dirPrefixFirst, TOPIC_PARTITION, i, extension,
              ZERO_PAD_FMT));
    }
    verify(expectedFiles, 3, schema, records);
  }

  @Test
  public void testWriteRecordsAfterCurrentScheduleRotationExpiryShouldGoToSameFile()
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
        PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        MockedWallclockTimestampExtractor.class.getName());
    partitioner.configure(parsedConfig);

    MockTime time = ((MockedWallclockTimestampExtractor) partitioner.getTimestampExtractor()).time;

    // Bring the clock to present.
    time.sleep(SYSTEM.milliseconds());

    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION, storage, writerProvider, partitioner, connectorConfig, context, time,
        null);

    // sleep for 11 minutes after startup
    time.sleep(TimeUnit.MINUTES.toMillis(11));

    topicPartitionWriter.write();
    //send records after nextScheduledRotation is reset by topicPartitionWriter.write()
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

    //simulate idle loop and kafka calling put with no records
    for (int i = 0; i < 5; i++) {
      // sleep for 11 minutes after startup
      time.sleep(TimeUnit.MINUTES.toMillis(11));
      //nextScheduledRotation should be reset
      topicPartitionWriter.write();
    }

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
      expectedFiles
          .add(FileUtils.fileKeyToCommit(topicsDir, dirPrefixFirst, TOPIC_PARTITION, i, extension,
              ZERO_PAD_FMT));
    }

    String dirPrefixLater = partitioner.generatePartitionedPath(TOPIC, encodedPartitionLater);
    for (int i : new int[]{3}) {
      expectedFiles
          .add(FileUtils.fileKeyToCommit(topicsDir, dirPrefixLater, TOPIC_PARTITION, i, extension,
              ZERO_PAD_FMT));
    }
    verify(expectedFiles, 3, schema, records);
  }

  @Test
  public void testWriteRecordTimeBasedPartitionWallclockMockedWithScheduleRotation()
      throws Exception {
    localProps.put(FLUSH_SIZE_CONFIG, "1000");
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
        TOPIC_PARTITION, storage, writerProvider, partitioner, connectorConfig, context, time, null);

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
    localProps.put(FLUSH_SIZE_CONFIG, "1000");
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
        TOPIC_PARTITION, storage, writerProvider, partitioner, connectorConfig, context, time, null);

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
    localProps.put(FLUSH_SIZE_CONFIG, "1000");
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
        TOPIC_PARTITION, storage, writerProvider, partitioner, connectorConfig, context, null);

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
    localProps.put(FLUSH_SIZE_CONFIG, "10");
    setUp();

    // Define the partitioner
    Partitioner<?> partitioner = new DefaultPartitioner<>();
    partitioner.configure(parsedConfig);
    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION, storage, writerProvider, partitioner,  connectorConfig, context, null);

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
    localProps.put(FLUSH_SIZE_CONFIG, "10");
    localProps.put(
        S3SinkConnectorConfig.ROTATE_INTERVAL_MS_CONFIG,
        String.valueOf(TimeUnit.MINUTES.toMillis(1))
    );
    setUp();

    // Define the partitioner
    Partitioner<?> partitioner = new DefaultPartitioner<>();
    partitioner.configure(parsedConfig);
    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION, storage, writerProvider, partitioner,  connectorConfig, context, null);

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
        TOPIC_PARTITION, storage, writerProvider, partitioner,  connectorConfig, context, null);

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
            TOPIC_PARTITION, storage, writerProvider, partitioner,  connectorConfig, context, null);

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

  @Test
  public void testIgnoreS3ObjectTaggingSdkClientException() throws Exception {
    // Tagging error occurred (SdkClientException) but getting ignored.
    testS3ObjectTaggingErrorHelper(true, true);
  }

  @Test
  public void testIgnoreS3ObjectTaggingRuntimeException() throws Exception {
    // Tagging error occurred (RuntimeException) but getting ignored.
    testS3ObjectTaggingErrorHelper(false, true);
  }

  @Test
  public void testFailS3ObjectTaggingSdkClientException() throws Exception {
    ConnectException exception = assertThrows(ConnectException.class,
            () -> testS3ObjectTaggingErrorHelper(true, false));
    assertEquals("Unable to tag S3 object topics_test-topic_partition=12_test-topic#12#0000000000.avro", exception.getMessage());
    assertEquals("Mock SdkClientException while tagging", exception.getCause().getMessage());
  }

  @Test
  public void testFailS3ObjectTaggingRuntimeException() throws Exception {
    ConnectException exception = assertThrows(ConnectException.class, () ->
            testS3ObjectTaggingErrorHelper(false, false));
    assertEquals("Unable to tag S3 object topics_test-topic_partition=12_test-topic#12#0000000000.avro", exception.getMessage());
    assertEquals("Mock RuntimeException while tagging", exception.getCause().getMessage());
  }

  @Test
  public void testExceptionOnNullKeysReported() throws Exception {
    String recordValue = "1";
    int kafkaOffset = 2;
    SinkRecord faultyRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null,
        Schema.STRING_SCHEMA, recordValue, kafkaOffset, 0L, TimestampType.NO_TIMESTAMP_TYPE, sampleHeaders());

    String exceptionMessage = String.format("Key cannot be null for SinkRecord: %s", sinkRecordToLoggableString(faultyRecord));
    testExceptionReportedToDLQ(faultyRecord, DataException.class, exceptionMessage, false, false);
    tearDown(); // clear mock S3 port for follow up test
    // test with faulty being first in batch
    testExceptionReportedToDLQ(faultyRecord, DataException.class, exceptionMessage, true, false);
  }

  @Test
  public void testExceptionOnEmptyHeadersReported() throws Exception{
    String recordValue = "1";
    int kafkaOffset = 2;
    SinkRecord faultyRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, "key",
        Schema.STRING_SCHEMA, recordValue, kafkaOffset, 0L, TimestampType.NO_TIMESTAMP_TYPE, Collections.emptyList());

    String exceptionMessage = String.format("Headers cannot be null for SinkRecord: %s", sinkRecordToLoggableString(faultyRecord));
    testExceptionReportedToDLQ(faultyRecord, DataException.class, exceptionMessage, false, false);
    tearDown(); // clear mock S3 port for follow up test
    // test with faulty being first in batch
    testExceptionReportedToDLQ(faultyRecord, DataException.class, exceptionMessage, true, false);
  }

  @Test
  public void testExceptionOnNullHeadersReported() throws Exception {
    String recordValue = "1";
    int kafkaOffset = 1;
    SinkRecord faultyRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, "key",
        Schema.STRING_SCHEMA, recordValue, kafkaOffset, 0L, TimestampType.NO_TIMESTAMP_TYPE, null);

    String exceptionMessage = String.format("Headers cannot be null for SinkRecord: %s", sinkRecordToLoggableString(faultyRecord));
    testExceptionReportedToDLQ(faultyRecord, DataException.class, exceptionMessage, false, false);
    tearDown(); // clear mock S3 port for follow up test
    // test with faulty being first in batch
    testExceptionReportedToDLQ(faultyRecord, DataException.class, exceptionMessage, true, false);
  }

  @Test
  public void testSchemaProjectionExceptionReported() throws Exception {
    String recordValue = "1";
    int kafkaOffset = 1;
    SinkRecord faultyRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, "key",
        null, recordValue, kafkaOffset, 0L, TimestampType.NO_TIMESTAMP_TYPE, sampleHeaders());

    String exceptionMessage = "Switch between schema-based and schema-less data is not supported";
    testExceptionReportedToDLQ(faultyRecord, SchemaProjectorException.class, exceptionMessage, false, true);
  }

  @Test
  public void testPartitioningExceptionReported() throws Exception {
    String field = "field";
    setUp();

    // Define the partitioner
    Partitioner<?> partitioner = new FieldPartitioner<>();
    parsedConfig.put(PARTITION_FIELD_NAME_CONFIG, Arrays.asList(field));
    partitioner.configure(parsedConfig);

    SinkTaskContext mockContext = mock(SinkTaskContext.class);
    ErrantRecordReporter mockReporter = mock(ErrantRecordReporter.class);
    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION, storage, writerProvider, partitioner,  connectorConfig, mockContext, mockReporter);

    Schema schema = SchemaBuilder.struct().field(field, Schema.STRING_SCHEMA);
    Struct struct = new Struct(schema).put(field, "a");

    topicPartitionWriter.buffer(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, "key", schema, struct, 0));
    // non-struct record should throw exception and get reported
    topicPartitionWriter.buffer(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, "key", schema, "not a struct", 1));

    // Test actual write
    topicPartitionWriter.write();
    ArgumentCaptor<Throwable> exceptionCaptor = ArgumentCaptor.forClass(PartitionException.class);
    Mockito.verify(mockReporter, times(1)).report(any(), exceptionCaptor.capture());
    assertEquals("Error encoding partition.", exceptionCaptor.getValue().getMessage());

    topicPartitionWriter.close();
  }

  // test DLQ for lower level exception coming from AvroData
  @Test
  public void testDataExceptionReportedIncorrectSchema() throws Exception {
    int kafkaOffset = 1;
    SinkRecord faultyRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, "key",
        createSchema(), "1", kafkaOffset, 0L, TimestampType.NO_TIMESTAMP_TYPE, sampleHeaders());

    String exceptionMessage = "Invalid type for STRUCT: class java.lang.String";
    testExceptionReportedToDLQ(faultyRecord, DataException.class, exceptionMessage, false, true);
    tearDown(); // clear mock S3 port for follow up test
    // test with faulty being first in batch
    testExceptionReportedToDLQ(faultyRecord, DataException.class, exceptionMessage, true, true);
  }

  // test DLQ for lower level exception coming from AvroData
  @Test
  public void testDataExceptionReportedNullValueForNonOptionalSchema() throws Exception {
    SinkRecord faultyRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, "key",
        createSchema(), null, 1, 0L, TimestampType.NO_TIMESTAMP_TYPE, sampleHeaders());

    String exceptionMessage = "Found null value for non-optional schema";
    testExceptionReportedToDLQ(faultyRecord, DataException.class, exceptionMessage, false, true);
    tearDown(); // clear mock S3 port for follow up test
    // test with faulty being first in batch
    testExceptionReportedToDLQ(faultyRecord, DataException.class, exceptionMessage, true, true);
  }

  @Test
  public void testRecordKeysAndHeadersWritten() throws Exception {
    setUp();
    // Define the partitioner
    Partitioner<?> partitioner = new DefaultPartitioner<>();
    partitioner.configure(parsedConfig);
    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION, storage, getKeyHeaderValueProvider(), partitioner,  connectorConfig, context, null);

    Schema schema = createSchema();
    List<Struct> records = createRecordBatches(schema, 3, 3);
    List<SinkRecord> sinkRecords = createSinkRecordsWithHeaders(records, "key", schema);

    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    // Test actual write
    topicPartitionWriter.write();
    topicPartitionWriter.close();

    String dirPrefix = partitioner.generatePartitionedPath(TOPIC, "partition=" + PARTITION);

    List<String> expectedValueFiles = new ArrayList<>();
    expectedValueFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 0, extension, ZERO_PAD_FMT));
    expectedValueFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 3, extension, ZERO_PAD_FMT));
    expectedValueFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 6, extension, ZERO_PAD_FMT));
    verifyRecordElement(expectedValueFiles, 3, sinkRecords, RecordElement.VALUES);

    List<String> expectedHeaderFiles = new ArrayList<>();
    expectedHeaderFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 0, ".headers.avro", ZERO_PAD_FMT));
    expectedHeaderFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 3, ".headers.avro", ZERO_PAD_FMT));
    expectedHeaderFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 6, ".headers.avro", ZERO_PAD_FMT));
    verifyRecordElement(expectedHeaderFiles, 3, sinkRecords, RecordElement.HEADERS);

    List<String> expectedKeyFiles = new ArrayList<>();
    expectedKeyFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 0, ".keys.avro", ZERO_PAD_FMT));
    expectedKeyFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 3, ".keys.avro", ZERO_PAD_FMT));
    expectedKeyFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 6, ".keys.avro", ZERO_PAD_FMT));
    verifyRecordElement(expectedKeyFiles, 3, sinkRecords, RecordElement.KEYS);
  }

  @Test
  public void testRecordHeadersWrittenJson() throws Exception {
    setUp();
    // Define the partitioner
    Partitioner<?> partitioner = new DefaultPartitioner<>();
    partitioner.configure(parsedConfig);

    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION, storage, getKeyHeaderValueProviderJsonHeaders(), partitioner,  connectorConfig, context, null);

    Schema schema = createSchema();
    List<Struct> records = createRecordBatches(schema, 3, 3);
    List<SinkRecord> sinkRecords = createSinkRecordsWithHeaders(records, "key", schema);

    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    // Test actual write
    topicPartitionWriter.write();
    topicPartitionWriter.close();

    String dirPrefix = partitioner.generatePartitionedPath(TOPIC, "partition=" + PARTITION);

    List<String> expectedValueFiles = new ArrayList<>();
    expectedValueFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 0, extension, ZERO_PAD_FMT));
    expectedValueFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 3, extension, ZERO_PAD_FMT));
    expectedValueFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 6, extension, ZERO_PAD_FMT));
    verifyRecordElement(expectedValueFiles, 3, sinkRecords, RecordElement.VALUES);

    List<String> expectedHeaderFiles = new ArrayList<>();
    expectedHeaderFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 0,
        HEADER_JSON_EXT, ZERO_PAD_FMT));
    expectedHeaderFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 3,
        HEADER_JSON_EXT, ZERO_PAD_FMT));
    expectedHeaderFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 6,
        HEADER_JSON_EXT, ZERO_PAD_FMT));
    verifyRecordElement(expectedHeaderFiles, 3, sinkRecords, RecordElement.HEADERS);
  }

  // Test if a given exception type was reported to the DLQ
  private <T extends DataException> void testExceptionReportedToDLQ(
      SinkRecord faultyRecord,
      Class<T> exceptionType,
      String exceptionMessage,
      boolean faultyFirst,
      boolean performFileCheck
  ) throws Exception {
    setUp();
    // Define the partitioner
    Partitioner<?> partitioner = new DefaultPartitioner<>();
    partitioner.configure(parsedConfig);

    SinkTaskContext mockContext = mock(SinkTaskContext.class);
    ErrantRecordReporter mockReporter = mock(ErrantRecordReporter.class);

    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION, storage, getKeyHeaderValueProvider(), partitioner,  connectorConfig, mockContext, mockReporter);

    // create sample records to write
    Schema schema = createSchema();
    List<Struct> records = createRecordBatches(schema, 3, 3);
    List<SinkRecord> sinkRecords = createSinkRecordsWithHeaders(records, "key", schema);

    // write a few valid records before the faulty one
    // enables DLQ testing for mid-batch errors
    if (!faultyFirst) {
      topicPartitionWriter.buffer(sinkRecords.get(0));
      topicPartitionWriter.buffer(sinkRecords.get(1));
      // should throw exception and get reported
      topicPartitionWriter.buffer(faultyRecord);
      topicPartitionWriter.buffer(faultyRecord); // send second to verify file rotation as expected
      // write rest of records
      for (int i = 2; i < sinkRecords.size(); i++) {
        topicPartitionWriter.buffer(sinkRecords.get(i));
      }
    } else {
      topicPartitionWriter.buffer(faultyRecord);
      topicPartitionWriter.buffer(faultyRecord); // send second to verify file rotation as expected
      // write valid records
      for (SinkRecord record : sinkRecords) {
        topicPartitionWriter.buffer(record);
      }
    }

    // Test actual write
    topicPartitionWriter.write();
    topicPartitionWriter.close();

    // Verify exception was reported
    ArgumentCaptor<T> exceptionCaptor = ArgumentCaptor.forClass(exceptionType);
    Mockito.verify(mockReporter, times(2)).report(any(), exceptionCaptor.capture());
    assertEquals(exceptionMessage, exceptionCaptor.getValue().getMessage());

    // the file check below asserts for file names and contents for cases when a faulty
    // record does not cause an extra rotation, needs to be turned off for special faulty record
    // test cases to pass, the extra rotation is currently expected behavior.
    if (performFileCheck) {
      String dirPrefix = partitioner.generatePartitionedPath(TOPIC, "partition=" + PARTITION);

      List<String> expectedFiles = new ArrayList<>();
      expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 0, extension, ZERO_PAD_FMT));
      expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 3, extension, ZERO_PAD_FMT));
      expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 6, extension, ZERO_PAD_FMT));
      verifyRecordElement(expectedFiles, 3, sinkRecords, RecordElement.VALUES);

      List<String> expectedHeaderFiles = new ArrayList<>();
      expectedHeaderFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 0, ".headers.avro", ZERO_PAD_FMT));
      expectedHeaderFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 3, ".headers.avro", ZERO_PAD_FMT));
      expectedHeaderFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 6, ".headers.avro", ZERO_PAD_FMT));
      verifyRecordElement(expectedHeaderFiles, 3, sinkRecords, RecordElement.HEADERS);

      List<String> expectedKeyFiles = new ArrayList<>();
      expectedKeyFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 0, ".keys.avro", ZERO_PAD_FMT));
      expectedKeyFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 3, ".keys.avro", ZERO_PAD_FMT));
      expectedKeyFiles.add(FileUtils.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 6, ".keys.avro", ZERO_PAD_FMT));
      verifyRecordElement(expectedKeyFiles, 3, sinkRecords, RecordElement.KEYS);
    }
  }

  private RecordWriterProvider<S3SinkConnectorConfig> getKeyHeaderValueProvider() {
    // setup key record provider for writing record key files.
    RecordWriterProvider<S3SinkConnectorConfig> keyWriterProvider =
        new AvroFormat(storage).getRecordWriterProvider();
    ((RecordViewSetter) keyWriterProvider).setRecordView(new KeyRecordView());
    // setup header record provider for writing record header files.
    RecordWriterProvider<S3SinkConnectorConfig> headerWriterProvider =
        new AvroFormat(storage).getRecordWriterProvider();
    ((RecordViewSetter) headerWriterProvider).setRecordView(new HeaderRecordView());
    // initialize the KVHWriterProvider with header and key writers turned on.
    return new KeyValueHeaderRecordWriterProvider(
        new AvroFormat(storage).getRecordWriterProvider(),
        keyWriterProvider,
        headerWriterProvider
    );
  }

  private RecordWriterProvider<S3SinkConnectorConfig> getKeyHeaderValueProviderJsonHeaders() {
    // setup header record provider for writing record header files.
    RecordWriterProvider<S3SinkConnectorConfig> headerWriterProvider =
        new JsonFormat(storage).getRecordWriterProvider();
    ((RecordViewSetter) headerWriterProvider).setRecordView(new HeaderRecordView());
    // initialize the KVHWriterProvider with header and key writers turned on.
    return new KeyValueHeaderRecordWriterProvider(
        new AvroFormat(storage).getRecordWriterProvider(),
        null,
        headerWriterProvider
    );
  }

  public void testWriteSchemaPartitionerWithAffix(
      boolean testWithSchemaData, S3SinkConnectorConfig.AffixType affixType
  ) throws Exception {
    localProps.put(FLUSH_SIZE_CONFIG, "9");
    localProps.put(FORMAT_CLASS_CONFIG, JsonFormat.class.getName());
    setUp();

    Format<S3SinkConnectorConfig, String> myFormat = new JsonFormat(storage);
    writerProvider = myFormat.getRecordWriterProvider();
    extension = writerProvider.getExtension();

    parsedConfig.put(SCHEMA_PARTITION_AFFIX_TYPE_CONFIG, affixType.name());
    // Define the partitioner
    Partitioner<?> basePartitioner = new DefaultPartitioner<>();
    Partitioner<?> partitioner = new SchemaPartitioner<>(basePartitioner);
    partitioner.configure(parsedConfig);

    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION, storage, writerProvider, partitioner, connectorConfig, context, null
    );

    List<Object> testData;
    if (testWithSchemaData) {
      testData = generateTestDataWithSchema(partitioner, affixType);
    } else {
      testData = generateTestDataWithoutSchema(partitioner, affixType);
    }

    String key = (String) testData.get(0);
    List<SinkRecord> actualRecords = (List<SinkRecord>) testData.get(1);
    List<SinkRecord> expectedRecords = (List<SinkRecord>) testData.get(2);
    List<String> expectedFiles = (List<String>) testData.get(3);


    for (SinkRecord actualRecord : actualRecords) {
      topicPartitionWriter.buffer(actualRecord);
    }
    // Test actual write
    topicPartitionWriter.write();
    topicPartitionWriter.close();

    verifyWithJsonOutput(
        expectedFiles, expectedRecords.size() / expectedFiles.size(), expectedRecords, CompressionType.NONE
    );
  }

  private List<Object> generateTestDataWithSchema(
      Partitioner<?> partitioner, S3SinkConnectorConfig.AffixType affixType
  ) {
    String key = "key";

    Schema schema1 = SchemaBuilder.struct()
        .name(null)
        .version(null)
        .field("boolean", Schema.BOOLEAN_SCHEMA)
        .field("int", Schema.INT32_SCHEMA)
        .field("long", Schema.INT64_SCHEMA)
        .field("float", Schema.FLOAT32_SCHEMA)
        .field("double", Schema.FLOAT64_SCHEMA)
        .build();
    List<Struct> records1 = createRecordBatches(schema1, 3, 6);

    Schema schema2 = SchemaBuilder.struct()
        .name("record1")
        .version(1)
        .field("boolean", Schema.BOOLEAN_SCHEMA)
        .field("int", Schema.INT32_SCHEMA)
        .field("long", Schema.INT64_SCHEMA)
        .field("float", Schema.FLOAT32_SCHEMA)
        .field("double", Schema.FLOAT64_SCHEMA)
        .build();

    List<Struct> records2 = createRecordBatches(schema2, 3, 6);

    Schema schema3 = SchemaBuilder.struct()
        .name("record2")
        .version(1)
        .field("boolean", Schema.BOOLEAN_SCHEMA)
        .field("int", Schema.INT32_SCHEMA)
        .field("long", Schema.INT64_SCHEMA)
        .field("float", Schema.FLOAT32_SCHEMA)
        .field("double", Schema.FLOAT64_SCHEMA)
        .build();

    List<Struct> records3 = createRecordBatches(schema3, 3, 6);

    ArrayList<SinkRecord> actualData = new ArrayList<>();
    int offset = 0;
    for (int i = 0; i < records1.size(); i++) {
      actualData.add(
          new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema1, records1.get(i),
              offset++
          )
      );
      actualData.add(
          new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema2, records2.get(i),
              offset++
          )
      );
      actualData.add(
          new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema3, records3.get(i),
              offset++
          )
      );
    }
    List<SinkRecord> expectedRecords = new ArrayList<>();
    int ibase = 16;
    float fbase = 12.2f;
    offset = 0;
    // The expected sequence of records is constructed taking into account that sorting of files occurs in verify

    for (int i = 0; i < 6; ++i) {
      for (int j = 0; j < 3; ++j) {
        expectedRecords.add(
            new SinkRecord(
                TOPIC, PARTITION, Schema.STRING_SCHEMA,
                key, schema1, createRecord(schema1, ibase + j, fbase + j),
                offset++
            )
        );
      }
    }
    for (int i = 0; i < 6; ++i) {
      for (int j = 0; j < 3; ++j) {
        expectedRecords.add(
            new SinkRecord(
                TOPIC, PARTITION, Schema.STRING_SCHEMA,
                key, schema2, createRecord(schema2, ibase + j, fbase + j),
                offset++
            )
        );
      }
    }
    for (int i = 0; i < 6; ++i) {
      for (int j = 0; j < 3; ++j) {
        expectedRecords.add(
            new SinkRecord(
                TOPIC, PARTITION, Schema.STRING_SCHEMA,
                key, schema3, createRecord(schema3, ibase + j, fbase + j),
                offset++
            )
        );
      }
    }

    String dirPrefix1 = generateS3DirectoryPathWithDefaultPartitioner(
        partitioner, affixType, PARTITION, TOPIC, "null"
    );
    String dirPrefix2 = generateS3DirectoryPathWithDefaultPartitioner(
        partitioner, affixType, PARTITION, TOPIC, "record1"
    );
    String dirPrefix3 = generateS3DirectoryPathWithDefaultPartitioner(
        partitioner, affixType, PARTITION, TOPIC, "record2"
    );
    List<String> expectedFiles = new ArrayList<>();
    for (int i = 0; i < 54; i += 9) {
      expectedFiles.add(FileUtils.fileKeyToCommit(
          topicsDir, dirPrefix1, TOPIC_PARTITION, i, extension, ZERO_PAD_FMT
      ));
      expectedFiles.add(FileUtils.fileKeyToCommit(
          topicsDir, dirPrefix2, TOPIC_PARTITION, i + 1, extension, ZERO_PAD_FMT
      ));
      expectedFiles.add(FileUtils.fileKeyToCommit(
          topicsDir, dirPrefix3, TOPIC_PARTITION, i + 2, extension, ZERO_PAD_FMT
      ));
    }
    return Arrays.asList(key, actualData, expectedRecords, expectedFiles);
  }

  private List<Object> generateTestDataWithoutSchema(
      Partitioner<?> partitioner, S3SinkConnectorConfig.AffixType affixType
  ) {
    String key = "key";
    List<String> records = createJsonRecordsWithoutSchema(18);

    ArrayList<SinkRecord> actualData = new ArrayList<>();
    int offset = 0;
    for (String record : records) {
      actualData.add(
          new SinkRecord(
              TOPIC, PARTITION, Schema.STRING_SCHEMA, key, null, record, offset++
          )
      );
    }
    List<SinkRecord> expectedRecords = new ArrayList<>(actualData);

    String dirPrefix = generateS3DirectoryPathWithDefaultPartitioner(
        partitioner, affixType, PARTITION, TOPIC, "null"
    );
    List<String> expectedFiles = new ArrayList<>();
    for (int i = 0; i < 18; i += 9) {
      expectedFiles.add(FileUtils.fileKeyToCommit(
          topicsDir, dirPrefix, TOPIC_PARTITION, i, extension, ZERO_PAD_FMT
      ));
    }
    return Arrays.asList(key, actualData, expectedRecords, expectedFiles);
  }

  private String generateS3DirectoryPathWithDefaultPartitioner(
      Partitioner<?> basePartitioner,
      S3SinkConnectorConfig.AffixType affixType, int partition,
      String topic, String schema_name
  ) {
    if (affixType == S3SinkConnectorConfig.AffixType.SUFFIX) {
      return basePartitioner.generatePartitionedPath(topic,
          "partition=" + partition + parsedConfig.get(DIRECTORY_DELIM_CONFIG)
              + "schema_name" + "=" + schema_name);
    } else if (affixType == S3SinkConnectorConfig.AffixType.PREFIX) {
      return basePartitioner.generatePartitionedPath(topic,
          "schema_name" + "=" + schema_name
              + parsedConfig.get(DIRECTORY_DELIM_CONFIG) + "partition=" + partition);
    } else {
      return basePartitioner.generatePartitionedPath(topic,
          "partition=" + partition);
    }
  }

  protected List<String> createJsonRecordsWithoutSchema(int size) {
    ArrayList<String> records = new ArrayList<>();
    int ibase = 16;
    float fbase = 12.2f;
    for (int i = 0; i < size; ++i) {
      String record = "{\"schema\":{\"type\":\"struct\",\"fields\":[ " +
          "{\"type\":\"boolean\",\"optional\":true,\"field\":\"booleanField\"}," +
          "{\"type\":\"int\",\"optional\":true,\"field\":\"intField\"}," +
          "{\"type\":\"long\",\"optional\":true,\"field\":\"longField\"}," +
          "{\"type\":\"float\",\"optional\":true,\"field\":\"floatField\"}," +
          "{\"type\":\"double\",\"optional\":true,\"field\":\"doubleField\"}]," +
          "\"payload\":" +
          "{\"booleanField\":\"true\"," +
          "\"intField\":" + String.valueOf(ibase + i) + "," +
          "\"longField\":" + String.valueOf((long) ibase + i) + "," +
          "\"floatField\":" + String.valueOf(fbase + i) + "," +
          "\"doubleField\":" + String.valueOf((double) (fbase + i)) +
          "}}";
      records.add(record);
    }
    return records;
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
  private List<SinkRecord> createSinkRecordsWithHeaders(List<Struct> records, String key, Schema schema) {
    return createSinkRecordsWithHeaders(records, key, schema, 0);
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
  private List<SinkRecord> createSinkRecordsWithHeaders(List<Struct> records, String key, Schema schema, int startOffset) {
    ArrayList<SinkRecord> sinkRecords = new ArrayList<>();
    for (int i = 0; i < records.size(); ++i) {
      sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, records.get(i),
          i + startOffset, 0L, TimestampType.NO_TIMESTAMP_TYPE, sampleHeaders()));
    }
    return sinkRecords;
  }

  private Iterable<Header> sampleHeaders() {
    return new ConnectHeaders()
        .addString("first-header-key", "first-header-value")
        .addLong("second-header-key", 8L)
        .addFloat("third-header-key", 6.5f);
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
        Object expectedRecord = format.getAvroData().fromConnectData(records.get(index).schema(), records.get(index++));
        assertEquals(expectedRecord, avroRecord);
      }
    }
  }

  private void verifyWithJsonOutput(
      List<String> expectedFileKeys, int expectedSize,
      List<SinkRecord> expectedRecords, CompressionType compressionType
  ) throws IOException {
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
      Collection<Object> actualRecords = readRecordsJson(
          S3_TEST_BUCKET_NAME, fileKey,
          s3, compressionType
      );
      assertEquals(expectedSize, actualRecords.size());
      for (Object currentRecord : actualRecords) {
        SinkRecord expectedRecord = expectedRecords.get(index++);
        Object expectedValue = expectedRecord.value();
        JsonConverter converter = new JsonConverter();
        converter.configure(Collections.singletonMap("schemas.enable", "false"), false);
        ObjectMapper mapper = new ObjectMapper();
        if (expectedValue instanceof Struct) {
          byte[] expectedBytes = converter.fromConnectData(TOPIC, expectedRecord.valueSchema(), expectedRecord.value());
          expectedValue = mapper.readValue(expectedBytes, Object.class);
        }
        assertEquals(expectedValue, currentRecord);
      }
    }
  }

  // based on verify()
  private void verifyRecordElement(List<String> expectedFileKeys, int expectedSize, List<SinkRecord> records, RecordElement fileType)
      throws IOException {

    List<S3ObjectSummary> summaries = listObjects(S3_TEST_BUCKET_NAME, null, s3);
    List<String> actualFiles;
    switch (fileType) {
      case KEYS:
        actualFiles = getS3FileListKeys(summaries);
        break;
      case HEADERS:
        actualFiles = getS3FileListHeaders(summaries);
        break;
      default:
        actualFiles = getS3FileListValues(summaries);
        break;
    }

    Collections.sort(actualFiles);
    Collections.sort(expectedFileKeys);
    assertEquals(expectedFileKeys, actualFiles);

    int index = 0;
    for (String fileKey : actualFiles) {
      Collection<Object> actualRecords;
      if (fileKey.endsWith(".json")) {
        actualRecords = readRecordsJson(S3_TEST_BUCKET_NAME, fileKey, s3, CompressionType.NONE);
      } else {
        actualRecords = readRecordsAvro(S3_TEST_BUCKET_NAME, fileKey, s3);
      }
      assertEquals(expectedSize, actualRecords.size());
      for (Object avroRecord : actualRecords) {

        SinkRecord currentRecord = records.get(index++);
        Object expectedRecord;
        if (fileKey.endsWith(HEADER_AVRO_EXT)) {
          Schema headerSchema = new HeaderRecordView().getViewSchema(currentRecord, false);
          Object value = new HeaderRecordView().getView(currentRecord, false);
          expectedRecord = ((NonRecordContainer) format.getAvroData().fromConnectData(headerSchema, value)).getValue();
        } else if (fileKey.endsWith(HEADER_JSON_EXT)) {
          Schema headerSchema = new HeaderRecordView().getViewSchema(currentRecord, true);
          Object value = new HeaderRecordView().getView(currentRecord, true);
          byte[] jsonBytes = configuredJsonConverter().fromConnectData(currentRecord.topic(), headerSchema, value);
          JsonParser reader = new ObjectMapper().getFactory().createParser(jsonBytes);
          expectedRecord = reader.readValueAs(Object.class);
        } else if (fileKey.endsWith(KEYS_AVRO_EXT)) {
          expectedRecord = ((NonRecordContainer) format.getAvroData().fromConnectData(currentRecord.keySchema(), currentRecord.key())).getValue();
          expectedRecord = new Utf8((String) expectedRecord); // fix assert conflicts due to java string and avro utf8
        } else {
          expectedRecord = format.getAvroData().fromConnectData(currentRecord.valueSchema(), currentRecord.value());
        }
        assertEquals(expectedRecord, avroRecord);
      }
    }
  }

  private JsonConverter configuredJsonConverter() {
    JsonConverter jsonConverter = new JsonConverter();
    Map<String, Object> converterConfig = new HashMap<>();
    converterConfig.put("schemas.enable", "false");
    converterConfig.put(
        "schemas.cache.size",
        String.valueOf(storage.conf().get(S3SinkConnectorConfig.SCHEMA_CACHE_SIZE_CONFIG))
    );
    jsonConverter.configure(converterConfig, false);
    return jsonConverter;
  }

  // whether a filename contains any of the extensions
  private boolean filenameContainsExtensions(String filename, Set<String> extensions) {
    for (String extension : extensions){
      if (filename.contains(extension)) {
        return true;
      }
    }
    return false;
  }

  // filter for values only.
  private List<String> getS3FileListValues(List<S3ObjectSummary> summaries) {
    Set<String> excludeExtensions = new HashSet<>(Arrays.asList(HEADER_AVRO_EXT, HEADER_JSON_EXT,
        KEYS_AVRO_EXT));
    List<String> filteredFiles = new ArrayList<>();
    for (S3ObjectSummary summary : summaries) {
      String fileKey = summary.getKey();
      if (!filenameContainsExtensions(fileKey, excludeExtensions)) {
        filteredFiles.add(fileKey);
      }
    }
    return filteredFiles;
  }

  private List<String> getS3FileListHeaders(List<S3ObjectSummary> summaries) {
    return getS3FileListFilter(summaries, RecordElement.HEADERS.name().toLowerCase());
  }

  private List<String> getS3FileListKeys(List<S3ObjectSummary> summaries) {
    return getS3FileListFilter(summaries, RecordElement.KEYS.name().toLowerCase());
  }

  // filter for keys or headers
  private List<String> getS3FileListFilter(List<S3ObjectSummary> summaries, String extension) {
    List<String> filteredFiles = new ArrayList<>();
    for (S3ObjectSummary summary : summaries) {
      String fileKey = summary.getKey();
      if (fileKey.contains(extension)) {
        filteredFiles.add(fileKey);
      }
    }
    return filteredFiles;
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

  private void testS3ObjectTaggingErrorHelper(boolean mockSdkClientException, boolean ignoreTaggingError) throws Exception {
    // Enabling tagging and setting behavior for tagging error.
    localProps.put(S3SinkConnectorConfig.S3_OBJECT_TAGGING_CONFIG, "true");
    localProps.put(S3SinkConnectorConfig.S3_OBJECT_BEHAVIOR_ON_TAGGING_ERROR_CONFIG, ignoreTaggingError ? "ignore" : "fail");

    // Setup mock exception while tagging
    setUpWithTaggingException(mockSdkClientException);

    // Define the partitioner
    Partitioner<?> partitioner = new DefaultPartitioner<>();
    partitioner.configure(parsedConfig);
    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
            TOPIC_PARTITION, storage, writerProvider, partitioner,  connectorConfig, context, null);

    String key = "key";
    Schema schema = createSchema();
    List<Struct> records = createRecordBatches(schema, 3, 3);

    Collection<SinkRecord> sinkRecords = createSinkRecords(records, key, schema);

    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    // Invoke write so as to simulate tagging error.
    topicPartitionWriter.write();
    topicPartitionWriter.close();
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
