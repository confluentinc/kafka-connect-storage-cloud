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

import com.amazonaws.services.s3.internal.SkipMd5CheckStrategy;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.SchemaProjector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.confluent.common.utils.MockTime;
import io.confluent.common.utils.Time;
import io.confluent.connect.s3.format.avro.AvroFormat;
import io.confluent.connect.s3.format.avro.AvroUtils;
import io.confluent.connect.s3.storage.S3OutputStream;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.s3.util.FileUtils;
import io.confluent.connect.storage.StorageSinkConnectorConfig;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;
import io.confluent.kafka.serializers.NonRecordContainer;

import static io.confluent.connect.avro.AvroData.AVRO_TYPE_ENUM;
import static org.apache.kafka.common.utils.Time.SYSTEM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DataWriterAvroTest extends DataWriterTestBase<AvroFormat> {

  protected static final String EXTENSION = ".avro";
  private String prevMd5Prop = null;

  public DataWriterAvroTest() {
    super(AvroFormat.class);
  }

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.putAll(localProps);
    return props;
  }

  //@Before should be omitted in order to be able to add properties per test.
  public void setUp() throws Exception {
    super.setUp();
    // Workaround to avoid AWS S3 client failing due to apparently incorrect S3Mock digest
    prevMd5Prop = System.getProperty(
        SkipMd5CheckStrategy.DISABLE_GET_OBJECT_MD5_VALIDATION_PROPERTY
    );
    System.setProperty(SkipMd5CheckStrategy.DISABLE_GET_OBJECT_MD5_VALIDATION_PROPERTY, "true");
  }

  @Override
  protected String getFileExtension() {
    return EXTENSION;
  }

  //@Before should be omitted in order to be able to add properties per test.
  public void setUpWithCommitException() throws Exception {
    super.setUp();

    // We'll replace 'storage' and 'format' here that were created by
    // the base class.
    storage = new S3Storage(connectorConfig, url, S3_TEST_BUCKET_NAME, s3) {
      private final AtomicInteger retries = new AtomicInteger(0);

      @Override
      public S3OutputStream create(String path, boolean overwrite, Class<?> formatClass) {
        return new TopicPartitionWriterTest.S3OutputStreamFlaky(path, this.conf(), s3, retries);
      }
    };
    format = new AvroFormat(storage);
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    localProps.clear();

    // Unset the property to the previous value
    if (prevMd5Prop != null) {
      System.setProperty(
          SkipMd5CheckStrategy.DISABLE_GET_OBJECT_MD5_VALIDATION_PROPERTY,
          prevMd5Prop
      );
    } else {
      System.clearProperty(SkipMd5CheckStrategy.DISABLE_GET_OBJECT_MD5_VALIDATION_PROPERTY);
    }
  }

  @Test
  public void testWriteRecords() throws Exception {
    setUp();
    task = new S3SinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);

    List<SinkRecord> sinkRecords = createRecords(7);
    // Perform write
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long[] validOffsets = {0, 3, 6};
    verify(sinkRecords, validOffsets);

  }

  @Test
  public void testWriteRecordsOfEnumsWithEnhancedAvroData() throws Exception {
    localProps.put(StorageSinkConnectorConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, "true");
    localProps.put(StorageSinkConnectorConfig.CONNECT_META_DATA_CONFIG, "true");
    setUp();
    task = new S3SinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);
    List<SinkRecord> sinkRecords = createRecordsWithEnums(7, 0, Collections.singleton(new TopicPartition(TOPIC, PARTITION)));

    // Perform write
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long[] validOffsets = {0, 3, 6};
    verify(sinkRecords, validOffsets);
  }

  @Test
  public void testWriteRecordsOfUnionsWithEnhancedAvroData() throws Exception {
    localProps.put(StorageSinkConnectorConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, "true");
    localProps.put(StorageSinkConnectorConfig.CONNECT_META_DATA_CONFIG, "true");
    setUp();
    task = new S3SinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);
    List<SinkRecord> sinkRecords = createRecordsWithUnion(7, 0, Collections.singleton(new TopicPartition (TOPIC, PARTITION)));

    // Perform write
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long[] validOffsets = {0, 3, 6, 9, 12, 15, 18, 21, 24, 27};
    verify(sinkRecords, validOffsets);
  }

  @Test
  public void testCompressFile() throws Exception {
    String avroCodec = "snappy";
    localProps.put(StorageSinkConnectorConfig.AVRO_CODEC_CONFIG, avroCodec);
    setUp();
    task = new S3SinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);

    List<SinkRecord> sinkRecords = createRecords(7);
    // Perform write
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    List<S3ObjectSummary> summaries = listObjects(S3_TEST_BUCKET_NAME, "/", s3);
    for(S3ObjectSummary summary: summaries){
      InputStream in = s3.getObject(summary.getBucketName(), summary.getKey()).getObjectContent();
      DatumReader<Object> reader = new GenericDatumReader<>();
      DataFileStream<Object> streamReader = new DataFileStream<>(in, reader);
      // make sure that produced Avro file has proper codec set
      Assert.assertEquals(avroCodec, streamReader.getMetaString(StorageSinkConnectorConfig.AVRO_CODEC_CONFIG));
      streamReader.close();
    }

    long[] validOffsets = {0, 3, 6};
    verify(sinkRecords, validOffsets);
  }


  @Test
  public void testRecoveryWithPartialFile() throws Exception {
    setUp();

    // Upload partial file.
    List<SinkRecord> sinkRecords = createRecords(2);
    byte[] partialData = AvroUtils.putRecords(sinkRecords, format.getAvroData());
    String fileKey = FileUtils.fileKeyToCommit(topicsDir, getDirectory(), TOPIC_PARTITION, 0, EXTENSION, ZERO_PAD_FMT);
    s3.putObject(S3_TEST_BUCKET_NAME, fileKey, new ByteArrayInputStream(partialData), null);

    // Accumulate rest of the records.
    sinkRecords.addAll(createRecords(5, 2));

    task = new S3SinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);
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

    task = new S3SinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);

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
    task = new S3SinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);

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
    task = new S3SinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);

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
    task = new S3SinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);

    List<SinkRecord> sinkRecords = createRecordsInterleaved(7 * context.assignment().size(), 9, context.assignment());
    // Perform write
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long[] validOffsets = {9, 12, 15};
    verify(sinkRecords, validOffsets, context.assignment());
  }

  @Test
  public void testPreCommitOnSizeRotation() throws Exception {
    localProps.put(S3SinkConnectorConfig.FLUSH_SIZE_CONFIG, "3");
    setUp();
    task = new S3SinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);

    List<SinkRecord> sinkRecords1 = createRecordsInterleaved(3 * context.assignment().size(), 0, context.assignment());

    task.put(sinkRecords1);
    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = task.preCommit(null);

    long[] validOffsets1 = {3, 3};
    verifyOffsets(offsetsToCommit, validOffsets1, context.assignment());

    List<SinkRecord> sinkRecords2 = createRecordsInterleaved(2 * context.assignment().size(), 3, context.assignment());

    task.put(sinkRecords2);
    offsetsToCommit = task.preCommit(null);

    // Actual values are null, we set to negative for the verifier.
    long[] validOffsets2 = {-1, -1};
    verifyOffsets(offsetsToCommit, validOffsets2, context.assignment());

    List<SinkRecord> sinkRecords3 = createRecordsInterleaved(context.assignment().size(), 5, context.assignment());

    task.put(sinkRecords3);
    offsetsToCommit = task.preCommit(null);

    long[] validOffsets3 = {6, 6};
    verifyOffsets(offsetsToCommit, validOffsets3, context.assignment());

    List<SinkRecord> sinkRecords4 = createRecordsInterleaved(3 * context.assignment().size(), 6, context.assignment());

    // Include all the records beside the last one in the second partition
    task.put(sinkRecords4.subList(0, 3 * context.assignment().size() - 1));
    offsetsToCommit = task.preCommit(null);

    // Actual values are null, we set to negative for the verifier.
    long[] validOffsets4 = {9, -1};
    verifyOffsets(offsetsToCommit, validOffsets4, context.assignment());

    task.close(context.assignment());
    task.stop();
  }

  @Test
  public void testPreCommitOnSchemaIncompatibilityRotation() throws Exception {
    localProps.put(S3SinkConnectorConfig.FLUSH_SIZE_CONFIG, "2");
    setUp();

    task = new S3SinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);
    List<SinkRecord> sinkRecords = createRecordsWithAlteringSchemas(2, 0);

    // Perform write
    task.put(sinkRecords);

    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = task.preCommit(null);

    long[] validOffsets = {1, -1};

    verifyOffsets(offsetsToCommit, validOffsets, context.assignment());

    task.close(context.assignment());
    task.stop();
  }

  @Test
  public void testPreCommitOnRotateTime() throws Exception {
    // Do not roll on size, only based on time.
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
        PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        TopicPartitionWriterTest.MockedWallclockTimestampExtractor.class.getName()
    );
    partitioner.configure(parsedConfig);

    MockTime time = ((TopicPartitionWriterTest.MockedWallclockTimestampExtractor) partitioner
        .getTimestampExtractor()).time;
    // Bring the clock to present.
    time.sleep(SYSTEM.milliseconds());

    List<SinkRecord> sinkRecords = createRecordsWithTimestamp(
        4,
        0,
        Collections.singleton(new TopicPartition(TOPIC, PARTITION)),
        time
    );

    task = new S3SinkTask(connectorConfig, context, storage, partitioner, format, time);

    // Perform write
    task.put(sinkRecords.subList(0, 3));

    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = task.preCommit(null);

    long[] validOffsets1 = {-1, -1};

    verifyOffsets(offsetsToCommit, validOffsets1, context.assignment());

    // 2 hours
    time.sleep(TimeUnit.HOURS.toMillis(2));

    long[] validOffsets2 = {3, -1};

    // Rotation is only based on rotate.interval.ms, so I need at least one record to trigger flush.
    task.put(sinkRecords.subList(3, 4));
    offsetsToCommit = task.preCommit(null);

    verifyOffsets(offsetsToCommit, validOffsets2, context.assignment());

    task.close(context.assignment());
    task.stop();
  }

  @Test
  public void testPreCommitOnRotateScheduleTime() throws Exception {
    // Do not roll on size, only based on time.
    localProps.put(S3SinkConnectorConfig.FLUSH_SIZE_CONFIG, "1000");
    localProps.put(
        S3SinkConnectorConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG,
        String.valueOf(TimeUnit.HOURS.toMillis(1))
    );
    setUp();

    // Define the partitioner
    TimeBasedPartitioner<?> partitioner = new TimeBasedPartitioner<>();
    parsedConfig.put(PartitionerConfig.PARTITION_DURATION_MS_CONFIG, TimeUnit.DAYS.toMillis(1));
    parsedConfig.put(
        PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        TopicPartitionWriterTest.MockedWallclockTimestampExtractor.class.getName()
    );
    partitioner.configure(parsedConfig);

    MockTime time = ((TopicPartitionWriterTest.MockedWallclockTimestampExtractor) partitioner
        .getTimestampExtractor()).time;
    // Bring the clock to present.
    time.sleep(SYSTEM.milliseconds());

    List<SinkRecord> sinkRecords = createRecordsWithTimestamp(
        3,
        0,
        Collections.singleton(new TopicPartition(TOPIC, PARTITION)),
        time
    );

    task = new S3SinkTask(connectorConfig, context, storage, partitioner, format, time);

    // Perform write
    task.put(sinkRecords);

    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = task.preCommit(null);

    long[] validOffsets1 = {-1, -1};

    verifyOffsets(offsetsToCommit, validOffsets1, context.assignment());

    // 1 hour + 10 minutes
    time.sleep(TimeUnit.HOURS.toMillis(1) + TimeUnit.MINUTES.toMillis(10));

    long[] validOffsets2 = {3, -1};

    // Since rotation depends on scheduled intervals, flush will happen even when no new records
    // are returned.
    task.put(Collections.<SinkRecord>emptyList());
    offsetsToCommit = task.preCommit(null);

    verifyOffsets(offsetsToCommit, validOffsets2, context.assignment());

    task.close(context.assignment());
    task.stop();
  }

  @Test
  public void testPreCommitOnRotateScheduleTimeWithException() throws Exception {
    // Do not roll on size, only based on time.
    localProps.put(S3SinkConnectorConfig.FLUSH_SIZE_CONFIG, "1000");
    localProps.put(
        S3SinkConnectorConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG,
        String.valueOf(TimeUnit.HOURS.toMillis(1))
    );
    setUpWithCommitException();

    // Define the partitioner
    TimeBasedPartitioner<?> partitioner = new TimeBasedPartitioner<>();
    parsedConfig.put(PartitionerConfig.PARTITION_DURATION_MS_CONFIG, TimeUnit.DAYS.toMillis(1));
    parsedConfig.put(
        PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        TopicPartitionWriterTest.MockedWallclockTimestampExtractor.class.getName()
    );
    partitioner.configure(parsedConfig);

    MockTime time = ((TopicPartitionWriterTest.MockedWallclockTimestampExtractor) partitioner
        .getTimestampExtractor()).time;
    // Bring the clock to present.
    time.sleep(SYSTEM.milliseconds());

    List<SinkRecord> sinkRecords = createRecordsWithTimestamp(
        3,
        0,
        Collections.singleton(new TopicPartition(TOPIC, PARTITION)),
        time
    );

    task = new S3SinkTask(connectorConfig, context, storage, partitioner, format, time);

    // Perform write
    task.put(sinkRecords);
    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = task.preCommit(null);

    long[] validOffsets1 = {-1, -1};
    verifyOffsets(offsetsToCommit, validOffsets1, context.assignment());

    // 1 hour + 10 minutes
    time.sleep(TimeUnit.HOURS.toMillis(1) + TimeUnit.MINUTES.toMillis(10));

    // Perform write with no records that will flush the outstanding records due to scheduled
    // interval
    task.put(Collections.<SinkRecord>emptyList());

    // After the exception is caught the connector resets offsets for rewind to the start offset
    long[] validOffsets2 = {0, -1};
    verifyRawOffsets(context.offsets(), validOffsets2, context.assignment());

    // Offsets get rewind and the consumer redelivers the records that failed to commit
    task.put(sinkRecords);

    // But a retry backoff is in effect so these records won't be written to the underlying
    // output stream until the backoff expires. Of course no offset commits happen either
    offsetsToCommit = task.preCommit(null);

    long[] validOffsets3 = {-1, -1};
    verifyOffsets(offsetsToCommit, validOffsets3, context.assignment());

    time.sleep(TimeUnit.MINUTES.toMillis(
        connectorConfig.getLong(S3SinkConnectorConfig.RETRY_BACKOFF_CONFIG)));

    // The backoff expires, the records are written to the underlying output stream. No commits yet
    task.put(Collections.<SinkRecord>emptyList());

    long[] validOffsets4 = {-1, -1};
    verifyOffsets(offsetsToCommit, validOffsets4, context.assignment());

    // 1 hour + 10 minutes
    time.sleep(TimeUnit.HOURS.toMillis(1) + TimeUnit.MINUTES.toMillis(10));

    task.put(Collections.<SinkRecord>emptyList());
    offsetsToCommit = task.preCommit(null);

    long[] validOffsets5 = {3, -1};
    verifyOffsets(offsetsToCommit, validOffsets5, context.assignment());

    task.close(context.assignment());
    task.stop();
  }

  @Test
  public void testRebalance() throws Exception {
    setUp();
    task = new S3SinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);

    List<SinkRecord> sinkRecords = createRecordsInterleaved(7 * context.assignment().size(), 0, context.assignment());
    // Starts with TOPIC_PARTITION and TOPIC_PARTITION2
    Set<TopicPartition> originalAssignment = new HashSet<>(context.assignment());
    Set<TopicPartition> nextAssignment = new HashSet<>();
    nextAssignment.add(TOPIC_PARTITION);
    nextAssignment.add(TOPIC_PARTITION3);

    // Perform write
    task.put(sinkRecords);

    task.close(context.assignment());
    // Set the new assignment to the context
    context.setAssignment(nextAssignment);
    task.open(context.assignment());

    assertEquals(null, task.getTopicPartitionWriter(TOPIC_PARTITION2));
    assertNotNull(task.getTopicPartitionWriter(TOPIC_PARTITION));
    assertNotNull(task.getTopicPartitionWriter(TOPIC_PARTITION3));

    long[] validOffsets = {0, 3, 6};
    verify(sinkRecords, validOffsets, originalAssignment);

    sinkRecords = createRecordsInterleaved(7 * context.assignment().size(), 6, context.assignment());
    // Perform write
    task.put(sinkRecords);
    task.close(nextAssignment);
    task.stop();

    long[] validOffsets1 = {0, 3, 6, 9, 12};
    verify(sinkRecords, validOffsets1, Collections.singleton(TOPIC_PARTITION), true);

    long[] validOffsets2 = {0, 3, 6};
    verify(sinkRecords, validOffsets2, Collections.singleton(TOPIC_PARTITION2), true);

    long[] validOffsets3 = {6, 9, 12};
    verify(sinkRecords, validOffsets3, Collections.singleton(TOPIC_PARTITION3), true);

    List<String> expectedFiles = getExpectedFiles(validOffsets1, TOPIC_PARTITION, EXTENSION);
    expectedFiles.addAll(getExpectedFiles(validOffsets2, TOPIC_PARTITION2, EXTENSION));
    expectedFiles.addAll(getExpectedFiles(validOffsets3, TOPIC_PARTITION3, EXTENSION));
    verifyFileListing(expectedFiles);
  }

  @Test
  public void testProjectBackward() throws Exception {
    localProps.put(S3SinkConnectorConfig.FLUSH_SIZE_CONFIG, "2");
    localProps.put(StorageSinkConnectorConfig.SCHEMA_COMPATIBILITY_CONFIG, "BACKWARD");
    setUp();

    task = new S3SinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);
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

    task = new S3SinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);
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
    localProps.put(StorageSinkConnectorConfig.SCHEMA_COMPATIBILITY_CONFIG, "FORWARD");
    setUp();

    task = new S3SinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);
    // By excluding the first element we get a list starting with record having the new schema.
    List<SinkRecord> sinkRecords = createRecordsWithAlteringSchemas(8, 0).subList(1, 8);

    // Perform write
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long[] validOffsets = {1, 2, 4, 6, 8};
    verify(sinkRecords, validOffsets);
  }

  @Test(expected=ConnectException.class)
  public void testProjectNoVersion() throws Exception {
    localProps.put(S3SinkConnectorConfig.FLUSH_SIZE_CONFIG, "2");
    localProps.put(StorageSinkConnectorConfig.SCHEMA_COMPATIBILITY_CONFIG, "BACKWARD");
    setUp();

    task = new S3SinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);
    List<SinkRecord> sinkRecords = createRecordsNoVersion(1, 0);
    sinkRecords.addAll(createRecordsWithAlteringSchemas(7, 0));

    // Perform write
    try {
      task.put(sinkRecords);
    } finally {
      task.close(context.assignment());
      task.stop();
      long[] validOffsets = {};
      verify(Collections.emptyList(), validOffsets);
    }
  }

  @Test
  public void testCorrectRecordWriterBasic() throws Exception {
    // Test the base-case -- no known embedded extension
    testCorrectRecordWriterHelper("this.is.dir");
  }

  @Test
  public void testCorrectRecordWriterOther() throws Exception {
    // Test with a different embedded extension
    testCorrectRecordWriterHelper("this.is.json.dir");
  }

  @Test
  public void testCorrectRecordWriterThis() throws Exception {
    // Test with our embedded extension
    testCorrectRecordWriterHelper("this.is" + EXTENSION + ".dir");
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

  protected List<SinkRecord> createRecordsWithPrimitive(int size, long startOffset, Set<TopicPartition> partitions) {
    String key = "key";
    Schema schema = Schema.INT32_SCHEMA;
    int record = 12;

    List<SinkRecord> sinkRecords = new ArrayList<>();
    for (TopicPartition tp : partitions) {
      for (long offset = startOffset; offset < startOffset + size; ++offset) {
        sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), Schema.STRING_SCHEMA, key, schema, record, offset));
      }
    }
    return sinkRecords;
  }

  protected List<SinkRecord> createRecordsWithEnums(int size, long startOffset, Set<TopicPartition> partitions) {
    String key = "key";
    Schema schema = createEnumSchema();
    SchemaAndValue valueAndSchema = new SchemaAndValue(schema, "bar");
    List<SinkRecord> sinkRecords = new ArrayList<>();
    for (TopicPartition tp : partitions) {
      for (long offset = startOffset; offset < startOffset + size; ++offset) {
        sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), Schema.STRING_SCHEMA, key, schema, valueAndSchema.value(), offset));
      }
    }
    return sinkRecords;
  }

  public Schema createEnumSchema() {
    // Enums are just converted to strings, original enum is preserved in parameters
    SchemaBuilder builder = SchemaBuilder.string().name("TestEnum");
    builder.parameter(AVRO_TYPE_ENUM, "TestEnum");
    for(String enumSymbol : new String[]{"foo", "bar", "baz"}) {
      builder.parameter(AVRO_TYPE_ENUM+"."+enumSymbol, enumSymbol);
    }
    return builder.build();
  }

  protected List<SinkRecord> createRecordsWithUnion(
      int size,
      long startOffset,
      Set<TopicPartition> partitions
  ) {
    Schema recordSchema1 = SchemaBuilder.struct().name("Test1")
        .field("test", Schema.INT32_SCHEMA).optional().build();
    Schema recordSchema2 = SchemaBuilder.struct().name("io.confluent.Test2")
        .field("test", Schema.INT32_SCHEMA).optional().build();
    Schema schema = SchemaBuilder.struct()
        .name("io.confluent.connect.avro.Union")
        .field("int", Schema.OPTIONAL_INT32_SCHEMA)
        .field("string", Schema.OPTIONAL_STRING_SCHEMA)
        .field("Test1", recordSchema1)
        .field("io.confluent.Test2", recordSchema2)
        .build();

    SchemaAndValue valueAndSchemaInt = new SchemaAndValue(schema, new Struct(schema).put("int", 12));
    SchemaAndValue valueAndSchemaString = new SchemaAndValue(schema, new Struct(schema).put("string", "teststring"));

    Struct schema1Test = new Struct(schema).put("Test1", new Struct(recordSchema1).put("test", 12));
    SchemaAndValue valueAndSchema1 = new SchemaAndValue(schema, schema1Test);

    Struct schema2Test = new Struct(schema).put("io.confluent.Test2", new Struct(recordSchema2).put("test", 12));
    SchemaAndValue valueAndSchema2 = new SchemaAndValue(schema, schema2Test);

    String key = "key";
    List<SinkRecord> sinkRecords = new ArrayList<>();
    for (TopicPartition tp : partitions) {
      for (long offset = startOffset; offset < startOffset + 4 * size;) {
        sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), Schema.STRING_SCHEMA, key, schema, valueAndSchemaInt.value(), offset++));
        sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), Schema.STRING_SCHEMA, key, schema, valueAndSchemaString.value(), offset++));
        sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), Schema.STRING_SCHEMA, key, schema, valueAndSchema1.value(), offset++));
        sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), Schema.STRING_SCHEMA, key, schema, valueAndSchema2.value(), offset++));
      }
    }
    return sinkRecords;
  }

  protected List<SinkRecord> createRecordsWithTimestamp(
      int size,
      long startOffset,
      Set<TopicPartition> partitions,
      Time time
  ) {
    String key = "key";
    Schema schema = createSchema();
    Struct record = createRecord(schema);

    List<SinkRecord> sinkRecords = new ArrayList<>();
    for (TopicPartition tp : partitions) {
      for (long offset = startOffset; offset < startOffset + size; ++offset) {
        sinkRecords.add(new SinkRecord(
            TOPIC,
            tp.partition(),
            Schema.STRING_SCHEMA,
            key,
            schema,
            record,
            offset,
            time.milliseconds(),
            TimestampType.CREATE_TIME
        ));
      }
    }
    return sinkRecords;
  }

  @Override
  protected List<SinkRecord> createGenericRecords(int count, long firstOffset) {
    return createRecordsNoVersion(count, firstOffset);
  }

  protected List<SinkRecord> createRecordsNoVersion(int size, long startOffset) {
    String key = "key";
    Schema schemaNoVersion = SchemaBuilder.struct().name("record")
                                 .field("boolean", Schema.BOOLEAN_SCHEMA)
                                 .field("int", Schema.INT32_SCHEMA)
                                 .field("long", Schema.INT64_SCHEMA)
                                 .field("float", Schema.FLOAT32_SCHEMA)
                                 .field("double", Schema.FLOAT64_SCHEMA)
                                 .build();

    Struct recordNoVersion = new Struct(schemaNoVersion);
    recordNoVersion.put("boolean", true)
        .put("int", 12)
        .put("long", 12L)
        .put("float", 12.2f)
        .put("double", 12.2);

    List<SinkRecord> sinkRecords = new ArrayList<>();
    for (long offset = startOffset; offset < startOffset + size; ++offset) {
      sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schemaNoVersion,
                                     recordNoVersion, offset));
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

  protected List<SinkRecord> createRecordsInterleaved(
      int size,
      long startOffset,
      Set<TopicPartition> partitionSet
  ) {
    String key = "key";
    Schema schema = createSchema();
    Struct record = createRecord(schema);

    Collection<TopicPartition> partitions = sortedPartitions(partitionSet);

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

  protected void verifyContents(List<SinkRecord> expectedRecords, int startIndex, Collection<Object> records) {
    Schema expectedSchema = null;
    for (Object avroRecord : records) {
      if (expectedSchema == null) {
        expectedSchema = expectedRecords.get(startIndex).valueSchema();
      }
      Object expectedValue = SchemaProjector.project(expectedRecords.get(startIndex).valueSchema(),
                                                     expectedRecords.get(startIndex++).value(),
                                                     expectedSchema);
      Object value = format.getAvroData().fromConnectData(expectedSchema, expectedValue);
      // AvroData wraps primitive types so their schema can be included. We need to unwrap
      // NonRecordContainers to just their value to properly handle these types
      if (value instanceof NonRecordContainer) {
        value = ((NonRecordContainer) value).getValue();
      }
      if (avroRecord instanceof Utf8) {
        assertEquals(value, avroRecord.toString());
      } else {
        assertEquals(value, avroRecord);
      }
    }
  }

  protected void verify(List<SinkRecord> sinkRecords, long[] validOffsets) throws IOException {
    verify(sinkRecords, validOffsets, Collections.singleton(new TopicPartition(TOPIC, PARTITION)), false);
  }

  protected void verify(List<SinkRecord> sinkRecords, long[] validOffsets, Set<TopicPartition> partitions)
      throws IOException {
    verify(sinkRecords, validOffsets, partitions, false);
  }

  /**
   * Verify files and records are uploaded appropriately.
   * @param sinkRecords a flat list of the records that need to appear in potentially several files in S3.
   * @param validOffsets an array containing the offsets that map to uploaded files for a topic-partition.
   *                     Offsets appear in ascending order, the difference between two consecutive offsets
   *                     equals the expected size of the file, and last offset in exclusive.
   * @throws IOException
   */
  protected void verify(List<SinkRecord> sinkRecords, long[] validOffsets,
                        Set<TopicPartition> partitions,
                        boolean skipFileListing)
      throws IOException {
    if (!skipFileListing) {
      verifyFileListing(validOffsets, partitions, EXTENSION);
    }

    for (TopicPartition tp : partitions) {
      for (int i = 1, j = 0; i < validOffsets.length; ++i) {
        long startOffset = validOffsets[i - 1];
        long size = validOffsets[i] - startOffset;

        FileUtils.fileKeyToCommit(topicsDir, getDirectory(tp.topic(), tp.partition()), tp, startOffset, EXTENSION, ZERO_PAD_FMT);
        Collection<Object> records = readRecords(topicsDir, getDirectory(tp.topic(), tp.partition()), tp, startOffset,
                                                 EXTENSION, ZERO_PAD_FMT, S3_TEST_BUCKET_NAME, s3);
        assertEquals(size, records.size());
        verifyContents(sinkRecords, j, records);
        j += size;
      }
    }
  }

  protected void verifyOffsets(
      Map<TopicPartition, OffsetAndMetadata> actualOffsets,
      long[] validOffsets,
      Set<TopicPartition> partitionSet
  ) {
    Collection<TopicPartition> partitions = sortedPartitions(partitionSet);
    int i = 0;
    Map<TopicPartition, OffsetAndMetadata> expectedOffsets = new HashMap<>();
    for (TopicPartition tp : partitions) {
      long offset = validOffsets[i++];
      if (offset >= 0) {
        expectedOffsets.put(tp, new OffsetAndMetadata(offset, ""));
      }
    }
    assertEquals(actualOffsets, expectedOffsets);
  }

  protected void verifyRawOffsets(
      Map<TopicPartition, Long> actualOffsets,
      long[] validOffsets,
      Set<TopicPartition> partitionSet
  ) {
    Collection<TopicPartition> partitions = sortedPartitions(partitionSet);
    int i = 0;
    Map<TopicPartition, Long> expectedOffsets = new HashMap<>();
    for (TopicPartition tp : partitions) {
      long offset = validOffsets[i++];
      if (offset >= 0) {
        expectedOffsets.put(tp, offset);
      }
    }
    assertEquals(actualOffsets, expectedOffsets);
  }
}

