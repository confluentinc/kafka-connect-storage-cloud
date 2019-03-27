package io.confluent.connect.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.internal.SkipMd5CheckStrategy;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.confluent.common.utils.MockTime;
import io.confluent.common.utils.Time;
import io.confluent.connect.s3.format.orc.OrcFormat;
import io.confluent.connect.s3.format.orc.OrcTestUtils;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.s3.util.FileUtils;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.junit.After;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.utils.Time.SYSTEM;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class DataWriterOrcTest extends TestWithMockedS3 {

  private static final String ZERO_PAD_FMT = "%010d";
  private final String extension = ".orc";
  protected S3Storage storage;
  protected AmazonS3 s3;
  OrcFormat format;
  Partitioner<FieldSchema> partitioner;
  S3SinkTask task;
  Map<String, String> localProps = new HashMap<>();
  private String prevMd5Prop = null;

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.putAll(localProps);
    return props;
  }

  //@Before should be omitted in order to be able to add properties per test.
  public void setUp() throws Exception {
    super.setUp();

    s3 = PowerMockito.spy(newS3Client(connectorConfig));

    storage = new S3Storage(connectorConfig, url, S3_TEST_BUCKET_NAME, s3);

    partitioner = new DefaultPartitioner<>();
    partitioner.configure(parsedConfig);
    format = new OrcFormat(storage);

    s3.createBucket(S3_TEST_BUCKET_NAME);
    assertTrue(s3.doesBucketExist(S3_TEST_BUCKET_NAME));

    // Workaround to avoid AWS S3 client failing due to apparently incorrect S3Mock digest
    prevMd5Prop = System.getProperty(
        SkipMd5CheckStrategy.DISABLE_GET_OBJECT_MD5_VALIDATION_PROPERTY
    );
    System.setProperty(SkipMd5CheckStrategy.DISABLE_GET_OBJECT_MD5_VALIDATION_PROPERTY, "true");
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

    List<SinkRecord> sinkRecords = createRecords(7, 0);
    // Perform write
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long[] validOffsets = {0, 3, 6};
    verify(sinkRecords, validOffsets);
  }

  @Test
  public void testWriteAllTypeRecords() throws Exception {
    setUp();
    task = new S3SinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);

    List<SinkRecord> sinkRecords = createAllTypeRecords(3);
    // Perform write
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long[] validOffsets = {0, 3};
    verify(sinkRecords, validOffsets);
  }

  @Test
  public void testCompressFile() throws Exception {
    String codec = "SNAPPY";
    localProps.put(S3SinkConnectorConfig.ORC_CODEC_CONFIG, codec);
    setUp();
    task = new S3SinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);

    List<SinkRecord> sinkRecords = createRecords(7, 0);
    // Perform write
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    List<S3ObjectSummary> summaries = listObjects(S3_TEST_BUCKET_NAME, "/", s3);
    for (S3ObjectSummary summary : summaries) {
      File tempFile = Files.createTempFile("test_orc", ".orc").toFile();
      try {
        s3.getObject(new GetObjectRequest(summary.getBucketName(), summary.getKey()), tempFile);
        Reader reader = OrcFile.createReader(new Path(tempFile.getAbsolutePath()), OrcFile.readerOptions(new Configuration()));
        CompressionKind compressionKind = reader.getCompressionKind();
        assertEquals(CompressionKind.SNAPPY, compressionKind);
      } finally {
        org.apache.commons.io.FileUtils.deleteQuietly(tempFile);
      }
    }

    long[] validOffsets = {0, 3, 6};
    verify(sinkRecords, validOffsets);
  }

  @Test
  public void testWriteRecordsWithInnerStruct() throws Exception {
    setUp();
    task = new S3SinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);
    List<SinkRecord> sinkRecords = createRecordsWithUnion(7, 0, Collections.singleton(new TopicPartition(TOPIC, PARTITION)));

    // Perform write
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long[] validOffsets = {0, 3, 6, 9, 12, 15, 18, 21, 24, 27};
    verify(sinkRecords, validOffsets);
  }

  @Test
  public void testRecoveryWithPartialFile() throws Exception {
    setUp();

    // Upload partial file.
    List<SinkRecord> sinkRecords = createRecords(2, 0);
    byte[] partialData = OrcTestUtils.putRecords(sinkRecords);
    String fileKey = FileUtils.fileKeyToCommit(topicsDir, getDirectory(), TOPIC_PARTITION, 0, extension, ZERO_PAD_FMT);
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

    List<SinkRecord> sinkRecords = createRecords(11000, 0);

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
    TimeBasedPartitioner<FieldSchema> partitioner = new TimeBasedPartitioner<>();
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
    TimeBasedPartitioner<FieldSchema> partitioner = new TimeBasedPartitioner<>();
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

    List<String> expectedFiles = getExpectedFiles(validOffsets1, TOPIC_PARTITION);
    expectedFiles.addAll(getExpectedFiles(validOffsets2, TOPIC_PARTITION2));
    expectedFiles.addAll(getExpectedFiles(validOffsets3, TOPIC_PARTITION3));
    verifyFileListing(expectedFiles);
  }

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

  protected void verify(List<SinkRecord> sinkRecords, long[] validOffsets) throws IOException {
    verify(sinkRecords, validOffsets, Collections.singleton(new TopicPartition(TOPIC, PARTITION)), false);
  }

  protected void verify(List<SinkRecord> sinkRecords, long[] validOffsets, Set<TopicPartition> partitions)
      throws IOException {
    verify(sinkRecords, validOffsets, partitions, false);
  }

  /**
   * Verify files and records are uploaded appropriately.
   *
   * @param sinkRecords  a flat list of the records that need to appear in potentially several files in S3.
   * @param validOffsets an array containing the offsets that map to uploaded files for a topic-partition.
   *                     Offsets appear in ascending order, the difference between two consecutive offsets
   *                     equals the expected size of the file, and last offset in exclusive.
   * @throws IOException
   */
  protected void verify(List<SinkRecord> sinkRecords, long[] validOffsets, Set<TopicPartition> partitions,
                        boolean skipFileListing)
      throws IOException {
    if (!skipFileListing) {
      verifyFileListing(validOffsets, partitions);
    }

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

  protected void verifyOffsets(Map<TopicPartition, OffsetAndMetadata> actualOffsets, long[] validOffsets,
                               Set<TopicPartition> partitions) {
    int i = 0;
    Map<TopicPartition, OffsetAndMetadata> expectedOffsets = new HashMap<>();
    for (TopicPartition tp : partitions) {
      long offset = validOffsets[i++];
      if (offset >= 0) {
        expectedOffsets.put(tp, new OffsetAndMetadata(offset, ""));
      }
    }
    assertTrue(Objects.equals(actualOffsets, expectedOffsets));
  }

  protected void verifyFileListing(long[] validOffsets, Set<TopicPartition> partitions) throws IOException {
    List<String> expectedFiles = new ArrayList<>();
    for (TopicPartition tp : partitions) {
      expectedFiles.addAll(getExpectedFiles(validOffsets, tp));
    }
    verifyFileListing(expectedFiles);
  }

  protected void verifyFileListing(List<String> expectedFiles) throws IOException {
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

  protected void verifyContents(List<SinkRecord> expectedRecords, int startIndex, Collection<Object> fileData) {
    Iterator<Object> iterator = fileData.iterator();
    while (iterator.hasNext()) {
      SinkRecord sinkRecord = expectedRecords.get(startIndex++);
      Object actualData = iterator.next();
      OrcTestUtils.assertRecordMatches(sinkRecord, actualData);
    }
  }

  protected String getDirectory() {
    return getDirectory(TOPIC, PARTITION);
  }

  protected String getDirectory(String topic, int partition) {
    String encodedPartition = "partition=" + String.valueOf(partition);
    return partitioner.generatePartitionedPath(topic, encodedPartition);
  }

  protected List<String> getExpectedFiles(long[] validOffsets, TopicPartition tp) {
    List<String> expectedFiles = new ArrayList<>();
    for (int i = 1; i < validOffsets.length; ++i) {
      long startOffset = validOffsets[i - 1];
      expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, getDirectory(tp.topic(), tp.partition()), tp, startOffset,
          extension, ZERO_PAD_FMT));
    }
    return expectedFiles;
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

  private List<SinkRecord> createAllTypeRecords(int size) {
    Schema schema = SchemaBuilder.struct()
        .field("int8", SchemaBuilder.int8().defaultValue((byte) 2).doc("int8 field").build())
        .field("int16", Schema.INT16_SCHEMA)
        .field("int32", Schema.INT32_SCHEMA)
        .field("int64", Schema.INT64_SCHEMA)
        .field("float32", Schema.FLOAT32_SCHEMA)
        .field("float64", Schema.FLOAT64_SCHEMA)
        .field("boolean", Schema.BOOLEAN_SCHEMA)
        .field("string", Schema.STRING_SCHEMA)
        .field("bytes", Schema.BYTES_SCHEMA)
        .field("timestamp", Timestamp.SCHEMA)
        .field("array", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
        .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional().build())
        .field("mapNonStringKeys",
            SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA).build())
        .build();
    Struct struct = new Struct(schema)
        .put("int8", (byte) 12)
        .put("int16", (short) 12)
        .put("int32", 12)
        .put("int64", 12L)
        .put("float32", 12.2f)
        .put("float64", 12.2)
        .put("boolean", true)
        .put("string", "foo")
        .put("timestamp", new Date())
        .put("bytes", ByteBuffer.wrap("foo".getBytes()));

    List<SinkRecord> sinkRecords = new ArrayList<>();
    for (TopicPartition tp : Collections.singleton(new TopicPartition(TOPIC, PARTITION))) {
      for (long offset = 0; offset < size; ++offset) {
        if (offset % 2 == 0) {
          struct.put("array", Arrays.asList("a", "b", "c"));
          Map<String, String> strMap = new HashMap<>();
          strMap.put("field", "1");
          struct.put("map", strMap);
        }
        sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), Schema.STRING_SCHEMA, "key", schema, struct, offset));
      }
    }
    return sinkRecords;
  }

  protected List<SinkRecord> createRecordsWithUnion(
      int size,
      long startOffset,
      Set<TopicPartition> partitions
  ) {
    Schema recordSchema1 = SchemaBuilder.struct().name("InnerStruct1")
        .field("test", Schema.INT32_SCHEMA).optional().build();
    Schema recordSchema2 = SchemaBuilder.struct().name("InnerStruct2")
        .field("test", Schema.INT32_SCHEMA).optional().build();
    Schema schema = SchemaBuilder.struct()
        .name("InnerStructTest")
        .field("int", Schema.OPTIONAL_INT32_SCHEMA)
        .field("string", Schema.OPTIONAL_STRING_SCHEMA)
        .field("InnerStruct1", recordSchema1)
        .field("InnerStruct2", recordSchema2)
        .build();

    SchemaAndValue valueAndSchemaInt = new SchemaAndValue(schema, new Struct(schema).put("int", 12));
    SchemaAndValue valueAndSchemaString = new SchemaAndValue(schema, new Struct(schema).put("string", "teststring"));

    Struct schema1Test = new Struct(schema).put("InnerStruct1", new Struct(recordSchema1).put("test", 12));
    SchemaAndValue valueAndSchema1 = new SchemaAndValue(schema, schema1Test);

    Struct schema2Test = new Struct(schema).put("InnerStruct2", new Struct(recordSchema2).put("test", 12));
    SchemaAndValue valueAndSchema2 = new SchemaAndValue(schema, schema2Test);

    String key = "key";
    List<SinkRecord> sinkRecords = new ArrayList<>();
    for (TopicPartition tp : partitions) {
      for (long offset = startOffset; offset < startOffset + 4 * size; ) {
        sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), Schema.STRING_SCHEMA, key, schema, valueAndSchemaInt.value(), offset++));
        sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), Schema.STRING_SCHEMA, key, schema, valueAndSchemaString.value(), offset++));
        sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), Schema.STRING_SCHEMA, key, schema, valueAndSchema1.value(), offset++));
        sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), Schema.STRING_SCHEMA, key, schema, valueAndSchema2.value(), offset++));
      }
    }
    return sinkRecords;
  }
}
