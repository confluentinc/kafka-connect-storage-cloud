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

import io.confluent.connect.s3.format.parquet.ParquetRecordWriterProvider;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.SchemaProjector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.codehaus.plexus.util.StringUtils;
import org.junit.After;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import io.confluent.common.utils.MockTime;
import io.confluent.common.utils.Time;
import io.confluent.connect.s3.format.parquet.ParquetFormat;
import io.confluent.connect.s3.format.parquet.ParquetUtils;
import io.confluent.connect.s3.util.FileUtils;
import io.confluent.connect.storage.StorageSinkConnectorConfig;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;
import io.confluent.kafka.serializers.NonRecordContainer;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

import static org.apache.kafka.common.utils.Time.SYSTEM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class DataWriterParquetTest extends DataWriterTestBase<ParquetFormat> {

  private static final String EXTENSION = ".snappy.parquet";

  public DataWriterParquetTest() {
    super(ParquetFormat.class);
  }

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.putAll(localProps);
    props.put(S3SinkConnectorConfig.FORMAT_CLASS_CONFIG, ParquetFormat.class.getName());
    return props;
  }

  @Override
  protected String getFileExtension() {
    return EXTENSION;
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    localProps.clear();
  }

  @Test
  public void testWithDefaultCodecWriteRecords() throws Exception {
    writeRecordsWithExtensionAndVerifyResult(
        "." + S3SinkConnectorConfig.PARQUET_CODEC_DEFAULT + ".parquet");
  }

  @Test
  public void testUncompressedCompressionWriteRecords() throws Exception {
    localProps.put(S3SinkConnectorConfig.PARQUET_CODEC_CONFIG, "none");
    writeRecordsWithExtensionAndVerifyResult(CompressionCodecName.UNCOMPRESSED.getExtension() + ".parquet");
  }

  @Test
  public void testGzipCompressionWriteRecords() throws Exception {
    localProps.put(S3SinkConnectorConfig.PARQUET_CODEC_CONFIG, "gzip");
    writeRecordsWithExtensionAndVerifyResult(CompressionCodecName.GZIP.getExtension() + ".parquet");
  }

  @Test
  public void testSnappyCompressionWriteRecords() throws Exception {
    localProps.put(S3SinkConnectorConfig.PARQUET_CODEC_CONFIG, "snappy");
    writeRecordsWithExtensionAndVerifyResult(EXTENSION);
  }

  @Test
  public void testRecoveryWithPartialFile() throws Exception {
    setUp();

    // Upload partial file.
    System.setProperty("com.amazonaws.services.s3.disableGetObjectMD5Validation", "true");
    List<SinkRecord> sinkRecords = createRecords(2);
    byte[] partialData = ParquetUtils.putRecords(sinkRecords, format.getAvroData());
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

    List<SinkRecord> sinkRecords = createRecordsInterleaved(7, 0, context.assignment());
    // Perform write
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long[] validOffsets = {0, 3, 6};
    verify(sinkRecords, validOffsets, context.assignment());
  }

  /**
   * Test for parquet writer with null array item(s) arrays
   * @link https://github.com/confluentinc/kafka-connect-storage-cloud/issues/339
   * @throws Exception
   */
  @Test
  public void testWriteRecordsInMultiplePartitionsWithArrayOfOptionalString() throws Exception {
    setUp();
    task = new S3SinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);

    List<SinkRecord> sinkRecords = createRecordsWithArrayOfOptionalString(
        7,
        context.assignment()
    );
    // Perform write
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    // Offsets where each file should start (embedded in the file name)
    long[] validOffsets = {0, 3, 6};
    verify(sinkRecords, validOffsets, context.assignment());
  }

  @Test
  public void testWriteInterleavedRecordsInMultiplePartitionsNonZeroInitialOffset() throws Exception {
    setUp();
    task = new S3SinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);

    List<SinkRecord> sinkRecords = createRecordsInterleaved(7, 9, context.assignment());
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

    List<SinkRecord> sinkRecords1 = createRecordsInterleaved(3, 0, context.assignment());

    task.put(sinkRecords1);
    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = task.preCommit(null);

    Long[] validOffsets1 = {3L, 3L};
    verifyOffsets(offsetsToCommit, validOffsets1, context.assignment());

    List<SinkRecord> sinkRecords2 = createRecordsInterleaved(2, 3, context.assignment());

    task.put(sinkRecords2);
    offsetsToCommit = task.preCommit(null);

    Long[] validOffsets2 = {null, null};
    verifyOffsets(offsetsToCommit, validOffsets2, context.assignment());

    List<SinkRecord> sinkRecords3 = createRecordsInterleaved(1, 5, context.assignment());

    task.put(sinkRecords3);
    offsetsToCommit = task.preCommit(null);

    Long[] validOffsets3 = {6L, 6L};
    verifyOffsets(offsetsToCommit, validOffsets3, context.assignment());

    List<SinkRecord> sinkRecords4 = createRecordsInterleaved(3, 6, context.assignment());

    // Include all the records beside the last one in the second partition
    task.put(sinkRecords4.subList(0, 3 * context.assignment().size() - 1));
    offsetsToCommit = task.preCommit(null);

    Long[] validOffsets4 = {9L, null};
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

    Long[] validOffsets = {1L, null};

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

    Long[] validOffsets1 = {null, null};

    verifyOffsets(offsetsToCommit, validOffsets1, context.assignment());

    // 2 hours
    time.sleep(TimeUnit.HOURS.toMillis(2));

    Long[] validOffsets2 = {3L, null};

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

    MockTime time = new MockTime();
    // Bring the clock to present.
    time.sleep(SYSTEM.milliseconds());

    List<SinkRecord> sinkRecords = createRecords(3, 0);

    task = new S3SinkTask(connectorConfig, context, storage, partitioner, format, time);

    // Perform write
    task.put(sinkRecords);

    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = task.preCommit(null);

    Long[] validOffsets1 = {null, null};

    verifyOffsets(offsetsToCommit, validOffsets1, context.assignment());

    // 1 hour + 10 minutes
    time.sleep(TimeUnit.HOURS.toMillis(1) + TimeUnit.MINUTES.toMillis(10));

    Long[] validOffsets2 = {3L, null};

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

    List<SinkRecord> sinkRecords = createRecordsInterleaved(7, 0, context.assignment());
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

    assertNull(task.getTopicPartitionWriter(TOPIC_PARTITION2));
    assertNotNull(task.getTopicPartitionWriter(TOPIC_PARTITION));
    assertNotNull(task.getTopicPartitionWriter(TOPIC_PARTITION3));

    long[] validOffsets = {0, 3, 6};
    verify(sinkRecords, validOffsets, originalAssignment);

    sinkRecords = createRecordsInterleaved(7, 6, context.assignment());
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
      verify(Collections.<SinkRecord>emptyList(), validOffsets);
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

  @Test
  public void testCorrectRecordWriterPartialThisA() throws Exception {
    // Test with our embedded extension
    testCorrectRecordWriterHelper("this.is.snappy.dir");
  }

  @Test
  public void testCorrectRecordWriterPartialThisB() throws Exception {
    // Test with our embedded extension
    testCorrectRecordWriterHelper("this.is.parquet.dir");
  }

  class SchemaConfig {
    public String name;
    public boolean optionalItems = false;
    public boolean regularItems = false;
    public boolean mapRegular = false;
    public boolean mapOptional = false;
    public SchemaConfig nested = null;
    public SchemaConfig nestedArray = null;

    public SchemaConfig() {}

    public SchemaConfig(
        String name,
        boolean regularItems,
        boolean optionalItems,
        boolean mapRegular,
        boolean mapOptional,
        SchemaConfig nested,
        SchemaConfig nestedArray
    ) {
      this.name = name;
      this.optionalItems = optionalItems;
      this.regularItems = regularItems;
      this.mapRegular = mapRegular;
      this.mapOptional = mapOptional;
      this.nested = nested;
      this.nestedArray = nestedArray;
    }

    public boolean hasOptionalItems() {
      if (optionalItems || mapOptional) {
        return true;
      }
      if (nested != null && nested.hasOptionalItems()) {
        return true;
      }
      return nestedArray != null && nestedArray.hasOptionalItems();
    }

    private Schema create() {
      SchemaBuilder builder = SchemaBuilder.struct();
      if (StringUtils.isNotBlank(name)) {
        builder.name(name);
      }
      if (regularItems) {
        builder.field("regular_items", SchemaBuilder.array(Schema.STRING_SCHEMA).build());
      }
      if (optionalItems) {
        builder.field("optional_items", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).build());
      }
      if (mapRegular) {
        builder.field("regular_map", SchemaBuilder.map(
            Schema.STRING_SCHEMA,
            SchemaBuilder.array(Schema.STRING_SCHEMA).build()
        ).build());
      }
      if (mapOptional) {
        builder.field("optional_map", SchemaBuilder.map(
            Schema.STRING_SCHEMA,
            SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).build()
        ).build());
      }
      if (this.nested != null) {
        builder.field("nested", nested.create());
      }
      if (this.nestedArray != null) {
        builder.field("nested_array", SchemaBuilder.array(nestedArray.create()));
      }
      return builder.build();
    }

  }

  /**
   * Tests ParquetRecordWriterProvider::schemaHasArrayOfOptionalItems()
   *
   * Test permutations of schemas and schema nesting with and without optional array items
   * somewhere within the schema.
   */
  @Test
  public void testSchemaHasArrayOfOptionalItems() {
    for (int regularItems = 0; regularItems < 2; ++regularItems) {
      for (int optionalItems = 0; optionalItems < 2; ++optionalItems) {
        for (int mapRegular = 0; mapRegular < 2; ++mapRegular) {
          for (int mapOptional = 0; mapOptional < 2; ++mapOptional) {
            for (int hasNested = 0; hasNested < 2; ++hasNested) {
              for (int hasNestedArray = 0; hasNestedArray < 2; ++hasNestedArray) {
                SchemaConfig conf = new SchemaConfig();
                SchemaConfig nested = null, nestedArray = null;
                if (hasNested != 0) {
                    // Invert tests so as to hit in isolation
                    nested = new SchemaConfig(
                        "nested_schema",
                        regularItems == 0,
                        optionalItems == 0,
                        mapRegular == 0,
                        mapOptional == 0,
                        /*nested=*/ null,
                        /*nestedArray=*/null
                    );
                }
                if (hasNestedArray != 0) {
                  // Invert tests so as to hit in isolation
                  nestedArray = new SchemaConfig(
                      "array_of_schema",
                      regularItems == 0,
                      optionalItems == 0,
                      mapRegular == 0,
                      mapOptional == 0,
                      /*nested=*/ null,
                      /*nestedArray=*/null
                  );
                }
                conf.nested = new SchemaConfig(
                    "nested_schema",
                    regularItems != 0,
                    optionalItems != 0,
                    mapRegular != 0,
                    mapOptional != 0,
                    nested,
                    nestedArray
                );
                Schema schema = conf.create();
                if (schema.fields().size() == 0) {
                  // Base case of everything is false
                  continue;
                }
                final boolean hasArrayOptional =
                    ParquetRecordWriterProvider.schemaHasArrayOfOptionalItems(
                      schema,
                      /*seenSchemas=*/null
                  );
                final boolean shouldHaveArrayOptional = conf.hasOptionalItems();
                assertEquals(hasArrayOptional, shouldHaveArrayOptional);
              }
            }
          }
        }
      }
    }
  }

  private List<SinkRecord> createRecordsWithArrayOfOptionalString(
      int size,
      Set<TopicPartition> partitions
  ) {
    SchemaConfig conf = new SchemaConfig();
    conf.regularItems = true;
    conf.optionalItems = true;
    conf.nested = new SchemaConfig(
        "nested_schema",
        true,
        true,
        false,
        false,
        null,
        null
    );

    Schema schema = conf.create();

    List<SinkRecord> sinkRecords = new ArrayList<>();
    for (TopicPartition tp : partitions) {
      // We're going to alternate internal and external array elements as null
      boolean hasString = true;
      for (long offset = 0; offset < size; ++offset) {
        LinkedList<String> optionalList = new LinkedList<>();
        // Alternate edge and internal as null items
        optionalList.add(hasString ? "item-1" : null);
        optionalList.add(hasString ? null : "item-2");
        optionalList.add(hasString ? "item-3" : null);
        Struct struct = new Struct(schema)
            .put("optional_items", optionalList)
            .put("regular_items", ImmutableList.of("reg-1", "reg-2"))
            .put(
                // Nested struct
                "nested",
                new Struct(schema.field("nested").schema())
                    // Nested option string array
                    .put("optional_items", optionalList.clone())
                    // Nested regular string array
                    .put("regular_items", ImmutableList.of("reg-1", "reg-2"))
            );
        sinkRecords.add(
            new SinkRecord(
                TOPIC,
                tp.partition(),
                Schema.STRING_SCHEMA,
                "key",
                schema,
                struct,
                offset
            )
        );
        hasString = !hasString;
      }
    }
    return sinkRecords;
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
    for (long offset = startOffset; offset < startOffset + size; ++offset) {
      for (TopicPartition tp : partitions) {
        sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), Schema.STRING_SCHEMA, key, schema, record, offset));
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

  @Override
  protected List<SinkRecord> createGenericRecords(int count, long firstOffset) {
    return createRecords(count, firstOffset);
  }

  @Override
  protected void verify(
      List<SinkRecord> sinkRecords,
      long[] validOffsets,
      Set<TopicPartition> partitions
  ) throws IOException {
    verify(sinkRecords, validOffsets, partitions, false);
  }

  protected void verify(List<SinkRecord> sinkRecords, long[] validOffsets, String extension) throws IOException {
    verify(sinkRecords, validOffsets, Collections.singleton(new TopicPartition(TOPIC, PARTITION)), false, extension);
  }

  protected void verify(List<SinkRecord> sinkRecords, long[] validOffsets) throws IOException {
    verify(sinkRecords, validOffsets, Collections.singleton(new TopicPartition(TOPIC, PARTITION)), false);
  }

  protected void verify(List<SinkRecord> sinkRecords, long[] validOffsets, Set<TopicPartition> partitions,
                        boolean skipFileListing)
          throws IOException {
    verify(sinkRecords, validOffsets, partitions, skipFileListing, EXTENSION);
  }

  /**
   * Verify files and records are uploaded appropriately.
   * @param sinkRecords a flat list of the records that need to appear in potentially several files in S3.
   * @param validOffsets an array containing the offsets that map to uploaded files for a topic-partition.
   *                     Offsets appear in ascending order, the difference between two consecutive offsets
   *                     equals the expected size of the file, and last offset in exclusive.
   * @throws IOException throw this exception from readRecords
   */
  protected void verify(List<SinkRecord> sinkRecords, long[] validOffsets, Set<TopicPartition> partitions,
                        boolean skipFileListing, String extension)
          throws IOException {
    if (!skipFileListing) {
      verifyFileListing(validOffsets, partitions, extension);
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

  protected String getDirectory() {
    return getDirectory(TOPIC, PARTITION);
  }

  protected String getDirectory(String topic, int partition) {
    String encodedPartition = "partition=" + partition;
    return partitioner.generatePartitionedPath(topic, encodedPartition);
  }

  protected void verifyOffsets(Map<TopicPartition, OffsetAndMetadata> actualOffsets, Long[] validOffsets,
                               Set<TopicPartition> partitionSet) {
    Collection<TopicPartition> partitions = sortedPartitions(partitionSet);
    int i = 0;
    Map<TopicPartition, OffsetAndMetadata> expectedOffsets = new HashMap<>();
    for (TopicPartition tp : partitions) {
      Long offset = validOffsets[i++];
      if (offset != null) {
        expectedOffsets.put(tp, new OffsetAndMetadata(offset, ""));
      }
    }
    assertEquals(expectedOffsets, actualOffsets);
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

  protected void writeRecordsWithExtensionAndVerifyResult(String extension) throws Exception {
    setUp();
    task = new S3SinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);

    List<SinkRecord> sinkRecords = createRecords(7);
    // Perform write
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long[] validOffsets = {0, 3, 6};
    verify(sinkRecords, validOffsets, extension);
  }
}
