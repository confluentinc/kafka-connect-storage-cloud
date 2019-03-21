package io.confluent.connect.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.internal.SkipMd5CheckStrategy;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.confluent.connect.s3.format.orc.OrcFormat;
import io.confluent.connect.s3.format.orc.OrcUtils;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.s3.util.FileUtils;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.Partitioner;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
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

  protected void verifyContents(List<SinkRecord> expectedRecords, int startIndex, Collection<Object> records) {
    Iterator<Object> iterator = records.iterator();
    while (iterator.hasNext()){
      SinkRecord sinkRecord = expectedRecords.get(startIndex++);
      Object actualData = iterator.next();
      OrcUtils.assertRecordMatches(sinkRecord, actualData);
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
}
