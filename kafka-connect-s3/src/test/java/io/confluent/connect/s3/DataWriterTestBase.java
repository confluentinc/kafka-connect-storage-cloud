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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.confluent.connect.s3.format.KeyValueHeaderRecordWriterProvider;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.s3.util.FileUtils;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.Partitioner;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.powermock.api.mockito.PowerMockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class DataWriterTestBase<
    FORMAT extends Format<S3SinkConnectorConfig, String>
  > extends TestWithMockedS3 {

  protected static final String ZERO_PAD_FMT = "%010d";

  protected S3Storage storage;
  protected AmazonS3 s3;
  protected Partitioner<?> partitioner;
  protected S3SinkTask task;
  protected Map<String, String> localProps = new HashMap<>();

  // The format class
  protected FORMAT format;
  private final Class<FORMAT> clazz;

  /**
   * Return the default file extension
   *
   * @return The default file extension
   */
  protected abstract String getFileExtension();

  /**
   * Perform a simple verify that the S3 bucket has the given records (and only the given records)
   *
   * @param sinkRecords Sink records to verify ewxist on S3 storage
   * @param validOffsets List of valid offsets for these recores
   *
   * @throws IOException Thrown upon an IO exception,m such as S3 unreachable
   */
  protected abstract void verify(
      List<SinkRecord> sinkRecords,
      long[] validOffsets,
      Set<TopicPartition> partitions
  ) throws IOException;

  /**
   * Create a generic list of records, usually for purposes other than validating the
   * correctness of the records' structure themselves.  We don't care whether they do or don't
   * have a schema, for instance -- just that we can verify that they are records with some
   * comparable value for correctness validation purposes.
   *
   * @param count Number of records to create
   * @param firstOffset First valid offset for these records
   *
   * @return A list of the newly-created records
   */
  protected abstract List<SinkRecord> createGenericRecords(int count, long firstOffset);

  /**
   * Return the S3 "directory" that a given topic and partition number are expected to
   * reside.
   * @param topic The topic of the records
   * @param partition The partition number
   *
   * @return A string representing the S3 "directory"
   */
  protected abstract String getDirectory(String topic, int partition);

  /**
   * Constructor
   *
   * @param clazz  A Class<Format type> object for the purpose of creating
   *               a new format object.
   */
  protected DataWriterTestBase(final Class<FORMAT> clazz) {
    this.clazz = clazz;
  }

  //@Before should be omitted in order to be able to add properties per test.
  public void setUp() throws Exception {
    localProps.put(S3SinkConnectorConfig.FORMAT_CLASS_CONFIG, clazz.getName());

    super.setUp();

    s3 = PowerMockito.spy(newS3Client(connectorConfig));

    storage = new S3Storage(connectorConfig, url, S3_TEST_BUCKET_NAME, s3);

    partitioner = new DefaultPartitioner<>();
    partitioner.configure(parsedConfig);
    format = clazz.getDeclaredConstructor(S3Storage.class).newInstance(storage);
    assertEquals(format.getClass().getName(), clazz.getName());

    s3.createBucket(S3_TEST_BUCKET_NAME);
    assertTrue(s3.doesBucketExistV2(S3_TEST_BUCKET_NAME));
  }

  protected List<String> getExpectedFiles(long[] validOffsets, TopicPartition tp, String extension) {
    List<String> expectedFiles = new ArrayList<>();
    for (int i = 1; i < validOffsets.length; ++i) {
      long startOffset = validOffsets[i - 1];
      expectedFiles.add(FileUtils.fileKeyToCommit(
          topicsDir,
          getDirectory(tp.topic(), tp.partition()),
          tp,
          startOffset,
          extension, ZERO_PAD_FMT));
    }
    return expectedFiles;
  }

  protected List<String> getExpectedFiles(long[] validOffsets,
                                          Collection<TopicPartition> partitions,
                                          String extension) {
    List<String> expectedFiles = new ArrayList<>();
    for (TopicPartition tp : partitions) {
      expectedFiles.addAll(getExpectedFiles(validOffsets, tp, extension));
    }
    return expectedFiles;
  }

  protected void verifyFileListing(List<String> expectedFiles) throws IOException {
    List<S3ObjectSummary> summaries = listObjects(S3_TEST_BUCKET_NAME, null, s3);
    List<String> actualFiles = new ArrayList<>();
    for (S3ObjectSummary summary : summaries) {
      String fileKey = summary.getKey();
      actualFiles.add(fileKey);
    }

    assertThat(actualFiles).containsExactlyInAnyOrderElementsOf(expectedFiles);
  }

  protected void verifyFileListing(long[] validOffsets, Collection<TopicPartition> partitions,
                                   String extension) throws IOException {
    List<String> expectedFiles = getExpectedFiles(validOffsets, partitions, extension);
    verifyFileListing(expectedFiles);
  }

  /**
   * Test that what ends up on S3 have the correct files names, given the topic
   * dir which may have anything in it, such as the file extension itself, etc.
   * @param topicDir The directory to save the records
   * @throws Exception On test failure
   */
  protected void testCorrectRecordWriterHelper(
      final String topicDir
  ) throws Exception {
    localProps.put(StorageCommonConfig.TOPICS_DIR_CONFIG, topicDir);
    setUp();

    task = new S3SinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);

    RecordWriterProvider<S3SinkConnectorConfig> keyValueRecordWriterProvider =
        task.newRecordWriterProvider(connectorConfig);
    assertEquals(keyValueRecordWriterProvider.getExtension(), getFileExtension());
    assertThat(keyValueRecordWriterProvider).isInstanceOf(KeyValueHeaderRecordWriterProvider.class);
    KeyValueHeaderRecordWriterProvider kvProvider =
        (KeyValueHeaderRecordWriterProvider)keyValueRecordWriterProvider;

    final int offsetCount = 1;
    //final int offsetCount = 6;

    Set<TopicPartition> partitions = new HashSet<>();
    List<SinkRecord> records = createGenericRecords(offsetCount, 1);
    List<String> expectedFiles = new ArrayList<>();
    Map<Integer, Long> offsetMap = new HashMap<>();
    for (SinkRecord record : records) {
      TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());
      partitions.add(tp);
      Long offset = offsetMap.getOrDefault(tp.partition(), 0L);
      offsetMap.put(tp.partition(), offset + 1);
      String fileKey = FileUtils.fileKeyToCommit(
          topicsDir,
          getDirectory(tp.topic(), tp.partition()),
          tp,
          offset,
          getFileExtension(),
          ZERO_PAD_FMT
      );
      expectedFiles.add(fileKey);
      RecordWriter actualRecordWriterProvider =
          kvProvider.getRecordWriter(connectorConfig, fileKey);
      actualRecordWriterProvider.write(record);
      actualRecordWriterProvider.commit();
      actualRecordWriterProvider.close();
    }

    // Where is std::iota() in java?
    long[] validOffsets = new long[offsetCount + 1];
    for (int i = 0; i <= offsetCount; ++i) {
      validOffsets[i] = i;
    }

    // Sanity check to make sure we generated the correct names along the way against
    // what it is going to check for later (not checking against what's actually going to
    // be on S3, which is another check altogether).
    assertThat(expectedFiles).containsExactlyInAnyOrderElementsOf(
        getExpectedFiles(validOffsets, partitions, getFileExtension())
    );

    // Now check what actually made it to S3
    verify(records, validOffsets, partitions);
  }

  /**
   * Sort a collection of TopicPartition objects by the partition number.
   * Expectation is that there are not duplicate partition numbers in the set
   * (i.e. only one topic).
   * @param partitions Collection of TopicPartition objects
   * @return Coll3ection of TopicPartition sorted by TopicPartition::partition()
   */
  protected static Collection<TopicPartition> sortedPartitions(
      Collection<TopicPartition> partitions
  ) {
    // Sort by partition #
    TreeMap<Integer, TopicPartition> map = new TreeMap<>();
    for (TopicPartition partition : partitions) {
      if (map.containsKey(partition.partition())) {
        throw new RuntimeException("A duplicate partition number not expected.");
      }
      map.put(partition.partition(), partition);
    }
    return  map.values();
  }

}
