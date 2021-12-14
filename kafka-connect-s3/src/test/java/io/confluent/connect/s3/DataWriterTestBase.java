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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

  //
  // -DataWriterTestBase
  //
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
  protected abstract void verify(List<SinkRecord> sinkRecords, long[] validOffsets) throws IOException;

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
  //
  // DataWriterTestBase-
  //

  //@Before should be omitted in order to be able to add properties per test.
  public void setUp() throws Exception {
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

  /**
   *
   * @param topicDir
   * @throws Exception
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

    List<SinkRecord> records = createGenericRecords(6, 1);
    int offset = 0;
    for (SinkRecord record : records) {
      TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());
      String fileKey = FileUtils.fileKeyToCommit(
          topicsDir,
          getDirectory(tp.topic(), tp.partition()),
          tp,
          offset++,
          getFileExtension(),
          ZERO_PAD_FMT
      );
      RecordWriter actualRecordWriterProvider =
          kvProvider.getRecordWriter(connectorConfig, fileKey);
      actualRecordWriterProvider.write(record);
      actualRecordWriterProvider.commit();
      actualRecordWriterProvider.close();
    }

    long[] validOffsets = {0, 1, 2, 3, 4, 5, 6};
    verify(records, validOffsets);
  }

}
