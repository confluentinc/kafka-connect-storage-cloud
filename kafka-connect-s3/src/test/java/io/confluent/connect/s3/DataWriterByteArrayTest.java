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

import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.confluent.connect.s3.format.bytearray.ByteArrayFormat;
import io.confluent.connect.s3.storage.CompressionType;
import io.confluent.connect.s3.storage.S3OutputStream;
import io.confluent.connect.s3.util.FileUtils;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.Deflater;

import static org.junit.Assert.assertArrayEquals;

public class DataWriterByteArrayTest extends DataWriterTestBase<ByteArrayFormat> {

  private static final String DEFAULT_EXTENSION = ".bin";

  private ByteArrayConverter converter;

  public DataWriterByteArrayTest() {
    super(ByteArrayFormat.class);
  }

  @Override
  protected String getFileExtension() {
    return DEFAULT_EXTENSION;
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
    converter = new ByteArrayConverter();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    localProps.clear();
  }

  @Test
  public void testBufferOverflowFix() throws Exception {
    localProps.put(S3SinkConnectorConfig.FORMAT_CLASS_CONFIG, ByteArrayFormat.class.getName());
    setUp();
    PowerMockito.doReturn(5).when(connectorConfig).getPartSize();
    S3OutputStream out = new S3OutputStream(S3_TEST_BUCKET_NAME, connectorConfig, s3);
    out.write(new byte[]{65,66,67,68,69});
    out.write(70);
  }

  @Test
  public void testNoSchema() throws Exception {
    localProps.put(S3SinkConnectorConfig.FORMAT_CLASS_CONFIG, ByteArrayFormat.class.getName());
    setUp();
    task = new S3SinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);

    List<SinkRecord> sinkRecords = createByteArrayRecordsWithoutSchema(7 * context.assignment().size(), 0, context.assignment());
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long[] validOffsets = {0, 3, 6};
    verify(sinkRecords, validOffsets, context.assignment(), DEFAULT_EXTENSION);
  }

  @Test
  public void testGzipCompression() throws Exception {
    CompressionType compressionType = CompressionType.GZIP;
    localProps.put(S3SinkConnectorConfig.FORMAT_CLASS_CONFIG, ByteArrayFormat.class.getName());
    localProps.put(S3SinkConnectorConfig.COMPRESSION_TYPE_CONFIG, compressionType.name);
    setUp();
    task = new S3SinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);

    List<SinkRecord> sinkRecords = createByteArrayRecordsWithoutSchema(7 * context.assignment().size(), 0, context.assignment());
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long[] validOffsets = {0, 3, 6};
    verify(sinkRecords, validOffsets, context.assignment(), DEFAULT_EXTENSION + ".gz");
  }

  @Test
  public void testBestGzipCompression() throws Exception {
    CompressionType compressionType = CompressionType.GZIP;
    localProps.put(S3SinkConnectorConfig.FORMAT_CLASS_CONFIG, ByteArrayFormat.class.getName());
    localProps.put(S3SinkConnectorConfig.COMPRESSION_TYPE_CONFIG, compressionType.name);
    localProps.put(S3SinkConnectorConfig.COMPRESSION_LEVEL_CONFIG, String.valueOf(Deflater.BEST_COMPRESSION));
    setUp();
    task = new S3SinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);

    List<SinkRecord> sinkRecords = createByteArrayRecordsWithoutSchema(7 * context.assignment().size(), 0, context.assignment());
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long[] validOffsets = {0, 3, 6};
    verify(sinkRecords, validOffsets, context.assignment(), DEFAULT_EXTENSION + ".gz");
  }

  @Test
  public void testCustomExtensionAndLineSeparator() throws Exception {
    String extension = ".customExtensionForTest";
    localProps.put(S3SinkConnectorConfig.FORMAT_CLASS_CONFIG, ByteArrayFormat.class.getName());
    localProps.put(S3SinkConnectorConfig.FORMAT_BYTEARRAY_LINE_SEPARATOR_CONFIG, "SEPARATOR");
    localProps.put(S3SinkConnectorConfig.FORMAT_BYTEARRAY_EXTENSION_CONFIG, extension);
    setUp();
    task = new S3SinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);

    List<SinkRecord> sinkRecords = createByteArrayRecordsWithoutSchema(7 * context.assignment().size(), 0, context.assignment());
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long[] validOffsets = {0, 3, 6};
    verify(sinkRecords, validOffsets, context.assignment(), extension);
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
    testCorrectRecordWriterHelper("this.is" + DEFAULT_EXTENSION + ".dir");
  }

  protected List<SinkRecord> createByteArrayRecordsWithoutSchema(int size, long startOffset, Set<TopicPartition> partitions) {
    String key = "key";
    int ibase = 12;

    List<SinkRecord> sinkRecords = new ArrayList<>();
    for (long offset = startOffset, total = 0; total < size; ++offset) {
      for (TopicPartition tp : partitions) {
        byte[] record = ("{\"schema\":{\"type\":\"struct\",\"fields\":[ " +
                            "{\"type\":\"boolean\",\"optional\":true,\"field\":\"booleanField\"}," +
                            "{\"type\":\"int32\",\"optional\":true,\"field\":\"intField\"}," +
                            "{\"type\":\"int64\",\"optional\":true,\"field\":\"longField\"}," +
                            "{\"type\":\"string\",\"optional\":false,\"field\":\"stringField\"}]," +
                            "\"payload\":" +
                            "{\"booleanField\":\"true\"," +
                            "\"intField\":" + String.valueOf(ibase) + "," +
                            "\"longField\":" + String.valueOf((long) ibase) + "," +
                            "\"stringField\":str" + String.valueOf(ibase) +
                            "}}").getBytes();
        sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), null, key, null, record, offset));
        if (++total >= size) {
          break;
        }
      }
    }
    return sinkRecords;
  }

  protected String getDirectory(String topic, int partition) {
    String encodedPartition = "partition=" + partition;
    return partitioner.generatePartitionedPath(topic, encodedPartition);
  }

  protected void verifyContents(List<SinkRecord> expectedRecords, int startIndex, Collection<Object> records) {
    for (Object record : records) {
      byte[] bytes = (byte[]) record;
      SinkRecord expectedRecord = expectedRecords.get(startIndex++);
      byte[] expectedBytes = (byte[]) expectedRecord.value();
      assertArrayEquals(expectedBytes, bytes);
    }
  }

  @Override
  protected List<SinkRecord> createGenericRecords(int count, long firstOffset) {
    return createByteArrayRecordsWithoutSchema(
        count * context.assignment().size(),
        firstOffset,
        context.assignment()
    );
  }

  @Override
  protected void verify(
      List<SinkRecord> sinkRecords,
      long[] validOffsets,
      Set<TopicPartition> partitions
  ) throws IOException {
    verify(
        sinkRecords,
        validOffsets,
        partitions,
        DEFAULT_EXTENSION
    );
  }

  protected void verify(List<SinkRecord> sinkRecords, long[] validOffsets, Set<TopicPartition> partitions,
                        String extension) throws IOException {
    verify(sinkRecords, validOffsets, partitions, extension, false);
  }

  /**
   * Verify files and records are uploaded appropriately.
   * @param sinkRecords a flat list of the records that need to appear in potentially several files in S3.
   * @param validOffsets an array containing the offsets that map to uploaded files for a topic-partition.
   *                     Offsets appear in ascending order, the difference between two consecutive offsets
   *                     equals the expected size of the file, and last offset in exclusive.
   * @throws IOException
   */
  protected void verify(List<SinkRecord> sinkRecords, long[] validOffsets, Set<TopicPartition> partitions,
                        String extension, boolean skipFileListing)
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
        // assertEquals(size, records.size());
        verifyContents(sinkRecords, j, records);
        j += size;
      }
    }
  }
}
