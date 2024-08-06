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

import com.amazonaws.services.s3.model.CannedAccessControlList;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.confluent.connect.s3.format.avro.AvroUtils;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.s3.util.FileUtils;
import io.confluent.connect.storage.StorageFactory;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.powermock.api.easymock.PowerMock.replayAll;
import static org.powermock.api.easymock.PowerMock.verifyAll;

@RunWith(PowerMockRunner.class)
@PrepareForTest({S3SinkTask.class, StorageFactory.class})
@PowerMockIgnore({"io.findify.s3mock.*", "akka.*", "javax.*", "org.xml.*", "com.sun.org.apache.xerces.*"})
public class S3SinkTaskTest extends DataWriterAvroTest {

  //@Before should be omitted in order to be able to add properties per test.
  public void setUp() throws Exception {
    super.setUp();
    Capture<Class<S3Storage>> capturedStorage = EasyMock.newCapture();
    Capture<Class<S3SinkConnectorConfig>> capturedStorageConf = EasyMock.newCapture();
    Capture<S3SinkConnectorConfig> capturedConf = EasyMock.newCapture();
    Capture<String> capturedUrl = EasyMock.newCapture();
    PowerMock.mockStatic(StorageFactory.class);
    EasyMock.expect(StorageFactory.createStorage(EasyMock.capture(capturedStorage),
                                                 EasyMock.capture(capturedStorageConf),
                                                 EasyMock.capture(capturedConf),
                                                 EasyMock.capture(capturedUrl))).andReturn(storage);
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    localProps.clear();
  }

  @Test
  public void testTaskType() throws Exception {
    setUp();
    replayAll();
    task = new S3SinkTask();
    SinkTask.class.isAssignableFrom(task.getClass());
  }

  @Test
  public void testWriteRecord() throws Exception {
    setUp();
    replayAll();
    task = new S3SinkTask();
    task.initialize(context);
    task.start(properties);
    verifyAll();

    List<SinkRecord> sinkRecords = createRecords(7);
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long[] validOffsets = {0, 3, 6};
    verify(sinkRecords, validOffsets);
  }

  @Test
  public void testWriteNullRecords() throws Exception {
    setUp();
    replayAll();
    task = new S3SinkTask();
    SinkTaskContext mockContext = mock(SinkTaskContext.class);
    ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
    when(mockContext.errantRecordReporter()).thenReturn(reporter);
    when(mockContext.assignment()).thenReturn(Collections.singleton(TOPIC_PARTITION));
    task.initialize(mockContext);

    properties.put(S3SinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG,
        S3SinkConnectorConfig.IgnoreOrFailBehavior.IGNORE.toString());
    task.start(properties);
    verifyAll();

    List<SinkRecord> sinkRecords = createRecordsWithPrimitive(3, 0,
        Collections.singleton(new TopicPartition(TOPIC, PARTITION)));
    sinkRecords.add(
        new SinkRecord(TOPIC, PARTITION, null, null, Schema.OPTIONAL_STRING_SCHEMA, null,
            0));
    sinkRecords.add(new SinkRecord(TOPIC, PARTITION, null, null, null, null, 1));
    sinkRecords.addAll(createRecordsWithPrimitive(4, 3,
        Collections.singleton(new TopicPartition(TOPIC, PARTITION))));
    task.put(sinkRecords);
    task.close(mockContext.assignment());
    task.stop();

    long[] validOffsets = {0, 3, 6};

    // expect sink records like the ones we put, but without the null records
    List<SinkRecord> expectedSinkRecords = createRecordsWithPrimitive(3, 0,
        Collections.singleton(new TopicPartition(TOPIC, PARTITION)));
    expectedSinkRecords.addAll(createRecordsWithPrimitive(4, 3,
        Collections.singleton(new TopicPartition(TOPIC, PARTITION))));
    verify(expectedSinkRecords, validOffsets);
    Mockito.verify(reporter, times(2)).report(any(), any(DataException.class));
  }

  @Test
  public void testWriteRecordWithPrimitives() throws Exception {
    setUp();
    replayAll();
    task = new S3SinkTask();
    task.initialize(context);
    task.start(properties);
    verifyAll();

    List<SinkRecord> sinkRecords = createRecordsWithPrimitive(7, 0, Collections.singleton(new TopicPartition (TOPIC, PARTITION)));
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

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

    replayAll();
    task = new S3SinkTask();
    task.initialize(context);
    task.start(properties);
    verifyAll();
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

    List<SinkRecord> sinkRecords = createRecords(11000);
    replayAll();

    task = new S3SinkTask();
    task.initialize(context);
    task.start(properties);
    verifyAll();

    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long[] validOffsets = {0, 10000};
    verify(sinkRecords, validOffsets);
  }

  @Test
  public void testPartitionerConfig() throws Exception {
    localProps.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());
    localProps.put("custom.partitioner.config", "arbitrary value");
    setUp();
    replayAll();
    task = new S3SinkTask();
    task.initialize(context);
    task.start(properties);
  }

  @Test
  public void testWriteRecordsWithDigestMultipleParts() throws Exception {
    localProps.put(S3SinkConnectorConfig.FLUSH_SIZE_CONFIG, "10000");
    localProps.put(S3SinkConnectorConfig.SEND_DIGEST_CONFIG, "true");
    setUp();

    List<SinkRecord> sinkRecords = createRecords(11000);
    replayAll();

    task = new S3SinkTask();
    task.initialize(context);
    task.start(properties);
    verifyAll();

    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long[] validOffsets = {0, 10000};
    verify(sinkRecords, validOffsets);
  }

  public static class CustomPartitioner<T> extends DefaultPartitioner<T> {
    @Override
    public void configure(Map<String, Object> map) {
      assertTrue("Custom parameters were not passed down to the partitioner implementation",
          map.containsKey("custom.partitioner.config"));
    }
  }

  @Test
  public void testAclCannedConfig() throws Exception {
    localProps.put(S3SinkConnectorConfig.ACL_CANNED_CONFIG, CannedAccessControlList.BucketOwnerFullControl.toString());
    setUp();
    replayAll();
    task = new S3SinkTask();
    task.initialize(context);
    task.start(properties);
  }

}

