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

import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Map;

import io.confluent.connect.s3.format.avro.AvroUtils;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.s3.util.FileUtils;
import io.confluent.connect.storage.StorageFactory;

import static org.powermock.api.easymock.PowerMock.replayAll;
import static org.powermock.api.easymock.PowerMock.verifyAll;

import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest({S3SinkTask.class, StorageFactory.class})
@PowerMockIgnore({"io.findify.s3mock.*", "akka.*", "javax.*", "org.xml.*"})
public class S3SinkTaskTest extends DataWriterAvroTest {

  private static final String ZERO_PAD_FMT = "%010d";
  private final String extension = ".avro";

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
  public void testRecoveryWithPartialFile() throws Exception {
    setUp();

    // Upload partial file.
    List<SinkRecord> sinkRecords = createRecords(2);
    byte[] partialData = AvroUtils.putRecords(sinkRecords, format.getAvroData());
    String fileKey = FileUtils.fileKeyToCommit(topicsDir, getDirectory(), TOPIC_PARTITION, 0, extension, ZERO_PAD_FMT);
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

  public static class CustomPartitioner<T> extends DefaultPartitioner<T> {
    @Override
    public void configure(Map<String, Object> map) {
      assertTrue("Custom parameters were not passed down to the partitioner implementation",
          map.containsKey("custom.partitioner.config"));
    }
  }
}

