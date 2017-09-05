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

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.s3.format.avro.AvroUtils;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.s3.util.FileUtils;
import io.confluent.connect.storage.StorageFactory;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;
import static org.powermock.api.easymock.PowerMock.replayAll;
import static org.powermock.api.easymock.PowerMock.verifyAll;

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
  public void testWriteRecordsSpanningMultiplePartsWithRetry() throws Exception {
    localProps.put(S3SinkConnectorConfig.FLUSH_SIZE_CONFIG, "10000");
    localProps.put(S3SinkConnectorConfig.S3_PART_RETRIES_CONFIG, "3");
    setUp();

    List<SinkRecord> sinkRecords = createRecords(11000);
    int totalBytes = calcByteSize(sinkRecords);
    final int parts = totalBytes / connectorConfig.getPartSize();

    // From time to time fail S3 upload part method
    final AtomicInteger count = new AtomicInteger();
    PowerMockito.doAnswer(new Answer<UploadPartResult>() {
      @Override
      public UploadPartResult answer(InvocationOnMock invocationOnMock) throws Throwable {
        if(count.getAndIncrement() % parts == 0){
          throw new SdkClientException("Boom!");
        } else {
          return (UploadPartResult)invocationOnMock.callRealMethod();
        }
      }
    }).when(s3).uploadPart(Mockito.isA(UploadPartRequest.class));


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

  private int calcByteSize(List<SinkRecord> sinkRecords) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>());
    AvroData avroData = new AvroData(1);
    boolean writerInit = false;
    for(SinkRecord sinkRecord: sinkRecords){
      if(!writerInit){
        writer.create(avroData.fromConnectSchema(sinkRecord.valueSchema()), baos);
        writerInit = true;
      }
      writer.append(avroData.fromConnectData(sinkRecord.valueSchema(), sinkRecord.value()));
    }
    return baos.size();
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

