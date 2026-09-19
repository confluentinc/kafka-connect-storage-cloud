/*
 * Copyright 2025 Confluent Inc.
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

import io.confluent.connect.storage.StorageSinkConnectorConfig;
import io.confluent.connect.storage.StorageSinkConnectorConfig.Mode;
import io.confluent.connect.storage.format.backup.EnvelopeTransformer;
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

import java.lang.reflect.Field;
import java.util.Collections;

import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.StorageFactory;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.easymock.PowerMock.replayAll;
import static org.powermock.api.easymock.PowerMock.verifyAll;

@RunWith(PowerMockRunner.class)
// Deliberately omit BackupS3SinkTask from @PrepareForTest: PowerMock instrumentation on the
// class-under-test defeats jacoco line coverage tracking. Only StorageFactory needs mockStatic.
@PrepareForTest({StorageFactory.class})
@PowerMockIgnore({"io.findify.s3mock.*", "akka.*", "javax.*", "org.xml.*", "com.sun.org.apache.xerces.*",
    "org.jacoco.*"})
public class BackupS3SinkTaskTest extends DataWriterAvroTest {

  private static final String KEY_CONVERTER_CONFIG = "key.converter";
  private static final String VALUE_CONVERTER_CONFIG = "value.converter";
  private static final String STRING_CONVERTER =
      "org.apache.kafka.connect.storage.StringConverter";
  private static final String ENVELOPE_TRANSFORMER_FIELD = "envelopeTransformer";

  //@Before omitted so per-test localProps can be set before setUp().
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

  private void putBackupModeProps() {
    localProps.put(StorageSinkConnectorConfig.MODE_CONFIG, Mode.BACKUP_FULL_RECORD.name());
    localProps.put(KEY_CONVERTER_CONFIG, STRING_CONVERTER);
    localProps.put(VALUE_CONVERTER_CONFIG, STRING_CONVERTER);
  }

  @Test
  public void testTaskType() throws Exception {
    putBackupModeProps();
    setUp();
    replayAll();
    BackupS3SinkTask task = new BackupS3SinkTask();
    assertTrue(SinkTask.class.isAssignableFrom(task.getClass()));
    assertTrue(S3SinkTask.class.isAssignableFrom(task.getClass()));
  }

  @Test
  public void testStartWiresEnvelopeTransformer() throws Exception {
    putBackupModeProps();
    setUp();
    replayAll();
    BackupS3SinkTask task = new BackupS3SinkTask();
    task.initialize(context);
    task.start(properties);
    verifyAll();

    Object transformer = readTransformer(task);
    assertNotNull("start() must initialize the envelope transformer", transformer);
    assertTrue(transformer instanceof EnvelopeTransformer);

    task.stop();
  }

  @Test
  public void testPutForwardsWrappedRecordsToSuper() throws Exception {
    putBackupModeProps();
    setUp();
    replayAll();
    BackupS3SinkTask task = new BackupS3SinkTask();
    task.initialize(context);
    task.start(properties);
    verifyAll();

    // empty collection exercises wrap + super.put chain without SinkRecord setup
    task.put(Collections.<SinkRecord>emptyList());

    task.stop();
  }

  @Test
  public void testStopClearsEnvelopeTransformer() throws Exception {
    putBackupModeProps();
    setUp();
    replayAll();
    BackupS3SinkTask task = new BackupS3SinkTask();
    task.initialize(context);
    task.start(properties);
    verifyAll();
    assertNotNull(readTransformer(task));

    task.stop();

    assertNull("stop() must clear the envelope transformer", readTransformer(task));
  }

  private static Object readTransformer(BackupS3SinkTask task) throws Exception {
    Field f = BackupS3SinkTask.class.getDeclaredField(ENVELOPE_TRANSFORMER_FIELD);
    f.setAccessible(true);
    return f.get(task);
  }
}
