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

package io.confluent.connect.s3.format;

import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.errors.FileExistsException;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.header.Headers;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.*;
import static org.junit.Assert.fail;

public class KeyValueHeaderWriterProviderTest {

  private RecordWriterProvider<S3SinkConnectorConfig> mockValueProvider;
  private RecordWriterProvider<S3SinkConnectorConfig> mockKeyProvider;
  private RecordWriterProvider<S3SinkConnectorConfig> mockHeaderProvider;
  private S3SinkConnectorConfig mockConfig;
  private SinkRecord mockSinkRecord;
  private RecordWriter mockValueWriter;
  private RecordWriter mockKeyWriter;
  private RecordWriter mockHeaderWriter;

  @Before
  public void setUp() {
    mockValueProvider = mock(RecordWriterProvider.class);
    mockKeyProvider = mock(RecordWriterProvider.class);
    mockHeaderProvider = mock(RecordWriterProvider.class);
    mockConfig = mock(S3SinkConnectorConfig.class);
    when(mockConfig.getTombstoneEncodedPartition()).thenReturn("tombstone");
    mockSinkRecord = mock(SinkRecord.class);
    mockValueWriter = mock(RecordWriter.class);
    mockKeyWriter = mock(RecordWriter.class);
    mockHeaderWriter = mock(RecordWriter.class);

    when(mockValueProvider.getRecordWriter(any(), anyString())).thenReturn(mockValueWriter);
    when(mockKeyProvider.getRecordWriter(any(), anyString())).thenReturn(mockKeyWriter);
    when(mockHeaderProvider.getRecordWriter(any(), anyString())).thenReturn(mockHeaderWriter);
    when(mockHeaderProvider.getExtension()).thenReturn(".avro");
    when(mockKeyProvider.getExtension()).thenReturn(".avro");
    when(mockValueProvider.getExtension()).thenReturn(".avro");
  }

  @Test
  public void testWriteAllWritersPresent() {

    when(mockSinkRecord.key()).thenReturn("key1");
    when(mockSinkRecord.headers()).thenReturn(mock(Headers.class));
    when(mockSinkRecord.value()).thenReturn("value1");
    KeyValueHeaderRecordWriterProvider provider = new KeyValueHeaderRecordWriterProvider(
        mockValueProvider, mockKeyProvider, mockHeaderProvider);

    RecordWriter writer = provider.getRecordWriter(mockConfig, "test-file");
    writer.write(mockSinkRecord);

    verify(mockValueWriter).write(mockSinkRecord);
    verify(mockKeyWriter).write(mockSinkRecord);
    verify(mockHeaderWriter).write(mockSinkRecord);
  }

  @Test
  public void testWriteKeyMissingThrowsException() {
    KeyValueHeaderRecordWriterProvider provider = new KeyValueHeaderRecordWriterProvider(
        mockValueProvider, mockKeyProvider, mockHeaderProvider);

    when(mockSinkRecord.key()).thenReturn(null);

    RecordWriter writer = provider.getRecordWriter(mockConfig, "test-file");
    assertThrows(DataException.class, () -> writer.write(mockSinkRecord));
  }

  @Test
  public void testWriteHeadersMissingThrowsException() {
    KeyValueHeaderRecordWriterProvider provider = new KeyValueHeaderRecordWriterProvider(
        mockValueProvider, mockKeyProvider, mockHeaderProvider);

    when(mockSinkRecord.headers()).thenReturn(null);

    RecordWriter writer = provider.getRecordWriter(mockConfig, "test-file");
    assertThrows(DataException.class, () -> writer.write(mockSinkRecord));
  }

  @Test
  public void testWriteTombstoneRecord() {
    when(mockConfig.isTombstoneWriteEnabled()).thenReturn(true);

    KeyValueHeaderRecordWriterProvider provider = new KeyValueHeaderRecordWriterProvider(
        mockValueProvider, mockKeyProvider, null);

    when(mockSinkRecord.value()).thenReturn(null);
    when(mockSinkRecord.key()).thenReturn("key1");

    RecordWriter writer = provider.getRecordWriter(mockConfig, "test-file.tombstone");
    writer.write(mockSinkRecord);

    verify(mockKeyWriter).write(mockSinkRecord);
    verify(mockValueWriter, never()).write(mockSinkRecord);
  }

  @Test
  public void testWriteWithConditionalWritesValueExists() {
    when(mockConfig.isTombstoneWriteEnabled()).thenReturn(true);
    when(mockSinkRecord.headers()).thenReturn(mock(Headers.class));

    KeyValueHeaderRecordWriterProvider provider = new KeyValueHeaderRecordWriterProvider(
        mockValueProvider, mockKeyProvider, mockHeaderProvider);

    when(mockSinkRecord.value()).thenReturn("value1");
    when(mockSinkRecord.key()).thenReturn("key1");

    doThrow(FileExistsException.class).when(mockValueWriter).commit();
    try {
      RecordWriter writer = provider.getRecordWriter(mockConfig, "test-file");
      writer.commit();
    } catch (FileExistsException e) {
      verify(mockKeyWriter).commit();
      verify(mockValueWriter).commit();
      verify(mockHeaderWriter).commit();
      return;
    }
    fail("Expected FileExistsException to be thrown");
  }

  @Test
  public void testWriteWithConditionalWritesValueAndKeyExists() {
    when(mockConfig.isTombstoneWriteEnabled()).thenReturn(true);
    when(mockSinkRecord.headers()).thenReturn(mock(Headers.class));

    KeyValueHeaderRecordWriterProvider provider = new KeyValueHeaderRecordWriterProvider(
        mockValueProvider, mockKeyProvider, mockHeaderProvider);

    when(mockSinkRecord.value()).thenReturn("value1");
    when(mockSinkRecord.key()).thenReturn("key1");

    doThrow(FileExistsException.class).when(mockValueWriter).commit();
    doThrow(FileExistsException.class).when(mockKeyWriter).commit();
    try {
      RecordWriter writer = provider.getRecordWriter(mockConfig, "test-file");
      writer.commit();
    } catch (FileExistsException e) {
      verify(mockKeyWriter).commit();
      verify(mockValueWriter).commit();
      verify(mockHeaderWriter).commit();
      return;
    }
    fail("Expected FileExistsException to be thrown");
  }

  @Test
  public void testWriteWithConditionalWritesValueAndKeyAndHeaderExists() {
    when(mockConfig.isTombstoneWriteEnabled()).thenReturn(true);
    when(mockSinkRecord.headers()).thenReturn(mock(Headers.class));

    KeyValueHeaderRecordWriterProvider provider = new KeyValueHeaderRecordWriterProvider(
        mockValueProvider, mockKeyProvider, mockHeaderProvider);

    when(mockSinkRecord.value()).thenReturn("value1");
    when(mockSinkRecord.key()).thenReturn("key1");

    doThrow(FileExistsException.class).when(mockValueWriter).commit();
    doThrow(FileExistsException.class).when(mockKeyWriter).commit();
    doThrow(FileExistsException.class).when(mockHeaderWriter).commit();
    try {
      RecordWriter writer = provider.getRecordWriter(mockConfig, "test-file");
      writer.commit();
    } catch (FileExistsException e) {
      verify(mockKeyWriter).commit();
      verify(mockValueWriter).commit();
      verify(mockHeaderWriter).commit();
      return;
    }
    fail("Expected FileExistsException to be thrown");
  }

  @Test
  public void testCloseAndCommit() {
    KeyValueHeaderRecordWriterProvider provider = new KeyValueHeaderRecordWriterProvider(
        mockValueProvider, mockKeyProvider, mockHeaderProvider);

    RecordWriter writer = provider.getRecordWriter(mockConfig, "test-file");
    writer.close();
    writer.commit();

    verify(mockValueWriter).close();
    verify(mockKeyWriter).close();
    verify(mockHeaderWriter).close();

    verify(mockValueWriter).commit();
    verify(mockKeyWriter).commit();
    verify(mockHeaderWriter).commit();
  }

  @Test
  public void testGetKeyFilename() {
    RecordWriterProvider<S3SinkConnectorConfig> mockKeyProvider = new TestKeyHeaderProvider(".json", new RecordViews.KeyRecordView());
    RecordWriterProvider<S3SinkConnectorConfig> mockValueProvider = new TestKeyHeaderProvider(".avro", new RecordViews.ValueRecordView());

    KeyValueHeaderRecordWriterProvider provider = new KeyValueHeaderRecordWriterProvider(
        mockValueProvider, mockKeyProvider, null);

    String keyFilename = provider.getKeyFilename("test-file.avro");
    assertEquals("test-file.keys.json", keyFilename);
  }

  @Test
  public void testGetKeyFilenameWhenValueExtensionDifferent() {
    RecordWriterProvider<S3SinkConnectorConfig> mockKeyProvider = new TestKeyHeaderProvider(".json", new RecordViews.KeyRecordView());
    RecordWriterProvider<S3SinkConnectorConfig> mockValueProvider = new TestKeyHeaderProvider(".avro", new RecordViews.ValueRecordView());

    KeyValueHeaderRecordWriterProvider provider = new KeyValueHeaderRecordWriterProvider(
        mockValueProvider, mockKeyProvider, null);

    String keyFilename = provider.getKeyFilename("test-file");
    assertEquals("test-file.keys.json", keyFilename);
  }

  @Test
  public void testGetKeyFilenameWhenKeyWriterNotConfigured() {
    RecordWriterProvider<S3SinkConnectorConfig> mockValueProvider = new TestKeyHeaderProvider(".avro", new RecordViews.ValueRecordView());

    KeyValueHeaderRecordWriterProvider provider = new KeyValueHeaderRecordWriterProvider(
        mockValueProvider, null, null);

    String keyFilename = provider.getKeyFilename("test-file.avro");
    assertNull(keyFilename);
  }

  @Test
  public void testGetHeaderFilename() {
    RecordWriterProvider<S3SinkConnectorConfig> mockHeaderProvider = new TestKeyHeaderProvider(".json", new RecordViews.HeaderRecordView());
    RecordWriterProvider<S3SinkConnectorConfig> mockValueProvider = new TestKeyHeaderProvider(".avro", new RecordViews.ValueRecordView());

    KeyValueHeaderRecordWriterProvider provider = new KeyValueHeaderRecordWriterProvider(
        mockValueProvider, null, mockHeaderProvider);

    String headerFilename = provider.getHeaderFilename("test-file.avro");
    assertEquals("test-file.headers.json", headerFilename);
  }

  @Test
  public void testGetHeaderFilenameWhenValueFilExtensionDifferent() {
    RecordWriterProvider<S3SinkConnectorConfig> mockHeaderProvider = new TestKeyHeaderProvider(".json", new RecordViews.HeaderRecordView());
    RecordWriterProvider<S3SinkConnectorConfig> mockValueProvider = new TestKeyHeaderProvider(".avro", new RecordViews.ValueRecordView());

    KeyValueHeaderRecordWriterProvider provider = new KeyValueHeaderRecordWriterProvider(
        mockValueProvider, null, mockHeaderProvider);

    String headerFilename = provider.getHeaderFilename("test-file");
    assertEquals("test-file.headers.json", headerFilename);
  }

  @Test
  public void testGetHeaderFilenameWhenHeaderWriterNotConfigured() {
    RecordWriterProvider<S3SinkConnectorConfig> mockValueProvider = new TestKeyHeaderProvider(".avro", new RecordViews.ValueRecordView());

    KeyValueHeaderRecordWriterProvider provider = new KeyValueHeaderRecordWriterProvider(
        mockValueProvider, null, null);

    String headerFilename = provider.getHeaderFilename("test-file.avro");
    assertNull(headerFilename);
  }

  class TestKeyHeaderProvider extends RecordViewSetter
      implements RecordWriterProvider<S3SinkConnectorConfig> {
    String extension;
    RecordView recordView;

    TestKeyHeaderProvider(String extension, RecordView recordView) {
      this.extension = extension;
      this.recordView = recordView;
    }

    @Override
    public RecordWriter getRecordWriter(final S3SinkConnectorConfig conf, final String filename) {
      return mock(RecordWriter.class);
    }

    @Override
    public String getExtension() {
      return extension;
    }

    @Override
    public RecordView getRecordView() {
      return recordView;
    }
  }
}
