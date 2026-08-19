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

package io.confluent.connect.s3.backup;

import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.storage.S3OutputStream;
import io.confluent.connect.s3.storage.S3Storage;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class S3StorageWriterTest {

  private static final String TEST_PATH = "topics/foo/backup-metadata/schemas/42.entry.json";
  private static final String TEST_CONTENT = "{\"id\":\"42\",\"type\":\"AVRO\"}";

  private S3Storage storage;
  private S3SinkConnectorConfig config;
  private S3OutputStream out;
  private S3StorageWriter writer;

  @Before
  public void setUp() {
    storage = mock(S3Storage.class);
    config = mock(S3SinkConnectorConfig.class);
    out = mock(S3OutputStream.class);
    when(storage.conf()).thenReturn(config);
    when(storage.create(eq(TEST_PATH), any(S3SinkConnectorConfig.class), anyBoolean()))
        .thenReturn(out);
    writer = new S3StorageWriter(storage);
  }

  @Test
  public void writeCommitsAndClosesStream() throws IOException {
    writer.write(TEST_PATH, TEST_CONTENT);

    verify(out).write(TEST_CONTENT.getBytes());
    verify(out).commit();
    verify(out).close();
  }

  @Test(expected = ConnectException.class)
  public void writeRejectsNullContent() {
    writer.write(TEST_PATH, null);
  }

  @Test
  public void writeDoesNotOpenStreamWhenContentIsNull() {
    try {
      writer.write(TEST_PATH, null);
      fail("expected ConnectException");
    } catch (ConnectException expected) {
      // expected
    }
    verify(storage, never()).create(any(), any(S3SinkConnectorConfig.class), anyBoolean());
  }

  @Test
  public void writeClosesStreamEvenWhenCommitFails() throws IOException {
    doThrow(new IOException("commit failed")).when(out).commit();

    try {
      writer.write(TEST_PATH, TEST_CONTENT);
      fail("expected ConnectException");
    } catch (ConnectException expected) {
      // expected
    }
    verify(out).close();
  }

  @Test(expected = ConnectException.class)
  public void writeWrapsIoExceptionInConnectException() throws IOException {
    doThrow(new IOException("s3 down")).when(out).commit();

    writer.write(TEST_PATH, TEST_CONTENT);
  }

  @Test
  public void existsDelegatesToStorageTrue() {
    when(storage.exists(TEST_PATH)).thenReturn(true);

    assertTrue(writer.exists(TEST_PATH));
    verify(storage).exists(TEST_PATH);
  }

  @Test
  public void existsDelegatesToStorageFalse() {
    when(storage.exists(TEST_PATH)).thenReturn(false);

    assertFalse(writer.exists(TEST_PATH));
    verify(storage).exists(TEST_PATH);
  }

  @Test(expected = ConnectException.class)
  public void existsWrapsExceptionInConnectException() {
    when(storage.exists(TEST_PATH)).thenThrow(new RuntimeException("s3 down"));

    writer.exists(TEST_PATH);
  }
}
