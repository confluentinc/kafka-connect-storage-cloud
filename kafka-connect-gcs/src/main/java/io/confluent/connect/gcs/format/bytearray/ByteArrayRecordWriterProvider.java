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

package io.confluent.connect.gcs.format.bytearray;

import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import io.confluent.connect.gcs.GcsSinkConnectorConfig;
import io.confluent.connect.gcs.storage.GcsOutputStream;
import io.confluent.connect.gcs.storage.GcsStorage;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;

public class ByteArrayRecordWriterProvider implements RecordWriterProvider<GcsSinkConnectorConfig> {

  private static final Logger log = LoggerFactory.getLogger(ByteArrayRecordWriterProvider.class);
  private final GcsStorage storage;
  private final ByteArrayConverter converter;
  private final String extension;
  private final byte[] lineSeparatorBytes;

  ByteArrayRecordWriterProvider(GcsStorage storage, ByteArrayConverter converter) {
    this.storage = storage;
    this.converter = converter;
    this.extension = storage.conf().getByteArrayExtension();
    this.lineSeparatorBytes = storage.conf().getFormatByteArrayLineSeparator().getBytes();
  }

  @Override
  public String getExtension() {
    return extension;
  }

  @Override
  public RecordWriter getRecordWriter(final GcsSinkConnectorConfig conf, final String filename) {
    return new RecordWriter() {
      final GcsOutputStream gcsOS = storage.create(filename, true);

      @Override
      public void write(SinkRecord record) {
        log.trace("Sink record: {}", record);
        try {
          byte[] bytes = converter.fromConnectData(
              record.topic(), record.valueSchema(), record.value());
          gcsOS.write(bytes);
          gcsOS.write(lineSeparatorBytes);
        } catch (IOException | DataException e) {
          throw new ConnectException(e);
        }
      }

      @Override
      public void commit() {
        try {
          gcsOS.commit();
        } catch (IOException e) {
          throw new ConnectException(e);
        }
      }

      @Override
      public void close() {}
    };
  }
}
