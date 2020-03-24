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

import org.apache.kafka.connect.sink.SinkRecord;

import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;

import static java.util.Objects.requireNonNull;

public class KeyValueHeaderRecordWriterProvider
    implements RecordWriterProvider<S3SinkConnectorConfig> {

  private final RecordWriterProvider<S3SinkConnectorConfig> valueProvider;
  private final RecordWriterProvider<S3SinkConnectorConfig> keyProvider;
  private final RecordWriterProvider<S3SinkConnectorConfig> headerProvider;


  public KeyValueHeaderRecordWriterProvider(
      RecordWriterProvider<S3SinkConnectorConfig> valueProvider,
      RecordWriterProvider<S3SinkConnectorConfig> keyProvider,
      RecordWriterProvider<S3SinkConnectorConfig> headerProvider) {
    this.valueProvider = requireNonNull(valueProvider);
    this.keyProvider = keyProvider;
    this.headerProvider = headerProvider;
  }

  @Override
  public String getExtension() {
    return valueProvider.getExtension();
  }

  @Override
  public RecordWriter getRecordWriter(S3SinkConnectorConfig conf, String filename) {
    RecordWriter valueWriter = valueProvider.getRecordWriter(conf, filename);
    RecordWriter keyWriter =
        keyProvider == null ? null : keyProvider.getRecordWriter(conf, filename);
    RecordWriter headerWriter =
        headerProvider == null ? null : headerProvider.getRecordWriter(conf, filename);

    return new RecordWriter() {
      @Override
      public void write(SinkRecord sinkRecord) {
        valueWriter.write(sinkRecord);
        if (keyWriter != null) {
          keyWriter.write(sinkRecord);
        }
        if (headerWriter != null) {
          headerWriter.write(sinkRecord);
        }
      }

      @Override
      public void close() {
        valueWriter.close();
        if (keyWriter != null) {
          keyWriter.close();
        }
        if (headerWriter != null) {
          headerWriter.close();
        }
      }

      @Override
      public void commit() {
        valueWriter.commit();
        if (keyWriter != null) {
          keyWriter.commit();
        }
        if (headerWriter != null) {
          headerWriter.commit();
        }
      }
    };
  }
}
