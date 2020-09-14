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


import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

/**
 * A class that adds a record writer layer to manage writing values, keys and headers
 * with a single call. It provides an abstraction for writing, committing and
 * closing all three header, key and value files.
 */
public class KeyValueHeaderRecordWriterProvider
    implements RecordWriterProvider<S3SinkConnectorConfig> {

  private static final Logger log =
      LoggerFactory.getLogger(KeyValueHeaderRecordWriterProvider.class);

  @NotNull
  private final RecordWriterProvider<S3SinkConnectorConfig> valueProvider;

  @Nullable
  private final RecordWriterProvider<S3SinkConnectorConfig> keyProvider;

  @Nullable
  private final RecordWriterProvider<S3SinkConnectorConfig> headerProvider;

  public KeyValueHeaderRecordWriterProvider(
      RecordWriterProvider<S3SinkConnectorConfig> valueProvider,
      @Nullable RecordWriterProvider<S3SinkConnectorConfig> keyProvider,
      @Nullable RecordWriterProvider<S3SinkConnectorConfig> headerProvider) {
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
    // Remove extension to allow different formats for value, key and headers.
    // Each provider will add its own extension. The filename comes in with the value file format,
    // e.g. filename.avro, but when the format class is different for the key or the headers the
    // extension needs to be removed.
    int extensionIndex = filename.indexOf(valueProvider.getExtension());
    String strippedFilename = extensionIndex > -1
        ? filename.substring(0, extensionIndex)
        : filename;

    RecordWriter valueWriter = valueProvider.getRecordWriter(conf, strippedFilename);
    RecordWriter keyWriter =
        keyProvider == null ? null : keyProvider.getRecordWriter(conf, strippedFilename);
    RecordWriter headerWriter =
        headerProvider == null ? null : headerProvider.getRecordWriter(conf, strippedFilename);

    return new RecordWriter() {
      @Override
      public void write(SinkRecord sinkRecord) {
        valueWriter.write(sinkRecord); // null check happens in sink task
        // keyWriter != null means writing keys is turned on
        if (keyWriter != null && sinkRecord.key() == null) {
          throw new DataException(
              String.format("Key cannot be null for SinkRecord: %s", sinkRecord)
          );
        } else if (keyWriter != null) {
          keyWriter.write(sinkRecord);
        }

        // headerWriter != null means writing headers is turned on
        if (headerWriter != null && sinkRecord.headers() == null) {
          throw new DataException(
              String.format("Headers cannot be null for SinkRecord: %s", sinkRecord)
          );
        } else if (headerWriter != null) {
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
