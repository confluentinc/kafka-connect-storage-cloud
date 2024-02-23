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


import static io.confluent.connect.s3.util.Utils.sinkRecordToLoggableString;
import static java.util.Objects.requireNonNull;

import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class that adds a record writer layer to manage writing values, keys and headers
 * with a single call. It provides an abstraction for writing, committing and
 * closing all three header, key and value files.
 */
public class KeyValueHeaderRecordWriterProvider
    implements RecordWriterProvider<S3SinkConnectorConfig> {

  private static final Logger log =
      LoggerFactory.getLogger(KeyValueHeaderRecordWriterProvider.class);

  @Nullable
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
    String strippedFilename = filename.endsWith(valueProvider.getExtension())
        ? filename.substring(0, filename.length() - valueProvider.getExtension().length())
        : filename;

    Optional<RecordWriter> valueWriter = filename.contains(conf.getTombstoneEncodedPartition())
        ? Optional.empty() : Optional.of(valueProvider.getRecordWriter(conf, strippedFilename));
    Optional<RecordWriter> keyWriter = Optional.ofNullable(keyProvider)
            .map(keyProvider -> keyProvider.getRecordWriter(conf, strippedFilename));
    Optional<RecordWriter> headerWriter = Optional.ofNullable(headerProvider)
            .map(headerProvider -> headerProvider.getRecordWriter(conf, strippedFilename));

    return new RecordWriter() {
      @Override
      public void write(SinkRecord sinkRecord) {
        if (conf.isTombstoneWriteEnabled() && !keyWriter.isPresent()) {
          throw new ConnectException(
              "Key Writer must be configured when writing tombstone records is enabled.");
        }

        // The two data exceptions below must be caught before writing the value
        // to avoid misaligned K/V/H files.

        // keyWriter != null means writing keys is turned on
        if (keyWriter.isPresent() && sinkRecord.key() == null) {
          throw new DataException(
              String.format("Key cannot be null for SinkRecord: %s",
                  sinkRecordToLoggableString(sinkRecord))
          );
        }

        // headerWriter != null means writing headers is turned on
        if (headerWriter.isPresent()
            && (sinkRecord.headers() == null || sinkRecord.headers().isEmpty())) {
          throw new DataException(
              String.format("Headers cannot be null for SinkRecord: %s",
                  sinkRecordToLoggableString(sinkRecord))
          );
        }

        if (valueWriter.isPresent()) {
          valueWriter.get().write(sinkRecord);
        } else {
          // Should only encounter tombstones here.
          if (sinkRecord.value() != null) {
            throw new ConnectException(
                String.format("Value writer not configured for SinkRecord: %s."
                        + " fileName: %s, tombstonePartition: %s",
                    sinkRecordToLoggableString(sinkRecord), filename,
                    conf.getTombstoneEncodedPartition())
            );
          }
        }
        keyWriter.ifPresent(writer -> writer.write(sinkRecord));
        headerWriter.ifPresent(writer -> writer.write(sinkRecord));
      }

      @Override
      public void close() {
        valueWriter.ifPresent(RecordWriter::close);
        keyWriter.ifPresent(RecordWriter::close);
        headerWriter.ifPresent(RecordWriter::close);
      }

      @Override
      public void commit() {
        valueWriter.ifPresent(RecordWriter::commit);
        keyWriter.ifPresent(RecordWriter::commit);
        headerWriter.ifPresent(RecordWriter::commit);
      }
    };
  }
}
