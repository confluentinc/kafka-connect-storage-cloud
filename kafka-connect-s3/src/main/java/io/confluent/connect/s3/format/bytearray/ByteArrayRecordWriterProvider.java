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

package io.confluent.connect.s3.format.bytearray;

import static io.confluent.connect.s3.util.Utils.getAdjustedFilename;
import static io.confluent.connect.s3.util.Utils.sinkRecordToLoggableString;

import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.storage.IORecordWriter;
import io.confluent.connect.s3.format.RecordViewSetter;
import io.confluent.connect.s3.format.S3RetriableRecordWriter;
import io.confluent.connect.s3.storage.S3OutputStream;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class ByteArrayRecordWriterProvider extends RecordViewSetter
    implements RecordWriterProvider<S3SinkConnectorConfig> {

  private static final Logger log = LoggerFactory.getLogger(ByteArrayRecordWriterProvider.class);
  private final S3Storage storage;
  private final ByteArrayConverter converter;
  private final String extension;
  private final byte[] lineSeparatorBytes;

  ByteArrayRecordWriterProvider(S3Storage storage, ByteArrayConverter converter) {
    this.storage = storage;
    this.converter = converter;
    this.extension = storage.conf().getByteArrayExtension();
    this.lineSeparatorBytes = storage.conf()
        .getFormatByteArrayLineSeparator()
        .getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public String getExtension() {
    return extension + storage.conf().getCompressionType().extension;
  }

  @Override
  public RecordWriter getRecordWriter(final S3SinkConnectorConfig conf, final String filename) {
    return new S3RetriableRecordWriter(
        new IORecordWriter() {
          final String adjustedFilename = getAdjustedFilename(recordView, filename, getExtension());
          final S3OutputStream s3out = storage
              .create(adjustedFilename, true, ByteArrayFormat.class);
          final OutputStream s3outWrapper = s3out.wrapForCompression();

          @Override
          public void write(SinkRecord record) throws IOException {
            log.trace("Sink record with view {}: {}", recordView,
                sinkRecordToLoggableString(record));
            byte[] bytes = converter.fromConnectData(record.topic(),
                recordView.getViewSchema(record, false), recordView.getView(record, false));
            s3outWrapper.write(bytes);
            s3outWrapper.write(lineSeparatorBytes);
          }

          @Override
          public void commit() throws IOException {
            s3out.commit();
            s3outWrapper.close();
          }

          @Override
          public void close() throws IOException {
          }
        }
    );
  }
}
