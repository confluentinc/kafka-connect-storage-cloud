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

package io.confluent.connect.s3.format.avro;

import static io.confluent.connect.s3.util.Utils.getAdjustedFilename;
import static io.confluent.connect.s3.util.Utils.sinkRecordToLoggableString;

import io.confluent.connect.s3.storage.IORecordWriter;
import io.confluent.connect.s3.format.S3RetriableRecordWriter;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.format.RecordViewSetter;
import io.confluent.connect.s3.storage.S3OutputStream;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.kafka.serializers.NonRecordContainer;

public class AvroRecordWriterProvider extends RecordViewSetter
    implements RecordWriterProvider<S3SinkConnectorConfig> {

  private static final Logger log = LoggerFactory.getLogger(AvroRecordWriterProvider.class);
  private static final String EXTENSION = ".avro";
  private final S3Storage storage;
  private final AvroData avroData;

  AvroRecordWriterProvider(S3Storage storage, AvroData avroData) {
    this.storage = storage;
    this.avroData = avroData;
  }

  @Override
  public String getExtension() {
    return EXTENSION;
  }

  @Override
  public RecordWriter getRecordWriter(final S3SinkConnectorConfig conf, final String filename) {
    // This is not meant to be a thread-safe writer!
    return new S3RetriableRecordWriter(
        new IORecordWriter() {
          final String adjustedFilename = getAdjustedFilename(recordView, filename, getExtension());
          final DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>());
          Schema schema = null;
          S3OutputStream s3out;

          @Override
          public void write(SinkRecord record) throws IOException {
            if (schema == null) {
              schema = recordView.getViewSchema(record, false);
              log.info("Opening record writer for: {}", adjustedFilename);
              s3out = storage.create(adjustedFilename, true, AvroFormat.class);
              org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(schema);
              writer.setCodec(CodecFactory.fromString(conf.getAvroCodec()));
              writer.create(avroSchema, s3out);
            }
            log.trace("Sink record with view {}: {}", recordView,
                sinkRecordToLoggableString(record));
            Object value = avroData.fromConnectData(schema, recordView.getView(record, false));
            // AvroData wraps primitive types so their schema can be included. We need to unwrap
            // NonRecordContainers to just their value to properly handle these types
            if (value instanceof NonRecordContainer) {
              value = ((NonRecordContainer) value).getValue();
            }
            writer.append(value);
          }

          @Override
          public void commit() throws IOException {
            // Flush is required here, because closing the writer will close the underlying S3
            // output stream before committing any data to S3.
            writer.flush();
            s3out.commit();
            writer.close();
          }

          @Override
          public void close() throws IOException {
            writer.close();
          }
        }
    );
  }
}
