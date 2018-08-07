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

package io.confluent.connect.gcs.format.avro;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.gcs.GcsSinkConnectorConfig;
import io.confluent.connect.gcs.storage.GcsOutputStream;
import io.confluent.connect.gcs.storage.GcsStorage;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.kafka.serializers.NonRecordContainer;

public class AvroRecordWriterProvider implements RecordWriterProvider<GcsSinkConnectorConfig> {

  private static final Logger log = LoggerFactory.getLogger(AvroRecordWriterProvider.class);
  private static final String EXTENSION = ".avro";
  private final GcsStorage storage;
  private final AvroData avroData;

  AvroRecordWriterProvider(GcsStorage storage, AvroData avroData) {
    this.storage = storage;
    this.avroData = avroData;
  }

  @Override
  public String getExtension() {
    return EXTENSION;
  }

  @Override
  public RecordWriter getRecordWriter(final GcsSinkConnectorConfig conf, final String filename) {
    // This is not meant to be a thread-safe writer!
    return new RecordWriter() {
      final DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>());
      Schema schema = null;
      GcsOutputStream gcsOS;

      @Override
      public void write(SinkRecord record) {
        if (schema == null) {
          schema = record.valueSchema();
          try {
            log.info("Opening record writer for: {}", filename);
            gcsOS = storage.create(filename, true);
            org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(schema);
            writer.setCodec(CodecFactory.fromString(conf.getAvroCodec()));
            writer.create(avroSchema, gcsOS);
          } catch (IOException e) {
            throw new ConnectException(e);
          }
        }
        log.trace("Sink record: {}", record);
        Object value = avroData.fromConnectData(schema, record.value());
        try {
          // AvroData wraps primitive types so their schema can be included. We need to unwrap
          // NonRecordContainers to just their value to properly handle these types
          if (value instanceof NonRecordContainer) {
            value = ((NonRecordContainer) value).getValue();
          }
          writer.append(value);
        } catch (IOException e) {
          throw new ConnectException(e);
        }
      }

      @Override
      public void commit() {
        try {
          // Flush is required here, because closing the writer will close the underlying GCS
          // output stream before committing any data to GCS.
          writer.flush();
          gcsOS.commit();
          writer.close();
        } catch (IOException e) {
          throw new ConnectException(e);
        }
      }

      @Override
      public void close() {
        try {
          writer.close();
        } catch (IOException e) {
          throw new ConnectException(e);
        }
      }
    };
  }
}
