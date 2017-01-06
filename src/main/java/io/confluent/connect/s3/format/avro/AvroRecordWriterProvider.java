/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.confluent.connect.s3.format.avro;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.s3.storage.S3StorageConfig;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;

public class AvroRecordWriterProvider implements RecordWriterProvider<S3StorageConfig, AvroData> {

  private static final Logger log = LoggerFactory.getLogger(AvroRecordWriterProvider.class);
  private static final String EXTENSION = ".avro";
  private S3Storage storage;

  AvroRecordWriterProvider(S3Storage storage) {
    this.storage = storage;
  }

  @Override
  public String getExtension() {
    return EXTENSION;
  }

  @Override
  public RecordWriter<SinkRecord> getRecordWriter(S3StorageConfig conf, final String filename, SinkRecord record,
                                                  final AvroData avroData) {
    DatumWriter<Object> datumWriter = new GenericDatumWriter<>();
    final DataFileWriter<Object> writer = new DataFileWriter<>(datumWriter);

    final Schema schema = record.valueSchema();
    try {
      OutputStream wrapper = storage.create(filename, conf, true);
      org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(schema);
      writer.create(avroSchema, wrapper);
    } catch (IOException e) {
      throw new ConnectException(e);
    }

    return new RecordWriter<SinkRecord>() {
      @Override
      public void write(SinkRecord record) {
        log.trace("Sink record: {}", record.toString());
        Object value = avroData.fromConnectData(schema, record.value());
        try {
          writer.append(value);
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
