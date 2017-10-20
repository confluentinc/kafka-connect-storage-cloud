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

package  io.confluent.connect.s3.format.avro;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;

import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.serializers.NonRecordContainer;

public class AvroUtils {

  public static Collection<Object> getRecords(InputStream in) throws IOException {
    DatumReader<Object> reader = new GenericDatumReader<>();
    DataFileStream<Object> streamReader = new DataFileStream<>(in, reader);

    ArrayList<Object> records = new ArrayList<>();
    while (streamReader.hasNext()) {
      records.add(streamReader.next());
    }
    return records;
  }

  public static byte[] putRecords(Collection<SinkRecord> records, AvroData avroData) throws IOException {
    final DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>());
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Schema schema = null;
    for (SinkRecord record : records) {
      if (schema == null) {
        schema = record.valueSchema();
        org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(schema);
        writer.create(avroSchema, out);
      }
      Object value = avroData.fromConnectData(schema, record.value());
      // AvroData wraps primitive types so their schema can be included. We need to unwrap
      // NonRecordContainers to just their value to properly handle these types
      if (value instanceof NonRecordContainer) {
        value = ((NonRecordContainer) value).getValue();
      }
      writer.append(value);
    }
    writer.flush();
    return out.toByteArray();
  }

}