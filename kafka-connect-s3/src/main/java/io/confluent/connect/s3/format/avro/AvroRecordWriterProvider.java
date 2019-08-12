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

package io.confluent.connect.s3.format.avro;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.storage.S3OutputStream;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.kafka.serializers.NonRecordContainer;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AvroRecordWriterProvider implements RecordWriterProvider<S3SinkConnectorConfig> {

  private static final Logger log = LoggerFactory.getLogger(AvroRecordWriterProvider.class);
  private static final String EXTENSION = ".avro";
  private final S3Storage storage;
  private final AvroData avroData;

  private org.apache.avro.Schema linkingSchema = SchemaBuilder.record("linkingOutput")
          .fields()
          .name("shop_url").type().stringType().noDefault()
          .name("orig_id").type().stringType().noDefault()
          .name("second_shop_url").type().stringType().noDefault()
          .name("second_orig_id").type().stringType().noDefault()
          .name("group_name").type().stringType().noDefault()
          .name("email").type().booleanType().booleanDefault(false)
          .name("ip").type().booleanType().booleanDefault(false)
          .name("cartToken").type().booleanType().booleanDefault(false)
          .name("cookieQuery").type().booleanType().booleanDefault(false)
          .name("creditCardBin").type().booleanType().booleanDefault(false)
          .name("creditCardNumber").type().booleanType().booleanDefault(false)
          .name("customerId").type().booleanType().booleanDefault(false)
          .name("passengerDateOfBirth").type().booleanType().booleanDefault(false)
          .name("passengerName").type().booleanType().booleanDefault(false)
          .name("billing_shipping_City").type().booleanType().booleanDefault(false)
          .name("billing_shipping_Country_Code").type().booleanType().booleanDefault(false)
          .name("billing_shipping_Name").type().booleanType().booleanDefault(false)
          .name("billing_shipping_Phone").type().booleanType().booleanDefault(false)
          .name("billing_shipping_ProvinceCode").type().booleanType().booleanDefault(false)
          .name("billing_shipping_StreetAddress_1_2").type().booleanType().booleanDefault(false)
          .name("billing_shipping_ZipQuery").type().booleanType().booleanDefault(false)
          .endRecord(); //todo use schema registry to define the schema

  private List<String> supportedQueries = linkingSchema.getFields().stream()
          .filter(f -> f.schema().getType().equals(org.apache.avro.Schema.Type.BOOLEAN))
          .map(f -> f.name()).collect(Collectors.toList());

  AvroRecordWriterProvider(S3Storage storage, AvroData avroData) {
    this.storage = storage;
    this.avroData = avroData;
  }

  @Override
  public String getExtension() {
    return EXTENSION;
  }

  private void writeLinkingMessage(Object value, DataFileWriter<Object> writer) throws IOException {
    try {
      Record v = (Record) value;
      String shopUrl = (String) ((Record) v.get("shopUrl")).get("value");
      String origId = (String) ((Record) v.get("origId")).get("value");
      Map groups = (Map<String, List<Record>>) v.get("groups");
      String group = (String) groups.keySet().toArray()[0];
      List<Record> links = (List<Record>) groups.get(group);
      for (Record l : links) {
        String shopUrl2 = (String) ((Record) l.get("shopUrl")).get("value");
        String origId2 = (String) ((Record) l.get("origId")).get("value");
        List<String> queries = ((List<Record>) l.get("queries"))
                .stream()
                .map(q -> ((String) q.get("value")).replaceAll("[ \\\\+]", "_"))
                .collect(Collectors.toList());
        Record newRecord = new Record(linkingSchema);
        newRecord.put("shop_url", shopUrl);
        newRecord.put("orig_id", origId);
        newRecord.put("second_shop_url", shopUrl2);
        newRecord.put("second_orig_id", origId2);
        newRecord.put("group_name", group);
        for (String query : supportedQueries) {
          newRecord.put(query, queries.contains(query));
        }
        writer.append(newRecord);
      }
    }
    catch (ClassCastException e) {
      log.error("skipping message - failed to cast : " + e);
    }
    catch (NullPointerException e1){
      log.error("skipping message - bad structure : " + e1);
    }
  }

  @Override
  public RecordWriter getRecordWriter(final S3SinkConnectorConfig conf, final String filename) {
    // This is not meant to be a thread-safe writer!
    return new RecordWriter() {
      final DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>());
      Schema schema = null;
      S3OutputStream s3out;

      @Override
      public void write(SinkRecord record) {
        if (schema == null) {
          schema = record.valueSchema();
          try {
            log.info("Opening record writer for: {}", filename);
            s3out = storage.create(filename, true);
            org.apache.avro.Schema avroSchema;
            if (conf.getLinkingServiceResultsConverter()) {
              avroSchema = linkingSchema;
            } else {
              avroSchema = avroData.fromConnectSchema(schema);
            }
            writer.setCodec(CodecFactory.fromString(conf.getAvroCodec()));
            writer.create(avroSchema, s3out);
          } catch (IOException e) {
            throw new ConnectException(e);
          }
        }
        log.trace("Sink record: {}", record);
        Object value = avroData.fromConnectData(schema, record.value());
        try {
          // AvroData wraps primitive types so their schema can be included. We need to unwrap
          // NonRecordContainers to just their value to properly handle these types
          if (conf.getLinkingServiceResultsConverter()) {
            writeLinkingMessage(value, writer);
          } else {
            if (value instanceof NonRecordContainer) {
              value = ((NonRecordContainer) value).getValue();
            }
            writer.append(value);
          }
        } catch (IOException e) {
          throw new ConnectException(e);
        }
      }

      @Override
      public void commit() {
        try {
          // Flush is required here, because closing the writer will close the underlying S3
          // output stream before committing any data to S3.
          writer.flush();
          s3out.commit();
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
