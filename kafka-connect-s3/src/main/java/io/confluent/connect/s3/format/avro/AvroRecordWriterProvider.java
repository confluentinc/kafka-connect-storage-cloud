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

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.storage.S3OutputStream;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.kafka.serializers.NonRecordContainer;

public class AvroRecordWriterProvider implements RecordWriterProvider<S3SinkConnectorConfig> {

  private static final Logger log = LoggerFactory.getLogger(AvroRecordWriterProvider.class);
  private static final String EXTENSION = ".avro";
  private final S3Storage storage;
  private final AvroData avroData;

  // Supported schema types if combine key and value
  private final List<Schema.Type> supportedSchemaTypes = Arrays.asList(
      Schema.Type.INT8,
      Schema.Type.INT16,
      Schema.Type.INT32,
      Schema.Type.INT64,
      Schema.Type.FLOAT32,
      Schema.Type.FLOAT64,
      Schema.Type.BOOLEAN,
      Schema.Type.STRING
  );

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
    return new RecordWriter() {
      final DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>());
      // Connect schemas
      Schema keySchema = null;
      Schema valueSchema = null;

      // Avro schemas
      org.apache.avro.Schema avroKeySchema = null;
      org.apache.avro.Schema avroValueSchema = null;
      org.apache.avro.Schema outputSchema = null;

      // Set isSinkKey based on config
      boolean isSinkKey = conf.getSinkKey();

      // S3 output stream
      S3OutputStream s3out;

      @Override
      public void write(SinkRecord record) {
        if (valueSchema == null) {
          keySchema = record.keySchema();//get the connect key schema
          valueSchema = record.valueSchema();//get the connect value schema

          try {
            log.info("Opening record writer for: {}", filename);
            s3out = storage.create(filename, true);
            avroKeySchema = avroData.fromConnectSchema(keySchema);//convert to avro key schema
            avroValueSchema = avroData.fromConnectSchema(valueSchema);//convert to avro value schema

            // Combine avro key schema and avro value schema
            if (isSinkKey) {
              // Customized behaviour
              outputSchema = mergeSchema(keySchema, valueSchema);
            } else {
              // Original behaviour
              outputSchema = avroValueSchema;
            }

            writer.setCodec(CodecFactory.fromString(conf.getAvroCodec()));
            writer.create(outputSchema, s3out);
          } catch (IOException e) {
            throw new ConnectException(e);
          }
        }

        log.trace("Sink record: {}", record);
        // Object is either NonRecordContainer or GenericData.Record
        Object key = avroData.fromConnectData(keySchema, record.key());
        Object value = avroData.fromConnectData(valueSchema, record.value());
        Object outputValue;

        if (isSinkKey) {
          // Customized behaviour
          outputValue = mergeKeyValue(key, value, avroKeySchema, avroValueSchema, outputSchema);
        } else {
          // Original behaviour
          outputValue = value;
        }

        try {
          // AvroData wraps primitive types so their schema can be included. We need to unwrap
          // NonRecordContainers to just their value to properly handle these types
          if (outputValue instanceof NonRecordContainer) {
            outputValue = ((NonRecordContainer) outputValue).getValue();
          }
          writer.append(outputValue);
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

  /**
   * Get output schema in avro format
   *
   * @param keySchema the connect schema of the key
   * @param valueSchema the connect schema of the value
   * @return output schema in avro format
   */
  private org.apache.avro.Schema mergeSchema(Schema keySchema, Schema valueSchema) {
    // Create the connect schema builder
    SchemaBuilder builder = SchemaBuilder
            .struct()
            .name(valueSchema.name())
            .doc(valueSchema.doc())
            .version(valueSchema.version());

    // Process the key schema
    if (keySchema.type() == Schema.Type.STRUCT) {
      for (Field field: keySchema.fields()) {
        builder.field(field.name(), field.schema());
      }
    } else if (supportedSchemaTypes.contains(keySchema.type())) {
      builder.field("key", keySchema);
    } else {
      throw new ConnectException(
              "Key type: " + keySchema.type().toString() + "not supported!"
      );
    }

    // Process the value schema
    if (valueSchema.type() == Schema.Type.STRUCT) {
      for (Field field: valueSchema.fields()) {
        builder.field(field.name(), field.schema());
      }
    } else if (supportedSchemaTypes.contains(valueSchema.type())) {
      builder.field("value", valueSchema);
    } else {
      throw new ConnectException(
              "Value type: " + valueSchema.type().toString() + "not supported!"
      );
    }

    // Create the connect output schema
    Schema schema = builder.build();

    // Return the output schema in avro format
    return avroData.fromConnectSchema(schema);
  }

  /**
   * Get output value in GenericData.Record
   *
   * @param key the key object
   * @param value the value object
   * @param avroKeySchema the avro schema of the key
   * @param avroValueSchema the avro schema of the value
   * @param outputAvroSchema the avro schema of the output
   * @return output value in GenericData.Record
   */
  private GenericData.Record mergeKeyValue(
          Object key,
          Object value,
          org.apache.avro.Schema avroKeySchema,
          org.apache.avro.Schema avroValueSchema,
          org.apache.avro.Schema outputAvroSchema) {
    // Initialize outputValue
    GenericData.Record outputValue = new GenericData.Record(outputAvroSchema);

    // Process the key object
    if (key instanceof NonRecordContainer) {
      outputValue.put("key", ((NonRecordContainer) key).getValue());
    } else if (key instanceof GenericData.Record) {
      for (org.apache.avro.Schema.Field field: avroKeySchema.getFields()) {
        String fieldName = field.name();
        outputValue.put(fieldName, ((GenericData.Record) key).get(fieldName));
      }
    } else {
      throw new ConnectException(
              "The message key is neither NonRecordContainer nor GenericData.Record"
      );
    }

    // Process the value object
    if (value instanceof NonRecordContainer) {
      outputValue.put("value", ((NonRecordContainer) value).getValue());
    } else if (value instanceof GenericData.Record) {
      for (org.apache.avro.Schema.Field field: avroValueSchema.getFields()) {
        String fieldName = field.name();
        outputValue.put(fieldName, ((GenericData.Record) value).get(fieldName));
      }
    } else {
      throw new ConnectException(
              "The message value is neither NonRecordContainer nor GenericData.Record"
      );
    }

    return outputValue;
  }
}
