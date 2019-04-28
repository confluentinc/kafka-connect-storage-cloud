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

package io.confluent.connect.s3.format.json;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.storage.S3OutputStream;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.s3.util.JsonMapConverter;
import io.confluent.connect.s3.util.JsonObjectUtil;
import io.confluent.connect.storage.common.util.StringUtils;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

public class JsonRecordWriterProvider implements RecordWriterProvider<S3SinkConnectorConfig> {

  private static final Logger log = LoggerFactory.getLogger(JsonRecordWriterProvider.class);
  private static final String EXTENSION = ".json";
  private static final String LINE_SEPARATOR = System.lineSeparator();
  private static final byte[] LINE_SEPARATOR_BYTES
      = LINE_SEPARATOR.getBytes(StandardCharsets.UTF_8);
  private static final String PAYLOAD_FIELD_NAME = "payload";
  private static final String METADATA_FIELD_NAME = "metadata";
  private static final String CREATED_AT_FIELD_NAME = "created_at";
  private static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

  private final S3Storage storage;
  private final ObjectMapper mapper;
  private final JsonConverter converter;

  JsonRecordWriterProvider(S3Storage storage, JsonConverter converter) {
    this.storage = storage;
    this.mapper = new ObjectMapper();
    this.converter = converter;
  }

  @Override
  public String getExtension() {
    return EXTENSION + storage.conf().getCompressionType().extension;
  }

  private JSONObject generatePayloadMessage(JSONObject message,
                                            JSONObject metadata,
                                            S3SinkConnectorConfig conf) {
    try {
      if (StringUtils.isNotBlank(conf.getCopyMetadataFieldToMessages())) {
        Object copyValue = JsonObjectUtil.getOrDefault(
                metadata, conf.getCopyMetadataFieldToMessages(), null);
        message.put(conf.getCopyMetadataFieldToMessages(), copyValue);
      }
      if (StringUtils.isNotBlank(conf.getCreatedAtMetadataField())) {
        long epochTime = Long.valueOf(
                metadata.get(conf.getCreatedAtMetadataField()).toString()) * 1000;
        Date date = new Date(epochTime);
        DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
        String dateFormatted = dateFormat.format(date);
        message.put(CREATED_AT_FIELD_NAME, dateFormatted);
      }
      if (conf.getCoreOrderStalesConverter()) {
        message = generateOrderStalesMessage(message, metadata);
      }
    } catch (JSONException e) {
      throw new ConnectException(e);
    }

    return message;
  }

  private JSONObject generateOrderStalesMessage(JSONObject message, JSONObject metadata) {
    try {
      Object orderId = metadata.get("order_id");
      Object changedAt = metadata.get(CREATED_AT_FIELD_NAME);
      String[] messageParts = message.toString().replace("[","")
              .replace("]","")
              .split(",");
      String newValue = null;
      String oldValue = null;

      switch (messageParts[0]) {
        case "+":
          newValue = messageParts[2];
          break;
        case "-":
          oldValue = messageParts[2];
          break;
        case "~":
          oldValue = messageParts[2];
          newValue = messageParts[3];
          break;
        default:
          break;
      }

      String fieldName = messageParts[1];
      JSONObject newMessage = new JSONObject();
      newMessage.put("order_id", orderId);
      newMessage.put("change_at", changedAt);
      newMessage.put("field_name", fieldName);
      newMessage.put("old_value", oldValue);
      newMessage.put("new_value", newValue);

      return newMessage;
    } catch (Exception e) {
      throw new ConnectException(e);
    }
  }

  private void writeSingleItemPayloadRedshift(Object value,
                                              JsonGenerator writer,
                                              S3SinkConnectorConfig conf) {
    try {
      JSONObject payload =
              new JSONObject((String) ((HashMap)value).get(PAYLOAD_FIELD_NAME));
      JSONObject jsonObj = new JSONObject(String.valueOf(value));
      JSONObject metadata = jsonObj.getJSONObject(METADATA_FIELD_NAME);

      writer.writeObject(JsonMapConverter
              .toMap(generatePayloadMessage(payload, metadata, conf)));
      writer.writeRaw(LINE_SEPARATOR);
    } catch (Exception e) {
      throw new ConnectException(e);
    }
  }

  private void writePayloadRedshift(Object value,
                                    JsonGenerator writer,
                                    S3SinkConnectorConfig conf) {
    try {
      JSONArray payloadArr = null;
      try {
        payloadArr =
                new JSONArray((String) ((HashMap) value).get(PAYLOAD_FIELD_NAME));
      } catch (JSONException e) {
        log.error("---------------> Payload section is empty! <---------------" + e.getMessage());
      }

      if (payloadArr != null) {
        JSONObject jsonObj = new JSONObject(String.valueOf(value));
        JSONObject metadata = jsonObj.getJSONObject(METADATA_FIELD_NAME);

        for (int i = 0; i < payloadArr.length(); i++) {
          writer.writeObject(JsonMapConverter.toMap(
                  generatePayloadMessage(payloadArr.getJSONObject(i), metadata, conf)
          ));
          writer.writeRaw(LINE_SEPARATOR);
        }
      }
    } catch (Exception e) {
      throw new ConnectException(e);
    }
  }

  @Override
  public RecordWriter getRecordWriter(final S3SinkConnectorConfig conf, final String filename) {
    try {
      return new RecordWriter() {
        final S3OutputStream s3out = storage.create(filename, true);
        final OutputStream s3outWrapper = s3out.wrapForCompression();
        final JsonGenerator writer = mapper
                .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
                .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
                .setSerializationInclusion(JsonInclude.Include.NON_NULL)
                .setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
                .getFactory().createGenerator(s3outWrapper)
                .setRootValueSeparator(null);

        @Override
        public void write(SinkRecord record) {
          log.trace("Sink record: {}", record);
          try {
            Object value = record.value();
            if (value instanceof Struct) {
              byte[] rawJson = converter.fromConnectData(
                  record.topic(),
                  record.valueSchema(),
                  value
              );
              s3outWrapper.write(rawJson);
              s3outWrapper.write(LINE_SEPARATOR_BYTES);
            } else {
              if (conf.getWritePayloadRedshift() && !conf.getIsSingleItemPayloadRedshift()) {
                writePayloadRedshift(value, writer, conf);
              } else if (conf.getWritePayloadRedshift() && conf.getIsSingleItemPayloadRedshift()) {
                writeSingleItemPayloadRedshift(value, writer, conf);
              } else {
                writer.writeObject(value);
                writer.writeRaw(LINE_SEPARATOR);
              }
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
            s3outWrapper.close();
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
    } catch (IOException e) {
      throw new ConnectException(e);
    }
  }
}
