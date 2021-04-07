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

package io.confluent.connect.s3.format.csv;

import io.confluent.connect.storage.common.util.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;

import java.nio.charset.Charset;
import java.util.Date;
import java.util.Map;
import java.util.regex.Pattern;

public class CsvConverter implements Converter, HeaderConverter {

  private static final ConfigDef CONFIG_DEF = CsvConverterConfig.configDef();
  private static final Pattern CASE_CHANGE_PATTERN = Pattern.compile("([a-z])([A-Z])");
  private CsvConverterConfig config;
  private String fieldSeparator = ",";
  private Schema lastSchema;

  public CsvConverter() {
  }

  public ConfigDef config() {
    return CONFIG_DEF;
  }

  public void configure(Map<String, ?> configs) {
    this.config = new CsvConverterConfig(configs);
    this.fieldSeparator = config.getString("csv.field.sep");
  }

  public void configure(Map<String, ?> configs, boolean isKey) {
    configure(configs);
  }

  public byte[] fromConnectData(String topic, Schema schema, Object value) {
    if (value != null && schema == null) {
      throw new DataException("CSVConverter requires schema to be set");
    } else {
      // TODO: allow to specify charset if needed.
      if (schema.type() == Schema.Type.STRUCT) {
        this.lastSchema = schema;
      }
      return toCsvData(schema,
              value == null ? schema.defaultValue() : value).getBytes(Charset.defaultCharset());
    }
  }

  public byte[] getHeader() {
    if (lastSchema == null) {
      return null;
    }
    return schemaToHeader("", lastSchema).getBytes(Charset.defaultCharset());
  }

  private String schemaToHeader(String prefix, Schema schema) {
    switch (schema.type()) {
      case STRUCT:
        StringBuilder builder = new StringBuilder();
        for (Field f : schema.fields()) {
          if (builder.length() > 0) {
            builder.append(this.fieldSeparator);
          }
          if (StringUtils.isBlank(prefix)) {
            builder.append(schemaToHeader(toSnakeCase(f.name()), f.schema()));
          } else {
            builder.append(schemaToHeader(toSnakeCase(prefix + "_" + f.name()), f.schema()));
          }
        }
        return builder.toString();
      default:
        if (prefix.isEmpty()) {
          return prefix;
        } else {
          return "\"" + prefix + "\"";
        }
    }
  }


  private String toCsvData(Schema schema, Object value) {
    if (schema == null) {
      return "";
    }
    switch (schema.type()) {
      case STRUCT:
        return structToString(schema, value);
      case MAP:
        throw new DataException("Map is not supported");
      case INT32:
      case INT64:
        if (value != null) {
          return intToString(schema, value);
        } else {
          return "";
        }
      default:
        if (value != null) {
          return addQuotes(value);
        } else {
          return "";
        }
    }
  }

  private String structToString(Schema schema, Object value) {
    Struct struct = (Struct) value;
    StringBuilder buf = null;
    for (Field f : schema.fields()) {
      if (buf == null) {
        buf = new StringBuilder();
      } else {
        buf.append(this.fieldSeparator);
      }
      buf.append(toCsvData(f.schema(), struct != null ? struct.get(f) : null));
    }
    return buf == null ? "" : buf.toString();
  }

  private String intToString(Schema schema, Object value) {
    if (schema.name() != null && (
            schema.name().equals(Timestamp.LOGICAL_NAME)
            || schema.name().equals(Time.LOGICAL_NAME)
            || schema.name().equals(org.apache.kafka.connect.data.Date.LOGICAL_NAME)
        )) {
      return addQuotes(((Date)value).toInstant());
    } else {
      return addQuotes(value);
    }
  }

  public SchemaAndValue toConnectData(String topic, byte[] value) {
    throw new UnsupportedOperationException("Converting bytes to connect data "
            + "is not yet supported. This is converter only for Sink connector.");
  }

  public byte[] fromConnectHeader(String topic, String headerKey, Schema schema, Object value) {
    return this.fromConnectData(topic, schema, value);
  }

  public SchemaAndValue toConnectHeader(String topic, String headerKey, byte[] value) {
    return this.toConnectData(topic, value);
  }

  public void close() {
    // do nothing
  }

  private String toSnakeCase(String name) {
    return CASE_CHANGE_PATTERN.matcher(name).replaceAll("$1_$2").toLowerCase();
  }

  private String addQuotes(Object value) {
    return "\"" + String.valueOf(value).replaceAll("\"", "\"\"") + "\"";
  }

}
