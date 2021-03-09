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

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.regex.Pattern;

public class CsvConverter implements Converter, HeaderConverter {

  private static final ConfigDef CONFIG_DEF = CsvConverterConfig.configDef();
  private static final Pattern CASE_CHANGE_PATTERN = Pattern.compile("([a-z])(A-Z)");
  private CsvConverterConfig config;
  private String fieldSeparator;
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
      return toCsvData(schema,
              value == null ? schema.defaultValue() : value).getBytes(Charset.defaultCharset());
    }
  }

  public byte[] getHeader() {
    if (lastSchema == null) {
      return null;
    }
    // TODO: allow to specify charset if needed in future.
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
          builder.append('"');
          builder.append(schemaToHeader(toSnakeCase(f.name()), f.schema()));
          builder.append('"');
        }
        return builder.toString();
      default:
        if (StringUtils.isBlank(prefix)) {
          return toSnakeCase(schema.name());
        } else {
          return prefix + "_" + schema.name();
        }
    }
  }


  private String toCsvData(Schema schema, Object value) {
    this.lastSchema = schema;
    if (schema == null) {
      return "";
    }
    switch (schema.type()) {
      case STRUCT:
        Struct struct = (Struct) value;
        if (!struct.schema().equals(schema)) {
          throw new DataException("Mismatching schema.");
        }
        StringBuilder buf = new StringBuilder();
        for (Field f : struct.schema().fields()) {
          if (buf.length() > 0) {
            buf.append(this.fieldSeparator);
          }
          buf.append(toCsvData(f.schema(), struct.get(f)));
        }
        return buf.toString();
      case MAP:
        throw new DataException("Map is not supported");
      default:
        if (value != null) {
          addQuotes(value);
        } else return "";
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
