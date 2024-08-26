/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.connect.s3.format;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.sink.SinkRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public final class RecordViews {

  private static final Logger log = LoggerFactory.getLogger(RecordViews.class);

  public static final class ValueRecordView extends BaseRecordView {
    @Override
    public Schema getViewSchema(SinkRecord record, boolean enveloped) {
      return record.valueSchema();
    }

    @Override
    public Object getView(SinkRecord record, boolean enveloped) {
      return record.value();
    }

    @Override
    public String getExtension() {
      return "";
    }
  }

  public static final class KeyRecordView extends BaseRecordView {
    private static final String KEY_FIELD_NAME = "key";
    private static final String KEY_STRUCT_NAME = "RecordKey";

    @Override
    public Schema getViewSchema(SinkRecord record, boolean enveloped) {
      Schema keySchema = record.keySchema();
      if (enveloped) {
        keySchema = SchemaBuilder.struct().name(KEY_STRUCT_NAME)
            .field(KEY_FIELD_NAME, keySchema).build();
      }
      log.debug("Created key record view schema.");
      return keySchema;
    }

    @Override
    public Object getView(SinkRecord record, boolean enveloped) {
      Object view =  record.key();
      if (enveloped) {
        view = new Struct(getViewSchema(record, true)).put(KEY_FIELD_NAME, view);
      }
      return view;
    }

    @Override
    public String getExtension() {
      return ".keys";
    }
  }

  public static final class HeaderRecordView extends BaseRecordView {
    private static final String HEADER_FIELD_NAME = "headers";
    private static final String HEADER_STRUCT_NAME = "RecordHeaders";

    // VisibleForTesting
    static final Schema SINGLE_HEADER_SCHEMA = SchemaBuilder.struct()
        .field("key", Schema.STRING_SCHEMA)
        .field("value", Schema.STRING_SCHEMA)
        .build();

    @Override
    public Schema getViewSchema(SinkRecord record, boolean enveloped) {
      Schema headerSchema = SchemaBuilder.array(SINGLE_HEADER_SCHEMA).build();
      if (enveloped) {
        headerSchema = SchemaBuilder.struct().name(HEADER_STRUCT_NAME)
            .field(HEADER_FIELD_NAME, headerSchema).build();
      }
      log.debug("Created header record view schema.");
      return headerSchema;
    }

    @Override
    public Object getView(SinkRecord record, boolean enveloped) {
      Object view = StreamSupport.stream(record.headers().spliterator(), false)
          .map(h -> new Struct(SINGLE_HEADER_SCHEMA)
              .put("key", h.key())
              .put("value", Values.convertToString(h.schema(), h.value())))
          .collect(Collectors.toList());

      if (enveloped) {
        view = new Struct(getViewSchema(record, true)).put(HEADER_FIELD_NAME, view);
      }
      return view;
    }

    @Override
    public String getExtension() {
      return ".headers";
    }
  }

  private abstract static class BaseRecordView implements RecordView {
    @Override
    public String toString() {
      return this.getClass().getSimpleName();
    }
  }
}
