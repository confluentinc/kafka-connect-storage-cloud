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

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public final class RecordViews {
  public static final class KeyRecordView implements RecordView {
    @Override
    public Schema getViewSchema(SinkRecord record) {
      return record.keySchema();
    }

    @Override
    public Object getView(SinkRecord record) {
      return record.key();
    }
  }

  public static final class ValueRecordView implements RecordView {
    @Override
    public Schema getViewSchema(SinkRecord record) {
      return record.valueSchema();
    }

    @Override
    public Object getView(SinkRecord record) {
      return record.value();
    }
  }

  public static final class HeaderRecordView implements RecordView {
    private static final Schema SINGLE_HEADER_SCHEMA = SchemaBuilder.struct()
        .field("key", Schema.STRING_SCHEMA)
        .field("value", Schema.STRING_SCHEMA)
        .build();

    @Override
    public Schema getViewSchema(SinkRecord record) {
      return SchemaBuilder.array(SINGLE_HEADER_SCHEMA);
    }

    @Override
    public Object getView(SinkRecord record) {
      return StreamSupport.stream(record.headers().spliterator(), false)
          .map(h -> new Struct(SINGLE_HEADER_SCHEMA)
              .put("key", h.key())
              .put("value", Values.convertToString(h.schema(), h.value())))
          .collect(Collectors.toList());
    }
  }
}
