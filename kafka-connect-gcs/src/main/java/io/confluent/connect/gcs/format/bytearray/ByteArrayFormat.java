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

package io.confluent.connect.gcs.format.bytearray;

import org.apache.kafka.connect.converters.ByteArrayConverter;

import java.util.HashMap;
import java.util.Map;

import io.confluent.connect.gcs.GcsSinkConnectorConfig;
import io.confluent.connect.gcs.storage.GcsStorage;
import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.format.SchemaFileReader;
import io.confluent.connect.storage.hive.HiveFactory;

public class ByteArrayFormat implements Format<GcsSinkConnectorConfig, String> {
  private final GcsStorage storage;
  private final ByteArrayConverter converter;

  public ByteArrayFormat(GcsStorage storage) {
    this.storage = storage;
    this.converter = new ByteArrayConverter();
    Map<String, Object> converterConfig = new HashMap<>();
    this.converter.configure(converterConfig, false);
  }

  @Override
  public RecordWriterProvider<GcsSinkConnectorConfig> getRecordWriterProvider() {
    return new ByteArrayRecordWriterProvider(storage, converter);
  }

  @Override
  public SchemaFileReader<GcsSinkConnectorConfig, String> getSchemaFileReader() {
    throw new UnsupportedOperationException("Reading schemas from GCS is not currently supported");
  }

  @Override
  public HiveFactory getHiveFactory() {
    throw new UnsupportedOperationException(
        "Hive integration is not currently supported in GCS Connector");
  }

}
