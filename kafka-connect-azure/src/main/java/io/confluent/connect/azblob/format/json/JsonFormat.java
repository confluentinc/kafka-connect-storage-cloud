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

package io.confluent.connect.azblob.format.json;

import io.confluent.connect.azblob.AzBlobSinkConnectorConfig;
import io.confluent.connect.azblob.storage.AzBlobStorage;
import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.format.SchemaFileReader;
import io.confluent.connect.storage.hive.HiveFactory;
import org.apache.kafka.connect.json.JsonConverter;

import java.util.HashMap;
import java.util.Map;

public class JsonFormat implements Format<AzBlobSinkConnectorConfig, String> {
  private final AzBlobStorage storage;
  private final JsonConverter converter;

  public JsonFormat(AzBlobStorage storage) {
    this.storage = storage;
    this.converter = new JsonConverter();
    Map<String, Object> converterConfig = new HashMap<>();
    converterConfig.put("schemas.enable", "false");
    converterConfig.put("schemas.cache.size",
        String.valueOf(storage.conf().get(AzBlobSinkConnectorConfig.SCHEMA_CACHE_SIZE_CONFIG)));
    this.converter.configure(converterConfig, false);
  }

  @Override
  public RecordWriterProvider<AzBlobSinkConnectorConfig> getRecordWriterProvider() {
    return new JsonRecordWriterProvider(storage, converter);
  }

  @Override
  public SchemaFileReader<AzBlobSinkConnectorConfig, String> getSchemaFileReader() {
    throw new UnsupportedOperationException("Reading schemas from blob is not currently supported");
  }

  @Override
  public HiveFactory getHiveFactory() {
    throw new UnsupportedOperationException(
        "Hive integration is not currently supported in blob Connector");
  }

}
