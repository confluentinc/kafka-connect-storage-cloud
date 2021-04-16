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

import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.format.SchemaFileReader;

import java.util.HashMap;
import java.util.Map;

public class CsvFormat implements Format<S3SinkConnectorConfig, String> {

  private final S3Storage storage;
  private final CsvConverter converter;

  public CsvFormat(S3Storage storage) {
    this.storage = storage;
    this.converter = new CsvConverter();
    Map<String, Object> converterConfig = new HashMap<>();
    this.converter.configure(storage.conf().originals() != null ?
            storage.conf().originals() : converterConfig, false);
  }

  @Override
  public RecordWriterProvider<S3SinkConnectorConfig> getRecordWriterProvider() {
    return new CsvRecordWriterProvider(storage, converter);
  }

  @Override
  public SchemaFileReader<S3SinkConnectorConfig, String> getSchemaFileReader() {
    throw new UnsupportedOperationException("Reading schemas from S3 is not currently supported");
  }

  @Override
  @Deprecated
  public Object getHiveFactory() {
    throw new UnsupportedOperationException(
            "Hive integration is not currently supported in S3 Connector"
    );
  }
}
