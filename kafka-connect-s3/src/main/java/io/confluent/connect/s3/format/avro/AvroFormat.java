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

package io.confluent.connect.s3.format.avro;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.format.SchemaFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroFormat implements Format<S3SinkConnectorConfig, String> {
  private static final Logger log = LoggerFactory.getLogger(AvroFormat.class);
  private final S3Storage storage;
  private final AvroData avroData;

  public AvroFormat(S3Storage storage) {
    this.storage = storage;
    this.avroData = new AvroData(storage.conf().avroDataConfig());
  }

  @Override
  public RecordWriterProvider<S3SinkConnectorConfig> getRecordWriterProvider() {
    return new AvroRecordWriterProvider(storage, avroData);
  }

  @Override
  public SchemaFileReader<S3SinkConnectorConfig, String> getSchemaFileReader() {
    log.debug("Reading schemas from S3 is not currently supported");
    throw new UnsupportedOperationException("Reading schemas from S3 is not currently supported");
  }

  @Override
  @Deprecated
  public Object getHiveFactory() {
    log.debug("Hive integration is not currently supported in S3 Connector");
    throw new UnsupportedOperationException(
        "Hive integration is not currently supported in S3 Connector"
    );
  }

  public AvroData getAvroData() {
    return avroData;
  }
}
