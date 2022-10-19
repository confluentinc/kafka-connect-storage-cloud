/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.connect.s3.format.parquet;

import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.storage.hive.HiveFactory;
import io.confluent.connect.storage.hive.HiveMetaStore;
import io.confluent.connect.storage.hive.HiveUtil;
import org.apache.kafka.common.config.AbstractConfig;

public class ParquetHiveFactory implements HiveFactory {
  @Override
  public HiveUtil createHiveUtil(
          AbstractConfig config,
      io.confluent.connect.storage.hive.HiveMetaStore hiveMetaStore
  ) {
    return createHiveUtil((S3SinkConnectorConfig) config, hiveMetaStore);
  }

  public HiveUtil createHiveUtil(S3SinkConnectorConfig config, HiveMetaStore hiveMetaStore) {
    return new ParquetHiveUtil(config, hiveMetaStore);
  }
}
