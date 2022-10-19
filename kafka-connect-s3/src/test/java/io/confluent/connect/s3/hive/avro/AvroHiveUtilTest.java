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

package io.confluent.connect.s3.hive.avro;

import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.format.avro.AvroFormat;
import io.confluent.connect.s3.format.avro.AvroHiveUtil;
import io.confluent.connect.s3.hive.HiveUtilTestBase;
import io.confluent.connect.storage.hive.HiveUtil;
import java.util.Map;

public class AvroHiveUtilTest extends HiveUtilTestBase<AvroFormat> {

  public AvroHiveUtilTest(String hiveTableNameConfig) {
    super(AvroFormat.class, hiveTableNameConfig);
  }

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.put(S3SinkConnectorConfig.FORMAT_CLASS_CONFIG, AvroFormat.class.getName());
    return props;
  }

  @Override
  protected HiveUtil createHiveUtil() {
    return new AvroHiveUtil(connectorConfig, avroData, hiveMetaStore);
  }
}
