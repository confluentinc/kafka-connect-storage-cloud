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

package io.confluent.connect.s3;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import java.util.Map;

public class MockS3SinkConnectorConfig extends S3SinkConnectorConfig {
  public static final String TEST_PART_SIZE_CONFIG = "test.s3.part.size";

  public static ConfigDef newConfigDef() {
    ConfigDef configDef = S3SinkConnectorConfig.getConfig();
    final String group = "S3 Tests";
    int orderInGroup = 0;
    configDef.define(TEST_PART_SIZE_CONFIG,
                      Type.INT,
                      1024,
                      Importance.HIGH,
                      "Tests - The Part Size in S3 Multi-part Uploads for tests.",
                      group,
                      ++orderInGroup,
                      Width.MEDIUM,
                      "Tests - S3 Part Size for tests");
    return configDef;
  }

  public MockS3SinkConnectorConfig(Map<String, String> props) {
    super(newConfigDef(), props);
  }

  @Override
  public int getPartSize() {
    // Depends on the following test-internal property to override partSize for tests.
    // If not set, throws an exception.
    return getInt(TEST_PART_SIZE_CONFIG);
  }
}
