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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.regions.Regions;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.confluent.connect.storage.StorageSinkConnectorConfig;
import io.confluent.connect.storage.common.ComposableConfig;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.hive.HiveConfig;
import io.confluent.connect.storage.partitioner.PartitionerConfig;

public class MockS3SinkConnectorConfig extends S3SinkConnectorConfig {
  public static final String TEST_PART_SIZE_CONFIG = "test.s3.part.size";

  static {
    final String group = "S3 Tests";
    int orderInGroup = 0;
    CONFIG_DEF.define(TEST_PART_SIZE_CONFIG,
                      Type.INT,
                      1024,
                      Importance.HIGH,
                      "Tests - The Part Size in S3 Multi-part Uploads for tests.",
                      group,
                      ++orderInGroup,
                      Width.MEDIUM,
                      "Tests - S3 Part Size for tests");
  }

  public MockS3SinkConnectorConfig(Map<String, String> props) {
    super(props);
  }

  @Override
  public int getPartSize() {
    // Depends on the following test-internal property to override partSize for tests.
    // If not set, throws an exception.
    return getInt(TEST_PART_SIZE_CONFIG);
  }
}
