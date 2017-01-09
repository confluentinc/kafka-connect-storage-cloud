/*
 * Copyright 2016 Confluent Inc.
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

import org.apache.kafka.common.config.ConfigException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;

public class S3SinkConnectorConfigTest extends S3SinkConnectorTestBase {

  @Test
  public void testStorageClass() {
    // No real test case yet
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertEquals("io.confluent.connect.s3.storage.S3Storage", connectorConfig.getString(S3SinkConnectorConfig.STORAGE_CLASS_CONFIG));
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testUndefinedURL() {
    properties.remove(S3SinkConnectorConfig.STORE_URL_CONFIG);
    thrown.expect(ConfigException.class);
    thrown.expectMessage("Missing required configuration \"" + S3SinkConnectorConfig.STORE_URL_CONFIG
                         + "\" which has no default value.");
    connectorConfig = new S3SinkConnectorConfig(properties);
  }

}

