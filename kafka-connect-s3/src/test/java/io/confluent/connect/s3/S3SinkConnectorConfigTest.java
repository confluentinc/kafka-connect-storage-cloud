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

import org.junit.Before;
import org.junit.Test;

import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.common.StorageCommonConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class S3SinkConnectorConfigTest extends S3SinkConnectorTestBase {

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Test
  public void testStorageClass() throws Exception {
    // No real test case yet
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertEquals(S3Storage.class,
                 connectorConfig.getClass(StorageCommonConfig.STORAGE_CLASS_CONFIG));
  }

  @Test
  public void testUndefinedURL() throws Exception {
    properties.remove(StorageCommonConfig.STORE_URL_CONFIG);
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertNull(connectorConfig.getString(StorageCommonConfig.STORE_URL_CONFIG));
  }

}

