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

import org.junit.After;
import org.junit.Before;

import java.util.Map;

import io.confluent.connect.storage.StorageSinkTestBase;

public class S3SinkTestBase extends StorageSinkTestBase {

  protected static final String S3_TEST_URL = "http://localhost:8888";
  protected static final String S3_TEST_BUCKET_NAME = "kafka.bucket";

  protected S3SinkConnectorConfig connectorConfig;

  @Override
  protected Map<String, String> createProps() {
    url = S3_TEST_URL;
    Map<String, String> props = super.createProps();
    props.put(S3SinkConnectorConfig.S3_BUCKET_CONFIG, S3_TEST_BUCKET_NAME);
    props.put(S3SinkConnectorConfig.STORAGE_CLASS_CONFIG, "io.confluent.connect.s3.storage.S3Storage");
    return props;
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    connectorConfig = new S3SinkConnectorConfig(properties);
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }
}

