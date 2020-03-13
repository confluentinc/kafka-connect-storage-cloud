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

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.Map;

import io.confluent.connect.s3.util.S3BucketCheck;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({S3BucketCheck.class})
@PowerMockIgnore({"io.findify.s3mock.*", "akka.*", "javax.*", "org.xml.*", "com.sun.org.apache" +
        ".xerces.*"})
public class S3SinkConnectorTest extends S3SinkConnectorTestBase {

  private Map<String, String> configProps = new HashMap<>();
  private S3SinkConnector connector;

  @Before
  @Override
  public void setUp() {
    configProps = super.createProps();
  }

  @Test
  public void testVersion() {
    String version = new S3SinkConnector().version();
    assertNotNull(version);
    assertFalse(version.isEmpty());
  }

  @Test
  public void connectorType() {
    Connector connector = new S3SinkConnector();
    assertTrue(SinkConnector.class.isAssignableFrom(connector.getClass()));
  }

  @Test
  public void checkNonExistentBucket() {
    configProps.put(S3SinkConnectorConfig.S3_BUCKET_CONFIG, "non-existent-bucket");
    connector = spy(new S3SinkConnector(new S3SinkConnectorConfig(configProps)));
    mockStatic(S3BucketCheck.class);
    when(S3BucketCheck.checkBucketExists(Mockito.any())).thenReturn(false);
    Config config = connector.validate(configProps);
    for (ConfigValue configValue : config.configValues()) {
      if (configValue.name().equals(S3SinkConnectorConfig.S3_BUCKET_CONFIG)) {
        assertFalse(configValue.errorMessages().isEmpty());
      }
    }
  }

  @Test
  public void checkExistentBucket() {
    configProps.put(S3SinkConnectorConfig.S3_BUCKET_CONFIG, "existent-bucket");
    connector = spy(new S3SinkConnector(new S3SinkConnectorConfig(configProps)));
    mockStatic(S3BucketCheck.class);
    when(S3BucketCheck.checkBucketExists(Mockito.any())).thenReturn(true);
    Config config = connector.validate(configProps);
    for (ConfigValue configValue : config.configValues()) {
      if (configValue.name().equals(S3SinkConnectorConfig.S3_BUCKET_CONFIG)) {
        assertTrue(configValue.errorMessages().isEmpty());
      }
    }
  }
}

