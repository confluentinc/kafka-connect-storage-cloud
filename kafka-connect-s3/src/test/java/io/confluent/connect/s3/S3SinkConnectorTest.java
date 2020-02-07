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

import com.amazonaws.AmazonServiceException;
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.mockito.PowerMockito.doReturn;

@RunWith(PowerMockRunner.class)
@PrepareForTest({S3SinkConnector.class, AmazonServiceException.class})
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

  @Test(expected = ConnectException.class)
  public void checkNonExistentBucket() throws Exception {
    configProps.put(S3SinkConnectorConfig.S3_BUCKET_CONFIG, "non-existent-bucket");
    connector = spy(new S3SinkConnector(new S3SinkConnectorConfig(configProps)));
    doReturn(false).when(connector, "checkBucketExists", Mockito.any());
    connector.validate(configProps);
  }

  @Test()
  public void checkExistentBucket() throws Exception {
    configProps.put(S3SinkConnectorConfig.S3_BUCKET_CONFIG, "existent-bucket");
    connector = spy(new S3SinkConnector(new S3SinkConnectorConfig(configProps)));
    doReturn(true).when(connector, "checkBucketExists", Mockito.any());
    connector.validate(configProps);
  }
}

