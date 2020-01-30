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

package io.confluent.connect.s3;

import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

public class S3SinkConnectorTest {

  private Map<String, String> properties = new HashMap();
  private S3SinkConnector connector = PowerMockito.spy(new S3SinkConnector());
  private boolean testPassed;

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
  public void testInvalidBucketName() {
    testPassed = true;
    properties.put(S3SinkConnectorConfig.S3_BUCKET_CONFIG, "test_bucket");

    try {
      connector.validate(properties);
    } catch (ConnectException e) {
      testPassed = false;
    }
    assertFalse(testPassed);
  }

  @Test
  public void testBucketWithValidNameWhichExists() {
    testPassed = true;
    properties.put(S3SinkConnectorConfig.S3_BUCKET_CONFIG, "test-bucket");
    PowerMockito.doReturn(true).when(connector).checkBucketExists(Mockito.anyString());

    try {
      connector.validate(properties);
    } catch (ConnectException e) {
      testPassed = false;
    }
    assertTrue(testPassed);
  }

  @Test
  public void testBucketWithValidNameAndDoesNotExists() {
    testPassed = true;
    properties.put(S3SinkConnectorConfig.S3_BUCKET_CONFIG, "test-bucket");
    PowerMockito.doReturn(false).when(connector).checkBucketExists(Mockito.anyString());

    try {
      connector.validate(properties);
    } catch (ConnectException e) {
      testPassed = false;
    }
    assertFalse(testPassed);
  }
}

