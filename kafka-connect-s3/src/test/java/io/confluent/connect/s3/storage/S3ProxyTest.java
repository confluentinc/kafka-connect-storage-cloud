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

package io.confluent.connect.s3.storage;

import com.amazonaws.Protocol;
import org.apache.kafka.common.config.ConfigException;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.services.s3.S3Client;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.S3SinkConnectorTestBase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class S3ProxyTest extends S3SinkConnectorTestBase {

  /*protected S3Storage storage;
  protected S3Client s3;
  protected ClientOverrideConfiguration clientConfig;
  protected Map<String, String> localProps = new HashMap<>();

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.putAll(localProps);
    return props;
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    storage = new S3Storage(connectorConfig, url, S3_TEST_BUCKET_NAME, null);
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    localProps.clear();
  }

  @Test
  public void testNoProxy() throws Exception {
    setUp();
    clientConfig = storage.newClientConfiguration(connectorConfig);
    assertEquals(Protocol.HTTPS, clientConfig.getProtocol());
    assertEquals(null, clientConfig.getProxyHost());
    assertEquals(-1, clientConfig.getProxyPort());
    assertEquals(null, clientConfig.getProxyUsername());
    assertEquals(null, clientConfig.getProxyPassword());
  }

  @Test
  public void testNoProtocolThrowsException() throws Exception {
    localProps.put(S3SinkConnectorConfig.S3_PROXY_URL_CONFIG, "localhost");
    setUp();
    assertThrows("no protocol: localhost", ConfigException.class, () -> storage.newClientConfiguration(connectorConfig));
  }

  @Test
  public void testUnknownProtocolThrowsException() throws Exception {
    localProps.put(S3SinkConnectorConfig.S3_PROXY_URL_CONFIG, "unknown://localhost");
    setUp();
    assertThrows("unknown protocol: localhost", ConfigException.class, () -> storage.newClientConfiguration(connectorConfig));
  }

  @Test
  public void testProtocolOnly() throws Exception {
    localProps.put(S3SinkConnectorConfig.S3_PROXY_URL_CONFIG, "http://");
    setUp();
    clientConfig = storage.newClientConfiguration(connectorConfig);
    assertEquals(Protocol.HTTP, clientConfig.getProtocol());
    assertEquals("", clientConfig.getProxyHost());
    assertEquals(-1, clientConfig.getProxyPort());
    assertEquals(null, clientConfig.getProxyUsername());
    assertEquals(null, clientConfig.getProxyPassword());
  }

  @Test
  public void testProtocolHostOnly() throws Exception {
    localProps.put(S3SinkConnectorConfig.S3_PROXY_URL_CONFIG, "http://localhost");
    setUp();
    clientConfig = storage.newClientConfiguration(connectorConfig);
    assertEquals(Protocol.HTTP, clientConfig.getProtocol());
    assertEquals("localhost", clientConfig.getProxyHost());
    assertEquals(-1, clientConfig.getProxyPort());
    assertEquals(null, clientConfig.getProxyUsername());
    assertEquals(null, clientConfig.getProxyPassword());
  }

  @Test
  public void testProtocolIpOnly() throws Exception {
    localProps.put(S3SinkConnectorConfig.S3_PROXY_URL_CONFIG, "http://127.0.0.1");
    setUp();
    clientConfig = storage.newClientConfiguration(connectorConfig);
    assertEquals(Protocol.HTTP, clientConfig.getProtocol());
    assertEquals("127.0.0.1", clientConfig.getProxyHost());
    assertEquals(-1, clientConfig.getProxyPort());
    assertEquals(null, clientConfig.getProxyUsername());
    assertEquals(null, clientConfig.getProxyPassword());
  }

  @Test
  public void testUnknownHostOnly() throws Exception {
    localProps.put(S3SinkConnectorConfig.S3_PROXY_URL_CONFIG, "http://255.255.255.255");
    setUp();
    clientConfig = storage.newClientConfiguration(connectorConfig);
    assertEquals(Protocol.HTTP, clientConfig.getProtocol());
    assertEquals("255.255.255.255", clientConfig.getProxyHost());
    assertEquals(-1, clientConfig.getProxyPort());
    assertEquals(null, clientConfig.getProxyUsername());
    assertEquals(null, clientConfig.getProxyPassword());
  }

  @Test
  public void testProtocolPort() throws Exception {
    localProps.put(S3SinkConnectorConfig.S3_PROXY_URL_CONFIG, "http://:8080");
    setUp();
    clientConfig = storage.newClientConfiguration(connectorConfig);
    assertEquals(Protocol.HTTP, clientConfig.getProtocol());
    assertEquals("", clientConfig.getProxyHost());
    assertEquals(8080, clientConfig.getProxyPort());
    assertEquals(null, clientConfig.getProxyUsername());
    assertEquals(null, clientConfig.getProxyPassword());
  }

  @Test
  public void testProtocolHostPort() throws Exception {
    localProps.put(S3SinkConnectorConfig.S3_PROXY_URL_CONFIG, "http://localhost:8080");
    setUp();
    clientConfig = storage.newClientConfiguration(connectorConfig);
    assertEquals(Protocol.HTTP, clientConfig.getProtocol());
    assertEquals("localhost", clientConfig.getProxyHost());
    assertEquals(8080, clientConfig.getProxyPort());
    assertEquals(null, clientConfig.getProxyUsername());
    assertEquals(null, clientConfig.getProxyPassword());
  }

  @Test
  public void testProtocolHostPortAndPath() throws Exception {
    localProps.put(S3SinkConnectorConfig.S3_PROXY_URL_CONFIG, "http://localhost:8080/current");
    setUp();
    clientConfig = storage.newClientConfiguration(connectorConfig);
    assertEquals(Protocol.HTTP, clientConfig.getProtocol());
    assertEquals("localhost", clientConfig.getProxyHost());
    assertEquals(8080, clientConfig.getProxyPort());
    assertEquals(null, clientConfig.getProxyUsername());
    assertEquals(null, clientConfig.getProxyPassword());
  }

  @Test
  public void testProtocolHostPortUserOnUrlNoPass() throws Exception {
    localProps.put(S3SinkConnectorConfig.S3_PROXY_URL_CONFIG, "http://user@localhost:8080");
    setUp();
    clientConfig = storage.newClientConfiguration(connectorConfig);
    assertEquals(Protocol.HTTP, clientConfig.getProtocol());
    assertEquals("localhost", clientConfig.getProxyHost());
    assertEquals(8080, clientConfig.getProxyPort());
    assertEquals("user", clientConfig.getProxyUsername());
    assertEquals(null, clientConfig.getProxyPassword());
  }

  @Test
  public void testProtocolHostPortUserOnUrlEmptyPass() throws Exception {
    localProps.put(S3SinkConnectorConfig.S3_PROXY_URL_CONFIG, "http://user:@localhost:8080");
    setUp();
    clientConfig = storage.newClientConfiguration(connectorConfig);
    assertEquals(Protocol.HTTP, clientConfig.getProtocol());
    assertEquals("localhost", clientConfig.getProxyHost());
    assertEquals(8080, clientConfig.getProxyPort());
    assertEquals("user", clientConfig.getProxyUsername());
    assertEquals("", clientConfig.getProxyPassword());
  }

  @Test
  public void testProtocolHostPortUserPassOnUrl() throws Exception {
    localProps.put(S3SinkConnectorConfig.S3_PROXY_URL_CONFIG, "http://user:pass@localhost:8080");
    setUp();
    clientConfig = storage.newClientConfiguration(connectorConfig);
    assertEquals(Protocol.HTTP, clientConfig.getProtocol());
    assertEquals("localhost", clientConfig.getProxyHost());
    assertEquals(8080, clientConfig.getProxyPort());
    assertEquals("user", clientConfig.getProxyUsername());
    assertEquals("pass", clientConfig.getProxyPassword());
  }

  @Test
  public void testProtocolHostPortUserPassConfigsOverrideUrl() throws Exception {
    localProps.put(S3SinkConnectorConfig.S3_PROXY_URL_CONFIG, "http://user:pass@localhost:8080");
    localProps.put(S3SinkConnectorConfig.S3_PROXY_USER_CONFIG, "realuser");
    localProps.put(S3SinkConnectorConfig.S3_PROXY_PASS_CONFIG, "realpass");
    setUp();
    clientConfig = storage.newClientConfiguration(connectorConfig);
    assertEquals(Protocol.HTTP, clientConfig.getProtocol());
    assertEquals("localhost", clientConfig.getProxyHost());
    assertEquals(8080, clientConfig.getProxyPort());
    assertEquals("realuser", clientConfig.getProxyUsername());
    assertEquals("realpass", clientConfig.getProxyPassword());
  }

  @Test
  public void testProtocolHostPortUserPassConfigs() throws Exception {
    localProps.put(S3SinkConnectorConfig.S3_PROXY_URL_CONFIG, "http://localhost:8080");
    localProps.put(S3SinkConnectorConfig.S3_PROXY_USER_CONFIG, "realuser");
    localProps.put(S3SinkConnectorConfig.S3_PROXY_PASS_CONFIG, "realpass");
    setUp();
    clientConfig = storage.newClientConfiguration(connectorConfig);
    assertEquals(Protocol.HTTP, clientConfig.getProtocol());
    assertEquals("localhost", clientConfig.getProxyHost());
    assertEquals(8080, clientConfig.getProxyPort());
    assertEquals("realuser", clientConfig.getProxyUsername());
    assertEquals("realpass", clientConfig.getProxyPassword());
  }

  @Test
  public void testHTTPSProtocolHostPortUserPassConfigs() throws Exception {
    localProps.put(S3SinkConnectorConfig.S3_PROXY_URL_CONFIG, "https://localhost:8080");
    localProps.put(S3SinkConnectorConfig.S3_PROXY_USER_CONFIG, "realuser");
    localProps.put(S3SinkConnectorConfig.S3_PROXY_PASS_CONFIG, "realpass");
    setUp();
    clientConfig = storage.newClientConfiguration(connectorConfig);
    assertEquals(Protocol.HTTPS, clientConfig.getProtocol());
    assertEquals("localhost", clientConfig.getProxyHost());
    assertEquals(8080, clientConfig.getProxyPort());
    assertEquals("realuser", clientConfig.getProxyUsername());
    assertEquals("realpass", clientConfig.getProxyPassword());
  }*/
}
