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
import io.confluent.connect.s3.util.S3ProxyConfig;
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

  protected S3Storage storage;
  protected S3Client s3Client;
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
    assertThrows(ConfigException.class,
        () -> new S3ProxyConfig(new S3SinkConnectorConfig(createProps())));
  }

  @Test
  public void testNoProtocolThrowsException() throws Exception {
    localProps.put(S3SinkConnectorConfig.S3_PROXY_URL_CONFIG, "localhost");
    setUp();

    assertThrows("no protocol: localhost", ConfigException.class,
        () -> new S3ProxyConfig(new S3SinkConnectorConfig(createProps())));
  }

  @Test
  public void testUnknownProtocolThrowsException() throws Exception {
    localProps.put(S3SinkConnectorConfig.S3_PROXY_URL_CONFIG, "unknown://localhost");
    setUp();
    assertThrows("unknown protocol: localhost", ConfigException.class,
        () -> new S3ProxyConfig(new S3SinkConnectorConfig(createProps())));
  }

  @Test
  public void testProtocolOnly() throws Exception {
    localProps.put(S3SinkConnectorConfig.S3_PROXY_URL_CONFIG, "http://");
    setUp();
    assertThrows(ConfigException.class,
        () -> new S3ProxyConfig(new S3SinkConnectorConfig(createProps())));
  }

  @Test
  public void testProtocolHostOnly() throws Exception {
    localProps.put(S3SinkConnectorConfig.S3_PROXY_URL_CONFIG, "http://localhost");
    setUp();
    S3ProxyConfig proxyConfig = new S3ProxyConfig(new S3SinkConnectorConfig(createProps()));
    assertEquals("HTTP", proxyConfig.protocol());
    assertEquals("localhost", proxyConfig.host());
    assertEquals(-1, proxyConfig.port());
    assertEquals(null, proxyConfig.user());
    assertEquals(null, proxyConfig.pass());
  }

  @Test
  public void testProtocolIpOnly() throws Exception {
    localProps.put(S3SinkConnectorConfig.S3_PROXY_URL_CONFIG, "http://127.0.0.1");
    setUp();

    S3ProxyConfig proxyConfig = new S3ProxyConfig(new S3SinkConnectorConfig(createProps()));
    assertEquals("HTTP", proxyConfig.protocol());
    assertEquals("127.0.0.1", proxyConfig.host());
    assertEquals(-1, proxyConfig.port());
    assertEquals(null, proxyConfig.user());
    assertEquals(null, proxyConfig.pass());
  }

  @Test
  public void testUnknownHostOnly() throws Exception {
    localProps.put(S3SinkConnectorConfig.S3_PROXY_URL_CONFIG, "http://255.255.255.255");
    setUp();
    S3ProxyConfig proxyConfig = new S3ProxyConfig(new S3SinkConnectorConfig(createProps()));

    assertEquals("HTTP", proxyConfig.protocol());
    assertEquals("255.255.255.255", proxyConfig.host());
    assertEquals(-1, proxyConfig.port());
    assertEquals(null, proxyConfig.user());
    assertEquals(null, proxyConfig.pass());
  }
  
  @Test
  public void testProtocolPort() throws Exception {
    localProps.put(S3SinkConnectorConfig.S3_PROXY_URL_CONFIG, "http://:8080");
    setUp();
    S3ProxyConfig proxyConfig = new S3ProxyConfig(new S3SinkConnectorConfig(createProps()));
    assertEquals("HTTP", proxyConfig.protocol());
    assertEquals("", proxyConfig.host());
    assertEquals(8080, proxyConfig.port());
    assertEquals(null, proxyConfig.user());
    assertEquals(null, proxyConfig.pass());
  }

  @Test
  public void testProtocolHostPort() throws Exception {
    localProps.put(S3SinkConnectorConfig.S3_PROXY_URL_CONFIG, "http://localhost:8080");
    setUp();
    S3ProxyConfig proxyConfig = new S3ProxyConfig(new S3SinkConnectorConfig(createProps()));
    assertEquals("HTTP", proxyConfig.protocol());
    assertEquals("localhost", proxyConfig.host());
    assertEquals(8080, proxyConfig.port());
    assertEquals(null, proxyConfig.user());
    assertEquals(null, proxyConfig.pass());
  }

  @Test
  public void testProtocolHostPortAndPath() throws Exception {
    localProps.put(S3SinkConnectorConfig.S3_PROXY_URL_CONFIG, "http://localhost:8080/current");
    setUp();
    S3ProxyConfig proxyConfig = new S3ProxyConfig(new S3SinkConnectorConfig(createProps()));
    assertEquals("HTTP", proxyConfig.protocol());
    assertEquals("localhost", proxyConfig.host());
    assertEquals(8080, proxyConfig.port());
    assertEquals(null, proxyConfig.user());
    assertEquals(null, proxyConfig.pass());
  }

  @Test
  public void testProtocolHostPortUserOnUrlNoPass() throws Exception {
    localProps.put(S3SinkConnectorConfig.S3_PROXY_URL_CONFIG, "http://user@localhost:8080");
    setUp();
    S3ProxyConfig proxyConfig = new S3ProxyConfig(new S3SinkConnectorConfig(createProps()));
    assertEquals("HTTP", proxyConfig.protocol());
    assertEquals("localhost", proxyConfig.host());
    assertEquals(8080, proxyConfig.port());
    assertEquals("user", proxyConfig.user());
    assertEquals(null, proxyConfig.pass());
  }

  @Test
  public void testProtocolHostPortUserOnUrlEmptyPass() throws Exception {
    localProps.put(S3SinkConnectorConfig.S3_PROXY_URL_CONFIG, "http://user:@localhost:8080");
    setUp();
    S3ProxyConfig proxyConfig = new S3ProxyConfig(new S3SinkConnectorConfig(createProps()));
    assertEquals("HTTP", proxyConfig.protocol());
    assertEquals("localhost", proxyConfig.host());
    assertEquals(8080, proxyConfig.port());
    assertEquals("user", proxyConfig.user());
    assertEquals("", proxyConfig.pass());
  }

  @Test
  public void testProtocolHostPortUserPassOnUrl() throws Exception {
    localProps.put(S3SinkConnectorConfig.S3_PROXY_URL_CONFIG, "http://user:pass@localhost:8080");
    setUp();
    S3ProxyConfig proxyConfig = new S3ProxyConfig(new S3SinkConnectorConfig(createProps()));
    assertEquals("HTTP", proxyConfig.protocol());
    assertEquals("localhost", proxyConfig.host());
    assertEquals(8080, proxyConfig.port());
    assertEquals("user", proxyConfig.user());
    assertEquals("pass", proxyConfig.pass());
  }

  @Test
  public void testProtocolHostPortUserPassConfigsOverrideUrl() throws Exception {
    localProps.put(S3SinkConnectorConfig.S3_PROXY_URL_CONFIG, "http://user:pass@localhost:8080");
    localProps.put(S3SinkConnectorConfig.S3_PROXY_USER_CONFIG, "realuser");
    localProps.put(S3SinkConnectorConfig.S3_PROXY_PASS_CONFIG, "realpass");
    setUp();
    S3ProxyConfig proxyConfig = new S3ProxyConfig(new S3SinkConnectorConfig(createProps()));
    assertEquals("HTTP", proxyConfig.protocol());
    assertEquals("localhost", proxyConfig.host());
    assertEquals(8080, proxyConfig.port());
    assertEquals("realuser", proxyConfig.user());
    assertEquals("realpass", proxyConfig.pass());
  }

  @Test
  public void testProtocolHostPortUserPassConfigs() throws Exception {
    localProps.put(S3SinkConnectorConfig.S3_PROXY_URL_CONFIG, "http://localhost:8080");
    localProps.put(S3SinkConnectorConfig.S3_PROXY_USER_CONFIG, "realuser");
    localProps.put(S3SinkConnectorConfig.S3_PROXY_PASS_CONFIG, "realpass");
    setUp();
    S3ProxyConfig proxyConfig = new S3ProxyConfig(new S3SinkConnectorConfig(createProps()));
    assertEquals("HTTP", proxyConfig.protocol());
    assertEquals("localhost", proxyConfig.host());
    assertEquals(8080, proxyConfig.port());
    assertEquals("realuser", proxyConfig.user());
    assertEquals("realpass", proxyConfig.pass());
  }

  @Test
  public void testHTTPSProtocolHostPortUserPassConfigs() throws Exception {
    localProps.put(S3SinkConnectorConfig.S3_PROXY_URL_CONFIG, "https://localhost:8080");
    localProps.put(S3SinkConnectorConfig.S3_PROXY_USER_CONFIG, "realuser");
    localProps.put(S3SinkConnectorConfig.S3_PROXY_PASS_CONFIG, "realpass");
    setUp();
    S3ProxyConfig proxyConfig = new S3ProxyConfig(new S3SinkConnectorConfig(createProps()));
    assertEquals("HTTPS", proxyConfig.protocol());
    assertEquals("localhost", proxyConfig.host());
    assertEquals(8080, proxyConfig.port());
    assertEquals("realuser", proxyConfig.user());
    assertEquals("realpass", proxyConfig.pass());
  }

}
