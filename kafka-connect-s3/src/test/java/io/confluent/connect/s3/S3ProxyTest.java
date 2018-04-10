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

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.retry.PredefinedBackoffStrategies;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import org.apache.http.HttpStatus;
import org.apache.kafka.common.config.ConfigException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Matchers;

import java.util.HashMap;
import java.util.Map;

import io.confluent.connect.s3.storage.S3Storage;


import static io.confluent.connect.s3.S3SinkConnectorConfig.S3_RETRY_BACKOFF_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.S3_RETRY_MAX_TIME_MS;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class S3ProxyTest extends S3SinkConnectorTestBase {

  /**
   * Maximum retry limit.
   **/
  public static final int MAX_RETRIES = 30;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  protected S3Storage storage;
  protected ClientConfiguration clientConfig;
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
    thrown.expect(ConfigException.class);
    thrown.expectMessage(Matchers.contains("no protocol: localhost"));
    clientConfig = storage.newClientConfiguration(connectorConfig);
  }

  @Test
  public void testUnknownProtocolThrowsException() throws Exception {
    localProps.put(S3SinkConnectorConfig.S3_PROXY_URL_CONFIG, "unknown://localhost");
    setUp();
    thrown.expect(ConfigException.class);
    thrown.expectMessage(Matchers.contains("unknown protocol: localhost"));
    clientConfig = storage.newClientConfiguration(connectorConfig);
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
  }

  @Test
  public void testRetryPolicy() throws Exception {
    setUp();
    RetryPolicy retryPolicy = storage.newFullJitterRetryPolicy(connectorConfig);
    assertTrue(retryPolicy.getRetryCondition() instanceof PredefinedRetryPolicies
        .SDKDefaultRetryCondition);
    assertTrue(retryPolicy.getBackoffStrategy() instanceof PredefinedBackoffStrategies
        .FullJitterBackoffStrategy);
  }

  @Test
  public void testRetryPolicyNonRetriable() throws Exception {
    setUp();
    RetryPolicy retryPolicy = storage.newFullJitterRetryPolicy(connectorConfig);
    AmazonClientException e = new AmazonClientException("Non-retriable exception");
    assertFalse(retryPolicy.getRetryCondition().shouldRetry(null, e, 1));
  }

  @Test
  public void testRetryPolicyRetriableServiceException() throws Exception {
    setUp();
    RetryPolicy retryPolicy = storage.newFullJitterRetryPolicy(connectorConfig);
    AmazonServiceException e = new AmazonServiceException("Retriable exception");
    e.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    assertTrue(retryPolicy.getRetryCondition().shouldRetry(null, e, 1));
  }

  @Test
  public void testRetryPolicyNonRetriableServiceException() throws Exception {
    setUp();
    RetryPolicy retryPolicy = storage.newFullJitterRetryPolicy(connectorConfig);
    AmazonServiceException e = new AmazonServiceException("Non-retriable exception");
    e.setStatusCode(HttpStatus.SC_METHOD_NOT_ALLOWED);
    assertFalse(retryPolicy.getRetryCondition().shouldRetry(null, e, 1));
  }

  @Test
  public void testRetryPolicyRetriableThrottlingException() throws Exception {
    setUp();
    RetryPolicy retryPolicy = storage.newFullJitterRetryPolicy(connectorConfig);
    AmazonServiceException e = new AmazonServiceException("Retriable exception");
    e.setErrorCode("TooManyRequestsException");
    assertTrue(retryPolicy.getRetryCondition().shouldRetry(null, e, 1));
  }

  @Test
  public void testRetryPolicyRetriableSkewException() throws Exception {
    setUp();
    RetryPolicy retryPolicy = storage.newFullJitterRetryPolicy(connectorConfig);
    AmazonServiceException e = new AmazonServiceException("Retriable exception");
    e.setErrorCode("RequestExpired");
    assertTrue(retryPolicy.getRetryCondition().shouldRetry(null, e, 1));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRetryPolicyNegativeDelay() throws Exception {
    localProps.put(S3SinkConnectorConfig.S3_RETRY_BACKOFF_CONFIG, "-100");
    setUp();
    storage.newFullJitterRetryPolicy(connectorConfig);
  }

  @Test
  public void testRetryPolicyDelayRanges() throws Exception {
    assertComputeRetryInRange(10, 10L);
    assertComputeRetryInRange(10, 100L);
    assertComputeRetryInRange(10, 1000L);
    assertComputeRetryInRange(MAX_RETRIES + 1, 1000L);
    assertComputeRetryInRange(100, S3_RETRY_MAX_TIME_MS + 1);
    assertComputeRetryInRange(MAX_RETRIES + 1, S3_RETRY_MAX_TIME_MS + 1);
  }

  /**
   * Calculates exponential delay, capped by
   * {@link com.amazonaws.retry.PredefinedBackoffStrategies#MAX_RETRIES} number of retries
   * and {@link io.confluent.connect.s3.S3SinkConnectorConfig#S3_RETRY_MAX_TIME_MS} total delay time
   * in ms
   * @param retriesAttempted
   * @param baseDelay
   * @return
   * @see PredefinedBackoffStrategies#calculateExponentialDelay(int, int, int)
   */
  private int calculateExponentialDelay(
      int retriesAttempted, long baseDelay
  ) {
    int retries = Math.min(retriesAttempted, MAX_RETRIES);
    return (int) Math.min((1L << retries) * baseDelay, S3_RETRY_MAX_TIME_MS);
  }

  private void assertComputeRetryInRange(
      int retryAttempts,
      long retryBackoffMs
  ) throws Exception {

    localProps.put(S3_RETRY_BACKOFF_CONFIG, String.valueOf(retryBackoffMs));
    setUp();
    RetryPolicy retryPolicy = storage.newFullJitterRetryPolicy(connectorConfig);
    RetryPolicy.BackoffStrategy backoffStrategy = retryPolicy.getBackoffStrategy();

    for (int i = 0; i != 20; ++i) {
      for (int retries = 0; retries <= retryAttempts; ++retries) {
        long maxResult = calculateExponentialDelay(retries, retryBackoffMs);
        long result = backoffStrategy.delayBeforeNextRetry(null, null, retries);
        if (retryBackoffMs < 0) {
          assertEquals(0, result);
        } else {
          assertTrue(result >= 0L);
          assertTrue(result <= maxResult);
        }
      }
    }
  }
}
