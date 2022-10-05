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

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.retry.PredefinedBackoffStrategies;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import org.apache.http.HttpStatus;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import io.confluent.connect.s3.DummyAssertiveCredentialsProvider;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.S3SinkConnectorTestBase;

import static io.confluent.connect.s3.S3SinkConnectorConfig.S3_RETRY_BACKOFF_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.S3_RETRY_MAX_BACKOFF_TIME_MS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class S3StorageTest extends S3SinkConnectorTestBase {

  /**
   * Maximum retry limit.
   **/
  public static final int MAX_RETRIES = 30;

  protected RetryPolicy retryPolicy;
  protected Map<String, String> localProps = new HashMap<>();
  protected S3Storage storage;

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.putAll(localProps);
    return props;
  }

  //@Before should be omitted in order to be able to add properties per test.
  public void setUp() throws Exception {
    super.setUp();
    storage = new S3Storage(connectorConfig, url);
    retryPolicy = storage.newFullJitterRetryPolicy(connectorConfig);
  }

  @Test
  public void testRetryPolicy() throws Exception {
    setUp();
    assertTrue(retryPolicy.getRetryCondition() instanceof PredefinedRetryPolicies
        .SDKDefaultRetryCondition);
    assertTrue(retryPolicy.getBackoffStrategy() instanceof PredefinedBackoffStrategies
        .FullJitterBackoffStrategy);
  }

  @Test
  public void testRetryPolicyNonRetriable() throws Exception {
    setUp();
    AmazonClientException e = new AmazonClientException("Non-retriable exception");
    assertFalse(retryPolicy.getRetryCondition().shouldRetry(null, e, 1));
  }

  @Test
  public void testRetryPolicyRetriableServiceException() throws Exception {
    setUp();
    AmazonServiceException e = new AmazonServiceException("Retriable exception");
    e.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    assertTrue(retryPolicy.getRetryCondition().shouldRetry(null, e, 1));
  }

  @Test
  public void testRetryPolicyNonRetriableServiceException() throws Exception {
    setUp();
    AmazonServiceException e = new AmazonServiceException("Non-retriable exception");
    e.setStatusCode(HttpStatus.SC_METHOD_NOT_ALLOWED);
    assertFalse(retryPolicy.getRetryCondition().shouldRetry(null, e, 1));
  }

  @Test
  public void testRetryPolicyRetriableThrottlingException() throws Exception {
    setUp();
    AmazonServiceException e = new AmazonServiceException("Retriable exception");
    e.setErrorCode("TooManyRequestsException");
    assertTrue(retryPolicy.getRetryCondition().shouldRetry(null, e, 1));
  }

  @Test
  public void testRetryPolicyRetriableSkewException() throws Exception {
    setUp();
    AmazonServiceException e = new AmazonServiceException("Retriable exception");
    e.setErrorCode("RequestExpired");
    assertTrue(retryPolicy.getRetryCondition().shouldRetry(null, e, 1));
  }

  @Test
  public void testRetryPolicyDelayRanges() throws Exception {
    setUp();
    assertComputeRetryInRange(10, 10L);
    assertComputeRetryInRange(10, 100L);
    assertComputeRetryInRange(10, 1000L);
    assertComputeRetryInRange(MAX_RETRIES + 1, 1000L);
    assertComputeRetryInRange(100, S3_RETRY_MAX_BACKOFF_TIME_MS + 1);
    assertComputeRetryInRange(MAX_RETRIES + 1, S3_RETRY_MAX_BACKOFF_TIME_MS + 1);
  }

  @Test
  public void testDefaultCredentialsProvider() throws Exception {
    // default values
    setUp();
    AWSCredentialsProvider credentialsProvider = storage.newCredentialsProvider(connectorConfig);
    assertTrue(S3SinkConnectorConfig.CREDENTIALS_PROVIDER_CLASS_DEFAULT.isInstance(credentialsProvider));

    // empty default values
    localProps.put(S3SinkConnectorConfig.AWS_ACCESS_KEY_ID_CONFIG, "");
    localProps.put(S3SinkConnectorConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "");
    setUp();
    credentialsProvider = storage.newCredentialsProvider(connectorConfig);
    assertTrue(S3SinkConnectorConfig.CREDENTIALS_PROVIDER_CLASS_DEFAULT.isInstance(credentialsProvider));
  }

  @Test
  public void testUserDefinedCredentialsProvider() throws Exception {
    String configPrefix = S3SinkConnectorConfig.CREDENTIALS_PROVIDER_CONFIG_PREFIX;
    localProps.put(configPrefix.concat(DummyAssertiveCredentialsProvider.ACCESS_KEY_NAME), "foo_key");
    localProps.put(configPrefix.concat(DummyAssertiveCredentialsProvider.SECRET_KEY_NAME), "bar_secret");
    localProps.put(configPrefix.concat(DummyAssertiveCredentialsProvider.CONFIGS_NUM_KEY_NAME), "5");
    localProps.put(
        S3SinkConnectorConfig.CREDENTIALS_PROVIDER_CLASS_CONFIG,
        DummyAssertiveCredentialsProvider.class.getName()
    );
    setUp();
    AWSCredentialsProvider credentialsProvider = storage.newCredentialsProvider(connectorConfig);
    assertTrue(credentialsProvider instanceof DummyAssertiveCredentialsProvider);
  }

  @Test
  public void testUserSuppliedCredentials() throws Exception {
    localProps.put(S3SinkConnectorConfig.AWS_ACCESS_KEY_ID_CONFIG, "foo_key");
    localProps.put(S3SinkConnectorConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "bar_secret");
    setUp();
    AWSCredentialsProvider credentialsProvider = storage.newCredentialsProvider(connectorConfig);
    assertTrue(credentialsProvider instanceof AWSStaticCredentialsProvider);
  }

  /**
   * Calculates exponential delay, capped by
   * {@link com.amazonaws.retry.PredefinedBackoffStrategies#MAX_RETRIES} number of retries
   * and {@link io.confluent.connect.s3.S3SinkConnectorConfig#S3_RETRY_MAX_BACKOFF_TIME_MS} total delay time
   * in ms
   *
   * @param retriesAttempted
   * @param baseDelay
   * @return
   * @see PredefinedBackoffStrategies#calculateExponentialDelay(int, int, int)
   */
  private int calculateExponentialDelay(
      int retriesAttempted, long baseDelay
  ) {
    int retries = Math.min(retriesAttempted, MAX_RETRIES);
    return (int) Math.min((1L << retries) * baseDelay, S3_RETRY_MAX_BACKOFF_TIME_MS);
  }

  private void assertComputeRetryInRange(
      int retryAttempts,
      long retryBackoffMs
  ) throws Exception {
    localProps.put(S3_RETRY_BACKOFF_CONFIG, String.valueOf(retryBackoffMs));
    setUp();
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
