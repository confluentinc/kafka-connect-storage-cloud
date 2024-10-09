/*
 * Copyright 2024 Confluent Inc.
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

package io.confluent.connect.s3.auth;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AWSSecurityTokenServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class IamAssumeRoleChainedCredentialsProvider implements AWSCredentialsProvider, Closeable {

  private final String awsRoleArn;
  private final String middlewareRoleArn;
  private final String externalId;
  private final String middlewareExternalId;
  private final String sessionName;
  private AWSCredentialsProvider credentialsProvider;
  private final AWSSecurityTokenService internalStsClient;
  private final int maxRetries;
  private final long retryDelayInMillis;

  private static final int DEFAULT_MAX_RETRIES = 3;
  private static final long DEFAULT_RETRY_DELAY_MS = 2000;
  private static final long DEFAULT_REFRESH_INTERVAL_MINUTES = 60;

  private final ScheduledExecutorService scheduler;

  private static final Logger log = LoggerFactory.getLogger(
      IamAssumeRoleChainedCredentialsProvider.class);

  private IamAssumeRoleChainedCredentialsProvider(Builder builder) {
    this.awsRoleArn = builder.awsRoleArn;
    this.middlewareRoleArn = builder.confluentMiddlewareRoleArn;
    this.externalId = builder.externalId;
    this.middlewareExternalId = builder.confluentMiddlewareExternalId;
    this.sessionName = isBlank(builder.sessionName) ? "random-session" : builder.sessionName;
    this.maxRetries = builder.maxRetries >= 0 ? builder.maxRetries : DEFAULT_MAX_RETRIES;
    this.retryDelayInMillis =
        builder.retryDelayMs >= 0 ? builder.retryDelayMs : DEFAULT_RETRY_DELAY_MS;
    this.internalStsClient = AWSSecurityTokenServiceClientBuilder.defaultClient();
    this.scheduler = Executors.newScheduledThreadPool(1);

    this.buildCredentialsProvider();

    long refreshIntervalMinutes =
        builder.refreshIntervalMinutes >= 0 ? builder.refreshIntervalMinutes
            : DEFAULT_REFRESH_INTERVAL_MINUTES;
    scheduler.scheduleAtFixedRate(this::refresh, refreshIntervalMinutes, refreshIntervalMinutes,
        TimeUnit.MINUTES);
  }

  private void buildCredentialsProvider() {
    log.info("starting to build credential provider");
    int retryCount = 0;
    boolean success = false;
    long expRetryDelayInMillis = this.retryDelayInMillis;

    while (retryCount <= this.maxRetries && !success) {
      log.info("retry count {}", retryCount);
      try {
        // Step 1: Assume role in confluent middleware aws account
        AWSCredentialsProvider middlewareCredentialsProvider = getAwsCredentialsProvider(
            this.internalStsClient,
            this.middlewareRoleArn,
            "middleware-" + this.sessionName,
            this.middlewareExternalId);

        // Step 2: Chain assume role in customer's aws account using the middleware credentials
        this.credentialsProvider = getAwsCredentialsProvider(
            AWSSecurityTokenServiceClientBuilder.standard()
                .withCredentials(middlewareCredentialsProvider)
                .build(),
            this.awsRoleArn,
            this.sessionName,
            this.externalId);

        success = true;  // chain assume role is successful
      } catch (AWSSecurityTokenServiceException e) {
        log.error("exception in sts", e);
        if (retryCount == 0) {
          log.info("Failed to build aws credential provider, starting to retry.", e);
        } else {
          log.info("Failed retry Attempt {} of {} to build aws credential provider.",
              retryCount,
              this.maxRetries,
              e);
        }
        log.info("Awaiting {} milliseconds before retrying to build aws credential provider.",
            expRetryDelayInMillis);
        try {
          TimeUnit.MILLISECONDS.sleep(expRetryDelayInMillis);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt(); // Preserve interrupt status
        }
        expRetryDelayInMillis <<= 1;
        retryCount++;
      }
    }
  }

  private AWSCredentialsProvider getAwsCredentialsProvider(final AWSSecurityTokenService stsClient,
      final String roleArn,
      final String sessionName,
      final String externalId) {
    if (!isBlank(externalId)) {
      return new STSAssumeRoleSessionCredentialsProvider
          .Builder(roleArn, sessionName)
          .withStsClient(stsClient)
          .withExternalId(externalId)
          .build();
    }

    return new STSAssumeRoleSessionCredentialsProvider
        .Builder(roleArn, sessionName)
        .withStsClient(stsClient)
        .build();
  }

  public static boolean isBlank(String string) {
    return string == null || string.isEmpty() || string.trim().isEmpty();
  }

  @Override
  public AWSCredentials getCredentials() {
    return credentialsProvider.getCredentials();
  }

  @Override
  public void refresh() {
    this.buildCredentialsProvider();
  }

  @Override
  public void close() throws IOException {
    this.internalStsClient.shutdown();
    this.scheduler.shutdown();
  }

  public static class Builder {

    private final String awsRoleArn;
    private final String confluentMiddlewareRoleArn;
    private String externalId;
    private String confluentMiddlewareExternalId;
    private String sessionName;
    private int maxRetries = DEFAULT_MAX_RETRIES;
    private long retryDelayMs = DEFAULT_RETRY_DELAY_MS;
    private long refreshIntervalMinutes = DEFAULT_REFRESH_INTERVAL_MINUTES;

    public Builder(String awsRoleArn, String confluentMiddlewareRoleArn) {
      if (awsRoleArn == null || awsRoleArn.isEmpty()) {
        throw new IllegalArgumentException("awsRoleArn must be set");
      }

      if (confluentMiddlewareRoleArn == null || confluentMiddlewareRoleArn.isEmpty()) {
        throw new IllegalArgumentException("confluentMiddlewareRoleArn must be set");
      }

      this.awsRoleArn = awsRoleArn;
      this.confluentMiddlewareRoleArn = confluentMiddlewareRoleArn;
    }

    public Builder withExternalId(String externalId) {
      this.externalId = externalId;
      return this;
    }

    public Builder withConfluentMiddlewareExternalId(String externalId) {
      this.confluentMiddlewareExternalId = externalId;
      return this;
    }

    public Builder withSessionName(String sessionName) {
      this.sessionName = sessionName;
      return this;
    }

    public Builder withMaxRetries(int maxRetries) {
      this.maxRetries = maxRetries;
      return this;
    }

    public Builder withRetryDelayMs(long retryDelayMs) {
      this.retryDelayMs = retryDelayMs;
      return this;
    }

    public Builder withRefreshIntervalMinutes(long refreshIntervalMinutes) {
      this.refreshIntervalMinutes = refreshIntervalMinutes;
      return this;
    }

    public IamAssumeRoleChainedCredentialsProvider build() {
      return new IamAssumeRoleChainedCredentialsProvider(this);
    }
  }
}
