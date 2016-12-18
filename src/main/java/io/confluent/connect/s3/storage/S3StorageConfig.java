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

package io.confluent.connect.s3.storage;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.metrics.RequestMetricCollector;

/**
 * Configuration of S3 storage class.
 */
public class S3StorageConfig {
  private final String bucketName;
  private final AWSCredentials credentials;
  private final AWSCredentialsProvider provider;
  private final ClientConfiguration clientConfig;
  private final RequestMetricCollector collector;

  /**
   * Fully configurable constructor.
   *
   * @param bucketName   name of S3 bucket to contain all Kafka data.
   * @param provider     provider of credentials for S3 client.
   * @param credentials  credentials of S3 client to be used if provider is <code>null</code>.
   * @param clientConfig S3 client configuration.
   * @param collector    S3 collector request metrics.
   */
  public S3StorageConfig(String bucketName, AWSCredentialsProvider provider, final AWSCredentials credentials,
                         ClientConfiguration clientConfig, RequestMetricCollector collector) {
    this.bucketName = bucketName;
    if (provider == null) {
      this.provider = new AWSCredentialsProvider() {
        @Override
        public AWSCredentials getCredentials() {
          return credentials;
        }

        @Override
        public void refresh() {
        }
      };
    } else {
      this.provider = provider;
    }
    this.provider.refresh();
    this.credentials = this.provider.getCredentials();
    this.clientConfig = clientConfig;
    this.collector = collector;
  }

  /**
   * Constructor with default request metric collector.
   *
   * @param bucketName   name of S3 bucket to contain all Kafka data.
   * @param provider     provider of credentials for S3 client.
   * @param credentials  credentials of S3 client to be used if provider is <code>null</code>.
   * @param clientConfig S3 client configuration.
   */
  public S3StorageConfig(String bucketName, AWSCredentialsProvider provider, AWSCredentials credentials,
                         ClientConfiguration clientConfig) {
    this(bucketName, provider, credentials, clientConfig, RequestMetricCollector.NONE);
  }

  /**
   * Constructor with credentials and default request metric collector.
   *
   * @param bucketName   name of S3 bucket to contain all Kafka data.
   * @param credentials  credentials of S3 client to be used.
   * @param clientConfig S3 client configuration.
   */
  public S3StorageConfig(String bucketName, AWSCredentials credentials, ClientConfiguration clientConfig) {
    this(bucketName, null, credentials, clientConfig, RequestMetricCollector.NONE);
  }

  /**
   * Constructor with credentials' provider and default request metric collector.
   *
   * @param bucketName   name of S3 bucket to contain all Kafka data.
   * @param provider     provider of credentials for S3 client.
   * @param clientConfig S3 client configuration.
   */
  public S3StorageConfig(String bucketName, AWSCredentialsProvider provider, ClientConfiguration clientConfig) {
    this(bucketName, provider, null, clientConfig, RequestMetricCollector.NONE);
  }

  /**
   * Constructor with credentials, default client configuration and default request metric collector.
   *
   * @param bucketName  name of S3 bucket to contain all Kafka data.
   * @param credentials credentials of S3 client to be used.
   */
  public S3StorageConfig(String bucketName, AWSCredentials credentials) {
    this(bucketName, null, credentials, new ClientConfiguration(), RequestMetricCollector.NONE);
  }

  /**
   * Constructor with credentials' provider, default client configuration and default request metric collector.
   *
   * @param bucketName name of S3 bucket to contain all Kafka data.
   * @param provider   provider of credentials for S3 client.
   */
  public S3StorageConfig(String bucketName, AWSCredentialsProvider provider) {
    this(bucketName, provider, null, new ClientConfiguration(), RequestMetricCollector.NONE);
  }

  /**
   * Constructor with client configuration, default credentials discovery and default request metric collector.
   *
   * @param bucketName   name of S3 bucket to contain all Kafka data.
   * @param clientConfig S3 client configuration.
   */
  public S3StorageConfig(String bucketName, ClientConfiguration clientConfig) {
    this(bucketName, null, null, clientConfig, RequestMetricCollector.NONE);
  }

  /**
   * Constructor with default configuration settings and default credentials discovery.
   *
   * @param bucketName name of S3 bucket to contain all Kafka data.
   */
  public S3StorageConfig(String bucketName) {
    this(bucketName, null, null, new ClientConfiguration(), RequestMetricCollector.NONE);
  }

  /**
   * Return the name of S3 bucket used to store Kafka data.
   *
   * @return the S3 bucket name.
   */
  public String getBucketName() {
    return bucketName;
  }

  /**
   * Get the credentials for this storage instance.
   *
   * @return the credentials.
   */
  public AWSCredentials getCredentials() {
    return credentials;
  }

  /**
   * Get the credentials' provider for this storage instance.
   *
   * @return the credentials' provider.
   */
  public AWSCredentialsProvider getProvider() {
    return provider;
  }

  /**
   * Get the S3 client configuration for this storage instance.
   *
   * @return the client configuration.
   */
  public ClientConfiguration getClientConfig() {
    return clientConfig;
  }

  /**
   * Get the collector of S3 request metrics for this storage instance.
   *
   * @return the collector.
   */
  public RequestMetricCollector getCollector() {
    return collector;
  }
}
