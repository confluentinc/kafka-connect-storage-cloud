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

package io.confluent.connect.s3.storage;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.metrics.RequestMetricCollector;

import io.confluent.connect.s3.S3SinkConnectorConfig;

/**
 * Configuration of S3 storage class.
 */
public class S3StorageConfig {
  private final String bucketName;
  private final AWSCredentials credentials;
  private final AWSCredentialsProvider provider;
  private final ClientConfiguration s3ClientConfig;
  private final String ssea;
  private final int partSize;
  private final RequestMetricCollector collector;

  /**
   * Fully configurable constructor.
   *
   * @param props S3 connector properties.
   * @param provider provider of credentials for S3 client.
   * @param credentials credentials of S3 client to be used if provider is <code>null</code>.
   * @param s3ClientConfig S3 client configuration.
   * @param collector S3 collector request metrics.
   */
  public S3StorageConfig(S3SinkConnectorConfig props, AWSCredentialsProvider provider, final AWSCredentials credentials,
                         ClientConfiguration s3ClientConfig, RequestMetricCollector collector) {
    this.bucketName = props.getBucketName();
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
    this.s3ClientConfig = s3ClientConfig;
    this.ssea = props.getSSEA();
    this.partSize = props.getPartSize();
    this.collector = collector;
  }

  /**
   * Constructor with default request metric collector.
   *
   * @param props S3 connector properties.
   * @param provider provider of credentials for S3 client.
   * @param credentials credentials of S3 client to be used if provider is <code>null</code>.
   * @param s3ClientConfig S3 client configuration.
   */
  public S3StorageConfig(S3SinkConnectorConfig props, AWSCredentialsProvider provider, AWSCredentials credentials,
                         ClientConfiguration s3ClientConfig) {
    this(props, provider, credentials, s3ClientConfig, RequestMetricCollector.NONE);
  }

  /**
   * Constructor with credentials and default request metric collector.
   *
   * @param props S3 connector properties.
   * @param credentials credentials of S3 client to be used.
   * @param s3ClientConfig S3 client configuration.
   */
  public S3StorageConfig(S3SinkConnectorConfig props, AWSCredentials credentials, ClientConfiguration s3ClientConfig) {
    this(props, null, credentials, s3ClientConfig, RequestMetricCollector.NONE);
  }

  /**
   * Constructor with credentials' provider and default request metric collector.
   *
   * @param props S3 connector properties.
   * @param provider provider of credentials for S3 client.
   * @param s3ClientConfig S3 client configuration.
   */
  public S3StorageConfig(S3SinkConnectorConfig props, AWSCredentialsProvider provider, ClientConfiguration s3ClientConfig) {
    this(props, provider, null, s3ClientConfig, RequestMetricCollector.NONE);
  }

  /**
   * Constructor with credentials, default client configuration and default request metric collector.
   *
   * @param props S3 connector properties.
   * @param credentials credentials of S3 client to be used.
   */
  public S3StorageConfig(S3SinkConnectorConfig props, AWSCredentials credentials) {
    this(props, null, credentials, new ClientConfiguration(), RequestMetricCollector.NONE);
  }

  /**
   * Constructor with credentials' provider, default client configuration and default request metric collector.
   *
   * @param props S3 connector properties.
   * @param provider provider of credentials for S3 client.
   */
  public S3StorageConfig(S3SinkConnectorConfig props, AWSCredentialsProvider provider) {
    this(props, provider, null, new ClientConfiguration(), RequestMetricCollector.NONE);
  }

  /**
   * Constructor with client configuration, default credentials discovery and default request metric collector.
   *
   * @param props S3 connector properties.
   * @param s3ClientConfig S3 client configuration.
   */
  public S3StorageConfig(S3SinkConnectorConfig props, ClientConfiguration s3ClientConfig) {
    this(props, null, null, s3ClientConfig, RequestMetricCollector.NONE);
  }

  /**
   * Constructor with default configuration settings and default credentials discovery.
   *
   * @param props S3 connector properties.
   */
  public S3StorageConfig(S3SinkConnectorConfig props) {
    this(props, null, null, new ClientConfiguration(), RequestMetricCollector.NONE);
  }

  /**
   * Return the name of S3 bucket used to store Kafka data.
   *
   * @return the S3 bucket name.
   */
  public String bucket() {
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
  public AWSCredentialsProvider provider() {
    return provider;
  }

  /**
   * Get the S3 client configuration for this storage instance.
   *
   * @return the client configuration.
   */
  public ClientConfiguration clientConfig() {
    return s3ClientConfig;
  }

  /**
   * Get the collector of S3 request metrics for this storage instance.
   *
   * @return the collector.
   */
  public RequestMetricCollector collector() {
    return collector;
  }

  /**
   * Get the S3 server-side encryption algorithm.
   *
   * @return the server-side encryption algorithm.
   */
  public String ssea() {
    return ssea;
  }

  /**
   * Get the part size configured for S3 multi-part uploads.
   *
   * @return the collector.
   */
  public int partSize() {
    return partSize;
  }
}
