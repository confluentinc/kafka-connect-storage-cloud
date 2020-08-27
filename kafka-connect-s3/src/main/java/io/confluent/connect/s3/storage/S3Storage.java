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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.PredefinedClientConfigurations;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.retry.PredefinedBackoffStrategies;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.model.Tag;
import org.apache.avro.file.SeekableInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.util.Map;
import java.util.stream.Collectors;

import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.format.parquet.ParquetFormat;
import io.confluent.connect.s3.util.S3ProxyConfig;
import io.confluent.connect.s3.util.Version;
import io.confluent.connect.storage.Storage;
import io.confluent.connect.storage.common.util.StringUtils;

import static io.confluent.connect.s3.S3SinkConnectorConfig.AWS_ACCESS_KEY_ID_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.AWS_SECRET_ACCESS_KEY_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.REGION_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.S3_PATH_STYLE_ACCESS_ENABLED_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.S3_PROXY_URL_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.S3_RETRY_BACKOFF_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.S3_RETRY_MAX_BACKOFF_TIME_MS;
import static io.confluent.connect.s3.S3SinkConnectorConfig.WAN_MODE_CONFIG;

/**
 * S3 implementation of the storage interface for Connect sinks.
 */
public class S3Storage implements Storage<S3SinkConnectorConfig, ObjectListing> {

  private static final Logger log = LoggerFactory.getLogger(S3Storage.class);

  private final String url;
  private final String bucketName;
  private final AmazonS3 s3;
  private final S3SinkConnectorConfig conf;
  private static final String VERSION_FORMAT = "APN/1.0 Confluent/1.0 KafkaS3Connector/%s";

  /**
   * Construct an S3 storage class given a configuration and an AWS S3 address.
   *
   * @param conf the S3 configuration.
   * @param url the S3 address.
   */
  public S3Storage(S3SinkConnectorConfig conf, String url) {
    this.url = url;
    this.conf = conf;
    this.bucketName = conf.getBucketName();
    this.s3 = newS3Client(conf);
  }

  /**
   * Creates and configures S3 client.
   * Visible for testing.
   *
   * @param config the S3 configuration.
   * @return S3 client
   */
  public AmazonS3 newS3Client(S3SinkConnectorConfig config) {
    ClientConfiguration clientConfiguration = newClientConfiguration(config);
    AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard()
        .withAccelerateModeEnabled(config.getBoolean(WAN_MODE_CONFIG))
        .withPathStyleAccessEnabled(config.getBoolean(S3_PATH_STYLE_ACCESS_ENABLED_CONFIG))
        .withCredentials(newCredentialsProvider(config))
        .withClientConfiguration(clientConfiguration);

    String region = config.getString(REGION_CONFIG);
    if (StringUtils.isBlank(url)) {
      builder = "us-east-1".equals(region)
                ? builder.withRegion(Regions.US_EAST_1)
                : builder.withRegion(region);
    } else {
      builder = builder.withEndpointConfiguration(
          new AwsClientBuilder.EndpointConfiguration(url, region)
      );
    }

    return builder.build();
  }

  // Visible for testing.
  public S3Storage(S3SinkConnectorConfig conf, String url, String bucketName, AmazonS3 s3) {
    this.url = url;
    this.conf = conf;
    this.bucketName = bucketName;
    this.s3 = s3;
  }

  /**
   * Creates S3 client's configuration.
   * This method currently configures the AWS client retry policy to use full jitter.
   * Visible for testing.
   *
   * @param config the S3 configuration.
   * @return S3 client's configuration
   */
  public ClientConfiguration newClientConfiguration(S3SinkConnectorConfig config) {
    String version = String.format(VERSION_FORMAT, Version.getVersion());

    ClientConfiguration clientConfiguration = PredefinedClientConfigurations.defaultConfig();
    clientConfiguration.withUserAgentPrefix(version)
        .withRetryPolicy(newFullJitterRetryPolicy(config));
    if (StringUtils.isNotBlank(config.getString(S3_PROXY_URL_CONFIG))) {
      S3ProxyConfig proxyConfig = new S3ProxyConfig(config);
      clientConfiguration.withProtocol(proxyConfig.protocol())
          .withProxyHost(proxyConfig.host())
          .withProxyPort(proxyConfig.port())
          .withProxyUsername(proxyConfig.user())
          .withProxyPassword(proxyConfig.pass());
    }
    clientConfiguration.withUseExpectContinue(config.useExpectContinue());

    return clientConfiguration;
  }


  /**
   * Creates a retry policy, based on full jitter backoff strategy
   * and default retry condition.
   * Visible for testing.
   *
   * @param config the S3 configuration.
   * @return retry policy
   * @see com.amazonaws.retry.PredefinedRetryPolicies.SDKDefaultRetryCondition
   * @see PredefinedBackoffStrategies.FullJitterBackoffStrategy
   */
  protected RetryPolicy newFullJitterRetryPolicy(S3SinkConnectorConfig config) {
    PredefinedBackoffStrategies.FullJitterBackoffStrategy backoffStrategy =
        new PredefinedBackoffStrategies.FullJitterBackoffStrategy(
            config.getLong(S3_RETRY_BACKOFF_CONFIG).intValue(),
            S3_RETRY_MAX_BACKOFF_TIME_MS
        );

    RetryPolicy retryPolicy = new RetryPolicy(
        PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION,
        backoffStrategy,
        conf.getS3PartRetries(),
        false
    );
    return retryPolicy;
  }

  protected AWSCredentialsProvider newCredentialsProvider(S3SinkConnectorConfig config) {
    final String accessKeyId = config.getString(AWS_ACCESS_KEY_ID_CONFIG);
    final String secretKey = config.getPassword(AWS_SECRET_ACCESS_KEY_CONFIG).value();
    if (StringUtils.isNotBlank(accessKeyId) && StringUtils.isNotBlank(secretKey)) {
      log.info("Returning new credentials provider using the access key id and "
          + "the secret access key that were directly supplied through the connector's "
          + "configuration");
      BasicAWSCredentials basicCredentials = new BasicAWSCredentials(accessKeyId, secretKey);
      return new AWSStaticCredentialsProvider(basicCredentials);
    }
    log.info(
        "Returning new credentials provider based on the configured credentials provider class");
    return config.getCredentialsProvider();
  }

  @Override
  public boolean exists(String name) {
    return StringUtils.isNotBlank(name) && s3.doesObjectExist(bucketName, name);
  }

  public boolean bucketExists() {
    return StringUtils.isNotBlank(bucketName) && s3.doesBucketExistV2(bucketName);
  }

  @Override
  public boolean create(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OutputStream create(String path, S3SinkConnectorConfig conf, boolean overwrite) {
    return create(path, overwrite);
  }

  public S3OutputStream create(String path, boolean overwrite) {
    if (!overwrite) {
      throw new UnsupportedOperationException(
          "Creating a file without overwriting is not currently supported in S3 Connector"
      );
    }

    if (StringUtils.isBlank(path)) {
      throw new IllegalArgumentException("Path can not be empty!");
    }

    if (ParquetFormat.class.isAssignableFrom(
        this.conf.getClass(S3SinkConnectorConfig.FORMAT_CLASS_CONFIG))) {
      return new S3ParquetOutputStream(path, this.conf, s3);
    } else {
      // currently ignore what is passed as method argument.
      return new S3OutputStream(path, this.conf, s3);
    }
  }

  @Override
  public OutputStream append(String filename) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void delete(String name) {
    if (bucketName.equals(name)) {
      // TODO: decide whether to support delete for the top-level bucket.
      // s3.deleteBucket(name);
      return;
    } else {
      s3.deleteObject(bucketName, name);
    }
  }

  @Override
  public void close() {}

  public void addTags(String fileName, Map<String, String> tags) throws SdkClientException {
    ObjectTagging objectTagging = new ObjectTagging(tags.entrySet().stream()
        .map(e -> new Tag(e.getKey(), e.getValue()))
        .collect(Collectors.toList()));
    s3.setObjectTagging(new SetObjectTaggingRequest(this.bucketName, fileName, objectTagging));
  }

  @Override
  public ObjectListing list(String path) {
    return s3.listObjects(bucketName, path);
  }

  @Override
  public S3SinkConnectorConfig conf() {
    return conf;
  }

  @Override
  public String url() {
    return url;
  }

  @Override
  public SeekableInput open(String path, S3SinkConnectorConfig conf) {
    throw new UnsupportedOperationException(
        "File reading is not currently supported in S3 Connector"
    );
  }
}
