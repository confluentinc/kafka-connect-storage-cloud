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

import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.format.parquet.ParquetFormat;
import io.confluent.connect.s3.util.S3ProxyConfig;
import io.confluent.connect.s3.util.Version;
import io.confluent.connect.storage.Storage;
import io.confluent.connect.storage.common.util.StringUtils;
import org.apache.avro.file.SeekableInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.retries.StandardRetryStrategy;
import software.amazon.awssdk.retries.api.RetryStrategy;
import software.amazon.awssdk.retries.api.internal.backoff.ExponentialDelayWithJitter;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.PutObjectTaggingRequest;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Tagging;

import java.io.OutputStream;
import java.net.URI;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import static io.confluent.connect.s3.S3SinkConnectorConfig.REGION_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.S3_PATH_STYLE_ACCESS_ENABLED_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.S3_PROXY_URL_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.S3_RETRY_BACKOFF_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.S3_RETRY_MAX_BACKOFF_TIME_MS;
import static io.confluent.connect.s3.S3SinkConnectorConfig.WAN_MODE_CONFIG;

/**
 * S3 implementation of the storage interface for Connect sinks.
 */
public class S3Storage implements Storage<S3SinkConnectorConfig, ListObjectsResponse> {

  private static final Logger log = LoggerFactory.getLogger(S3Storage.class);

  private final String url;
  private final String bucketName;
  private final S3Client s3;
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
  public S3Client newS3Client(S3SinkConnectorConfig config) {
    log.info("Creating S3 client.");
    ClientOverrideConfiguration clientConfiguration = newClientConfiguration(config);
    S3ClientBuilder builder = S3Client.builder()
        .accelerate(config.getBoolean(WAN_MODE_CONFIG))
        .forcePathStyle(config.getBoolean(S3_PATH_STYLE_ACCESS_ENABLED_CONFIG))
        .credentialsProvider(newCredentialsProvider(config))
        .httpClientBuilder(newHttpClient(config))
        .overrideConfiguration(clientConfiguration);

    String region = config.getString(REGION_CONFIG);
    if (StringUtils.isBlank(url)) {
      builder = "us-east-1".equals(region)
                ? builder.region(Region.US_EAST_1)
                : builder.region(Region.of(region));
    } else {
      builder = builder
          .endpointOverride(URI.create(url))
          .region(Region.of(region));
    }
    log.info("S3 client created");
    return builder.build();
  }

  // Visible for testing.
  public S3Storage(S3SinkConnectorConfig conf, String url, String bucketName, S3Client s3) {
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
  public ClientOverrideConfiguration newClientConfiguration(S3SinkConnectorConfig config) {
    String version = String.format(VERSION_FORMAT, Version.getVersion());

    ClientOverrideConfiguration clientConfiguration = ClientOverrideConfiguration.builder()
        .retryStrategy(newRetryStrategy(conf))
        .advancedOptions(
            Collections.singletonMap(SdkAdvancedClientOption.USER_AGENT_PREFIX, version))
        .build();
    /*AWS SDK for Java v2 migration: userAgentPrefix override is a request-level config in v2. See https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/core/RequestOverrideConfiguration.Builder.html#addApiName(software.amazon.awssdk.core.ApiName).*/
    // TODO: Move to individual calls
    //clientConfiguration
    //.userAgentPrefix(version) -> Refer listObjects for reference
    //.retryPolicy(newFullJitterRetryPolicy(config)); -> Move to builder

    // Moved to http client builder
    /*if (StringUtils.isNotBlank(config.getString(S3_PROXY_URL_CONFIG))) {
      S3ProxyConfig proxyConfig = new S3ProxyConfig(config);
      clientConfiguration.protocol(proxyConfig.protocol())
          .proxyHost(proxyConfig.host())
          .proxyPort(proxyConfig.port())
          .proxyUsername(proxyConfig.user())
          .proxyPassword(proxyConfig.pass());
    }*/

    return clientConfiguration;
  }

  public SdkHttpClient.Builder newHttpClient(S3SinkConnectorConfig config) {
    ApacheHttpClient.Builder clientBuilder =
        ApacheHttpClient.builder().expectContinueEnabled(config.useExpectContinue());

    if (StringUtils.isNotBlank(config.getString(S3_PROXY_URL_CONFIG))) {
      S3ProxyConfig proxyConfig = new S3ProxyConfig(config);

      clientBuilder.proxyConfiguration(
          ProxyConfiguration.builder()
              .username(proxyConfig.user())
              .password(proxyConfig.pass())
              .endpoint(proxyConfig.getURI())
              .build()
      );
    }

    return clientBuilder;
  }

  protected RetryStrategy newRetryStrategy(S3SinkConnectorConfig conf) {
    return StandardRetryStrategy.builder()
        .maxAttempts(conf.getS3PartRetries())
        .backoffStrategy(
            new ExponentialDelayWithJitter(
                Random::new,
                Duration.ofMillis(conf.getLong(S3_RETRY_BACKOFF_CONFIG).intValue()),
                S3_RETRY_MAX_BACKOFF_TIME_MS))
        .throttlingBackoffStrategy(
            // EqualJitterBackoffStrategy.builder()
            // .baseDelay(SdkDefaultRetrySetting.throttledBaseDelay(LEGACY))
            // .maxBackoffTime(SdkDefaultRetrySetting.MAX_BACKOFF).build();
            new ExponentialDelayWithJitter(
                Random::new,
                Duration.ofMillis(conf.getLong(S3_RETRY_BACKOFF_CONFIG).intValue()),
                S3_RETRY_MAX_BACKOFF_TIME_MS))
        .build();
  }

  /**
   * Creates a retry policy, based on full jitter backoff strategy
   * and default retry condition.
   * Visible for testing.
   *
   * @param config the S3 configuration.
   * @return retry policy
   */
  /*protected RetryPolicy newFullJitterRetryPolicy(S3SinkConnectorConfig config) {
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
    log.info("Created a retry policy for the connector");
    return retryPolicy;
  }*/

  protected AwsCredentialsProvider newCredentialsProvider(S3SinkConnectorConfig config) {
    log.info("Returning new credentials provider based on the configured "
           + "credentials provider class");
    return config.getCredentialsProvider();
  }

  @Override
  public boolean exists(String name) {
    if (StringUtils.isBlank(name)) {
      log.debug("Name can not be empty!");
      return false;
    }
    try {
      s3.headObject(HeadObjectRequest.builder().bucket(bucketName).key(name).build());
      return true;
    } catch (AwsServiceException ase) {
      // A redirect error or an AccessDenied exception means the bucket exists but it's not in this
      // region or we don't have permissions to it.
      if ((ase.statusCode() == HttpStatusCode.MOVED_PERMANENTLY)
          || "AccessDenied".equals(ase.awsErrorDetails().errorCode())) {
        return true;
      }
      if (ase.statusCode() == HttpStatusCode.NOT_FOUND) {
        return false;
      }
      throw ase;
    }
  }

  public boolean bucketExists() {
    if (StringUtils.isBlank(bucketName)) {
      return false;
    }
    try {
      s3.headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
      return true;
    } catch (AwsServiceException ase) {
      // A redirect error or an AccessDenied exception means the bucket exists but it's not in
      // this region or we don't have permissions to it.
      if ((ase.statusCode() == HttpStatusCode.MOVED_PERMANENTLY)
          || "AccessDenied".equals(ase.awsErrorDetails().errorCode())) {
        System.out.println("bucket exists " + ase.statusCode());
      }
      if (ase.statusCode() == HttpStatusCode.NOT_FOUND) {
        System.out.println("bucket does not exist " + ase.statusCode());

      }
      throw ase;
    }
  }

  @Override
  public boolean create(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OutputStream create(String path, S3SinkConnectorConfig conf, boolean overwrite) {
    return create(path, overwrite, this.conf.getClass(S3SinkConnectorConfig.FORMAT_CLASS_CONFIG));
  }

  public S3OutputStream create(String path, boolean overwrite, Class<?> formatClass) {
    log.info("Creating S3 output stream.");
    if (!overwrite) {
      log.debug("Creating a file without overwriting is not currently supported in S3 Connector");
      throw new UnsupportedOperationException(
          "Creating a file without overwriting is not currently supported in S3 Connector"
      );
    }

    if (StringUtils.isBlank(path)) {
      log.debug("Path can not be empty!");
      throw new IllegalArgumentException("Path can not be empty!");
    }

    if (ParquetFormat.class.isAssignableFrom(formatClass)) {
      log.info("Create S3ParquetOutputStream for bucket '{}' key '{}'",
              this.conf.getBucketName(), path);
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
      s3.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(name).build());
    }
  }

  @Override
  public void close() {}

  public void addTags(String fileName, Map<String, String> tags) throws AwsServiceException,
      SdkClientException,
      S3Exception {
    Collection<Tag> tagSet = tags.entrySet().stream()
        .map(e -> Tag.builder()
            .key(e.getKey())
            .value(e.getValue())
            .build())
        .collect(Collectors.toList());
    PutObjectTaggingRequest request = PutObjectTaggingRequest.builder().bucket(bucketName)
            .key(fileName)
                .tagging(Tagging.builder().tagSet(tagSet).build())
                    .build();
    s3.putObjectTagging(request);
  }

  //Example of request override configuration
  @Override
  public ListObjectsResponse list(String path) {
    return s3.listObjects(ListObjectsRequest.builder().bucket(bucketName).prefix(path)
        //.overrideConfiguration(requestOverrideConfiguration)
        .build());
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
    log.debug("File reading is not currently supported in S3 Connector");
    throw new UnsupportedOperationException(
        "File reading is not currently supported in S3 Connector"
    );
  }
}
