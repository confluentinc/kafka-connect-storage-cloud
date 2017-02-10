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

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import org.apache.avro.file.SeekableInput;

import java.io.OutputStream;

import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.storage.Storage;
import io.confluent.connect.storage.common.util.StringUtils;

import static io.confluent.connect.s3.S3SinkConnectorConfig.WAN_MODE_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.REGION_CONFIG;

/**
 * S3 implementation of the storage interface for Connect sinks.
 */
public class S3Storage implements Storage<S3SinkConnectorConfig, ObjectListing> {

  private final String url;
  private final String bucketName;
  private final AmazonS3 s3;
  private final S3SinkConnectorConfig conf;

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

  public AmazonS3 newS3Client(S3SinkConnectorConfig config) {
    AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard()
                                        .withAccelerateModeEnabled(config.getBoolean(WAN_MODE_CONFIG))
                                        .withPathStyleAccessEnabled(true)
                                        .withCredentials(config.getCredentialsProvider());

    builder = StringUtils.isBlank(url) ?
                  builder.withRegion(config.getString(REGION_CONFIG)) :
                  builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(url, ""));

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
   * {@inheritDoc}
   *
   * @throws SdkClientException
   * @throws AmazonServiceException
   */
  @Override
  public boolean exists(String name) {
    return StringUtils.isNotBlank(name) && s3.doesObjectExist(bucketName, name);
  }

  /**
   * {@inheritDoc}
   *
   * @throws SdkClientException
   * @throws AmazonServiceException
   */
  public boolean bucketExists() {
    return StringUtils.isNotBlank(bucketName) && s3.doesBucketExist(bucketName);
  }

  /**
   * {@inheritDoc}
   *
   * @throws SdkClientException
   * @throws AmazonServiceException
   */
  @Override
  public boolean create(String name) {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   * This method is not supported in S3 storage.
   *
   * @throws UnsupportedOperationException
   */
  @Override
  public OutputStream append(String filename) {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   *
   * @throws SdkClientException
   * @throws AmazonServiceException
   */
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

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() {}

  /**
   * {@inheritDoc}
   *
   * @throws SdkClientException
   * @throws AmazonServiceException
   */
  @Override
  public ObjectListing list(String path) {
    return s3.listObjects(bucketName, path);
  }

  @Override
  public S3SinkConnectorConfig conf() { return conf; }

  @Override
  public String url() {
    return url;
  }

  @Override
  public SeekableInput open(String path, S3SinkConnectorConfig conf) {
    throw new UnsupportedOperationException("File reading is not currently supported in S3 Connector");
  }

  @Override
  public OutputStream create(String path, S3SinkConnectorConfig conf, boolean overwrite) {
    return create(path, overwrite);
  }

  public S3OutputStream create(String path, boolean overwrite) {
    if (!overwrite) {
      throw new UnsupportedOperationException("Creating a file without overwriting is not currently supported in S3 Connector");
    }

    if (StringUtils.isBlank(path)) {
      throw new IllegalArgumentException("Path can not be empty!");
    }

    // currently ignore what is passed as method argument.
    return new S3OutputStream(path, this.conf, s3);
  }
}
