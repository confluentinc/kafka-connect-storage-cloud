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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;

import io.confluent.connect.storage.Storage;
import io.confluent.connect.storage.wal.WAL;

/**
 * S3 implementation of the storage interface for Connect sinks.
 */
public class S3Storage implements Storage<ObjectListing, String, S3StorageConfig> {

  private final String url;
  private final String bucketName;
  private final AmazonS3Client client;
  private final S3StorageConfig conf;

  /**
   * Construct an S3 storage class given a configuration and an AWS S3 address.
   *
   * @param conf the S3 configuration.
   * @param url  the S3 address.
   */
  public S3Storage(S3StorageConfig conf, String url) {
    this.url = url;
    this.conf = conf;
    this.bucketName = conf.getBucketName();
    this.client = new AmazonS3Client(conf.getProvider(), conf.getClientConfig(), conf.getCollector());
  }

  /**
   * {@inheritDoc}
   *
   * @throws SdkClientException
   * @throws AmazonServiceException
   */
  @Override
  public boolean exists(String name) {
    return client.doesObjectExist(bucketName, name);
  }

  /**
   * {@inheritDoc}
   *
   * @throws SdkClientException
   * @throws AmazonServiceException
   */
  @Override
  public boolean mkdirs(String name) {
    return client.createBucket(name) != null;
  }

  /**
   * {@inheritDoc}
   * This method is not supported in S3 storage.
   *
   * @throws UnsupportedOperationException
   */
  @Override
  public void append(String filename, Object object) {
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
      client.deleteBucket(name);
    } else {
      client.deleteObject(bucketName, name);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @throws SdkClientException
   * @throws AmazonServiceException
   */
  @Override
  public void commit(String temp, String committed) {
    // TODO: Verify this functionality is needed. Throw new UnsupportedOperationException() otherwise
    renameObject(temp, committed);
  }

  /**
   * {@inheritDoc}
   * This method is not supported in S3 storage.
   *
   * @throws UnsupportedOperationException
   */
  @Override
  public void close() {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   * This method is not supported in S3 storage.
   *
   * @throws UnsupportedOperationException
   */
  @Override
  public WAL wal(String topicsDir, TopicPartition topicPart) {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   *
   * @throws SdkClientException
   * @throws AmazonServiceException
   */
  @Override
  public ObjectListing listStatus(String path, String filter) throws IOException {
    // TODO: Need to figure out who filter has worked and see if using marker does the job.
    return listStatus(path);
  }

  /**
   * {@inheritDoc}
   *
   * @throws SdkClientException
   * @throws AmazonServiceException
   */
  @Override
  public ObjectListing listStatus(String path) throws IOException {
    return client.listObjects(bucketName, path);
  }

  @Override
  public S3StorageConfig conf() {
    return conf;
  }

  @Override
  public String url() {
    return url;
  }

  private void renameObject(String source, String target) {
    if (!source.equals(target) && client.doesObjectExist(bucketName, source)) {
      client.copyObject(bucketName, source, bucketName, target);
      client.deleteObject(bucketName, source);
    }
  }
}
