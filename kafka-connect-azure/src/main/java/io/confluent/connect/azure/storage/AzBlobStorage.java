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

package io.confluent.connect.azure.storage;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.specialized.BlobOutputStream;

import io.confluent.connect.azure.AzBlobSinkConnectorConfig;
import io.confluent.connect.storage.Storage;
import io.confluent.connect.storage.common.util.StringUtils;
import org.apache.avro.file.SeekableInput;

import java.io.OutputStream;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import static io.confluent.connect.storage.common.util.StringUtils.isNotBlank;

/*
https://docs.microsoft.com/en-us/azure/storage/blobs/storage-java-how-to-use-blob-storage
Concepts:
Storage Account: All access to Azure Storage is done through a storage account.
   This storage account can be a General-purpose storage account or a Blob storage account which
   is specialized for storing objects/blobs. See About Azure storage accounts for more information.
Container: A container provides a grouping of a set of blobs. All blobs must be in a container.
  An account can contain an unlimited number of containers. A container can store an unlimited
  number of blobs. Note that the container name must be lowercase.
Blob: A file of any type and size. Azure Storage offers three types of blobs: block blobs,
  page blobs, and append blobs.
  Block blobs are ideal for storing text or binary files, such as documents and media files.
  Append blobs are similar to block blobs in that they are made up of blocks, but they are optimized
   for append operations, so they are useful for logging scenarios. A single block blob can contain
    up to 50,000 blocks of up to 100 MB each, for a total size of slightly more than 4.75 TB
    (100 MB X 50,000). A single append blob can contain up to 50,000 blocks of up to 4 MB each, for
     a total size of slightly more than 195 GB (4 MB X 50,000).
 */
public class AzBlobStorage implements Storage<AzBlobSinkConnectorConfig, Iterable<BlobItem>> {

  private final String containerName;
  private final AzBlobSinkConnectorConfig conf;
  private static final String VERSION_FORMAT = "APN/1.0 Confluent/1.0 KafkaAZBlobConnector/%s";
  private final BlobContainerClient container;

  /**
   * Construct an AzBlobStorage class given a configuration and an AZ Storage account + container.
   *
   * @param conf the AzBlobStorage configuration.
   */
  public AzBlobStorage(AzBlobSinkConnectorConfig conf, String url) throws URISyntaxException,
          InvalidKeyException {
    this.conf = conf;
    this.containerName = conf.getAzStorageContainerName();

    // Create a BlobServiceClient object which will be used to create a container client
    BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
            .connectionString(conf.getAzStorageAccountConnectionString())
            .buildClient();

    // Create the container and return a container client object
    container = blobServiceClient.createBlobContainerIfNotExists(containerName);
  }

  @Override
  public boolean exists(String name) {
    try {
      return isNotBlank(name) && container.getBlobClient(name).exists();
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public OutputStream append(String filename) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void delete(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
  }

  @Override
  public String url() {
    return container.getBlobContainerUrl();
  }

  @Override
  public Iterable<BlobItem> list(String path) {
    return container.listBlobsByHierarchy(path);
  }

  @Override
  public AzBlobSinkConnectorConfig conf() {
    return conf;
  }

  @Override
  public SeekableInput open(String path, AzBlobSinkConnectorConfig conf) {
    throw new UnsupportedOperationException(
            "File reading is not currently supported in AZ Blob Connector");
  }

  @Override
  public boolean create(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OutputStream create(String path, AzBlobSinkConnectorConfig conf, boolean overwrite) {
    return create(path, overwrite);
  }

  public BlobOutputStream create(String path, boolean overwrite) {
    if (!overwrite) {
      throw new UnsupportedOperationException(
              "Creating a file without overwriting is not currently supported in AZ Blob Connector");
    }

    if (StringUtils.isBlank(path)) {
      throw new IllegalArgumentException("Path can not be empty!");
    }

    try {
      return container.getBlobClient(path).getBlockBlobClient().getBlobOutputStream(true);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public boolean bucketExists() throws URISyntaxException {
    return isNotBlank(containerName) && container.exists();
  }

}