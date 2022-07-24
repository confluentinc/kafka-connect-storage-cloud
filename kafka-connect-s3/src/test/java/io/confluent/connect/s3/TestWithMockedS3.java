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

package io.confluent.connect.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
import com.amazonaws.services.s3.model.transform.XmlResponsesSaxParser;
import io.confluent.connect.s3.format.parquet.ParquetUtils;
import io.findify.s3mock.S3Mock;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.RetriableException;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import io.confluent.connect.s3.format.avro.AvroUtils;
import io.confluent.connect.s3.format.bytearray.ByteArrayUtils;
import io.confluent.connect.s3.format.json.JsonUtils;
import io.confluent.connect.s3.storage.CompressionType;
import io.confluent.connect.s3.storage.S3OutputStream;
import io.confluent.connect.s3.util.FileUtils;
import io.confluent.connect.storage.common.StorageCommonConfig;

public class TestWithMockedS3 extends S3SinkConnectorTestBase {

  private static final Logger log = LoggerFactory.getLogger(TestWithMockedS3.class);

  protected S3Mock s3mock;
  protected String port;
  @Rule
  public TemporaryFolder s3mockRoot = new TemporaryFolder();

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.put(StorageCommonConfig.DIRECTORY_DELIM_CONFIG, "_");
    props.put(StorageCommonConfig.FILE_DELIM_CONFIG, "#");
    return props;
  }

  //@Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    if (StringUtils.isBlank(port)) {
        port = url.substring(url.lastIndexOf(":") + 1);
    }
    File s3mockDir = s3mockRoot.newFolder("s3-tests-" + UUID.randomUUID().toString());
    System.out.println("Create folder: " + s3mockDir.getCanonicalPath());
    s3mock = S3Mock.create(Integer.parseInt(port), s3mockDir.getCanonicalPath());
    s3mock.start();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    if (s3mock != null) {
      s3mock.shutdown(); // shutdown the Akka and HTTP frameworks to close all connections
    }
  }

  public static List<S3ObjectSummary> listObjects(String bucket, String prefix, AmazonS3 s3) {
    List<S3ObjectSummary> objects = new ArrayList<>();
    ObjectListing listing;

    try {
      if (prefix == null) {
        listing = s3.listObjects(bucket);
      } else {
        listing = s3.listObjects(bucket, prefix);
      }

      objects.addAll(listing.getObjectSummaries());
      while (listing.isTruncated()) {
        listing = s3.listNextBatchOfObjects(listing);
        objects.addAll(listing.getObjectSummaries());
      }
    } catch (AmazonS3Exception e) {
     log.warn("listObjects for bucket '{}' prefix '{}' returned error code: {}", bucket, prefix, e.getStatusCode());
    }

    return objects;
  }

  public static Collection<Object> readRecords(String topicsDir, String directory, TopicPartition tp, long startOffset,
                                               String extension, String zeroPadFormat, String bucketName, AmazonS3 s3) throws IOException {
      String fileKey = FileUtils.fileKeyToCommit(topicsDir, directory, tp, startOffset,
          extension, zeroPadFormat);
      CompressionType compressionType = CompressionType.NONE;
      if (extension.endsWith(".gz")) {
        compressionType = CompressionType.GZIP;
      }
      if (".avro".equals(extension)) {
        return readRecordsAvro(bucketName, fileKey, s3);
      } else if (extension.startsWith(".json")) {
        return readRecordsJson(bucketName, fileKey, s3, compressionType);
      } else if (extension.startsWith(".bin")) {
        return readRecordsByteArray(bucketName, fileKey, s3, compressionType,
            S3SinkConnectorConfig.FORMAT_BYTEARRAY_LINE_SEPARATOR_DEFAULT.getBytes());
      } else if (extension.endsWith(".parquet")) {
          return readRecordsParquet(bucketName, fileKey, s3);
      } else if (extension.startsWith(".customExtensionForTest")) {
        return readRecordsByteArray(bucketName, fileKey, s3, compressionType,
            "SEPARATOR".getBytes());
      } else {
        throw new IllegalArgumentException("Unknown extension: " + extension);
      }
  }

  public static Collection<Object> readRecordsAvro(String bucketName, String fileKey, AmazonS3 s3) throws IOException {
      log.debug("Reading records from bucket '{}' key '{}': ", bucketName, fileKey);
      InputStream in = s3.getObject(bucketName, fileKey).getObjectContent();

      return AvroUtils.getRecords(in);
  }

  public static Collection<Object> readRecordsJson(String bucketName, String fileKey, AmazonS3 s3,
                                                   CompressionType compressionType) throws IOException {
      log.debug("Reading records from bucket '{}' key '{}': ", bucketName, fileKey);
      InputStream in = s3.getObject(bucketName, fileKey).getObjectContent();

      return JsonUtils.getRecords(compressionType.wrapForInput(in));
  }

  public static Collection<Object> readRecordsByteArray(String bucketName, String fileKey, AmazonS3 s3,
                                                        CompressionType compressionType, byte[] lineSeparatorBytes) throws IOException {
      log.debug("Reading records from bucket '{}' key '{}': ", bucketName, fileKey);
      InputStream in = s3.getObject(bucketName, fileKey).getObjectContent();

      return ByteArrayUtils.getRecords(compressionType.wrapForInput(in), lineSeparatorBytes);
  }

  public static List<Tag> getS3ObjectTags(String bucketName, String fileKey, AmazonS3 s3) throws IOException {
      //findify S3 mock does not currently support S3 object tag mocks, instead tags are stored as object data in AWS XML format
      //leverage this workaround to parse the xml until tag mocks are supported
      log.debug("Reading tags from bucket '{}' key '{}': ", bucketName, fileKey);
      InputStream in = s3.getObject(bucketName, fileKey).getObjectContent();
      XmlResponsesSaxParser parser = new XmlResponsesSaxParser();
      GetObjectTaggingResult tagsResult = parser.parseObjectTaggingResponse(in).getResult();

      List<Tag> tagList = new ArrayList<>();
      tagList.addAll(tagsResult.getTagSet());
      return tagList;
  }

  public static Collection<Object> readRecordsParquet(String bucketName, String fileKey, AmazonS3 s3) throws IOException {
      log.debug("Reading records from bucket '{}' key '{}': ", bucketName, fileKey);
      InputStream in = s3.getObject(bucketName, fileKey).getObjectContent();
      return ParquetUtils.getRecords(in, fileKey);
  }

  @Override
  public AmazonS3 newS3Client(S3SinkConnectorConfig config) {
    final AWSCredentialsProvider provider = new AWSCredentialsProvider() {
      private final AnonymousAWSCredentials credentials = new AnonymousAWSCredentials();
      @Override
      public AWSCredentials getCredentials() {
        return credentials;
      }

      @Override
      public void refresh() {
      }
    };

    AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard()
               .withAccelerateModeEnabled(config.getBoolean(S3SinkConnectorConfig.WAN_MODE_CONFIG))
               .withPathStyleAccessEnabled(config.getBoolean(S3SinkConnectorConfig.S3_PATH_STYLE_ACCESS_ENABLED_CONFIG))
               .withCredentials(provider);

    builder = url == null ?
                  builder.withRegion(config.getString(S3SinkConnectorConfig.REGION_CONFIG)) :
                  builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(url, ""));

    return builder.build();
  }

  class S3OutputStreamFlaky extends S3OutputStream {
    private final AtomicInteger retries;

    public S3OutputStreamFlaky(String key, S3SinkConnectorConfig conf, AmazonS3 s3, AtomicInteger retries) {
      super(key, conf, s3);
      this.retries = retries;
    }

    @Override
    public void commit() throws IOException {
      if (retries.getAndIncrement() == 0) {
        close();
        throw new RetriableException("Fake exception");
      }
      super.commit();
    }
  }
}
