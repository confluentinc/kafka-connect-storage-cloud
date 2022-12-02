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

package io.confluent.connect.s3.integration;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.confluent.common.utils.IntegrationTest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.confluent.connect.s3.util.S3Utils;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;

import static org.assertj.core.api.Assertions.assertThat;

@Category(IntegrationTest.class)
public abstract class BaseConnectorIT {
  protected static final int MAX_TASKS = 3;
  private static final long S3_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(60);

  protected static AmazonS3 S3Client;
  protected static final String TEST_BUCKET_NAME =
      "connect-s3-integration-testing-" + System.currentTimeMillis();
  protected EmbeddedConnectCluster connect;
  protected Map<String, String> props;

  @Before
  public void setup() {
    startConnect();
  }

  @After
  public void close() {
    // stop all Connect, Kafka and Zk threads.
    connect.stop();
  }

  protected void startConnect() {
    connect = new EmbeddedConnectCluster.Builder()
        .name("s3-connect-cluster")
        .build();

    // start the clusters
    connect.start();
  }


  /**
   * Wait up to {@link #S3_TIMEOUT_MS maximum time limit} for the connector to write the specified
   * number of files.
   *
   * @param bucketName  S3 bucket name
   * @param numFiles    expected number of files in the bucket
   * @return the time this method discovered the connector has written the files
   * @throws InterruptedException if this was interrupted
   */
  protected long waitForFilesInBucket(String bucketName, int numFiles) throws InterruptedException {
    return S3Utils.waitForFilesInBucket(S3Client, bucketName, numFiles, S3_TIMEOUT_MS);
  }

  /**
   * Get a list of the expected filenames for the bucket.
   * <p>
   * Format: topics/s3_topic/partition=97/s3_topic+97+0000000001.avro
   *
   * @param topic      the test kafka topic
   * @param partition  the expected partition for the tests
   * @param flushSize  the flush size connector config
   * @param numRecords the number of records produced in the test
   * @param extension  the expected extensions of the files including compression (snappy.parquet)
   * @return the list of expected filenames
   */
  protected List<String> getExpectedFilenames(
      String topic,
      int partition,
      int flushSize,
      long numRecords,
      String extension
  ) {
    int expectedFileCount = (int) numRecords / flushSize;
    List<String> expectedFiles = new ArrayList<>();
    for (int offset = 0; offset < expectedFileCount * flushSize; offset += flushSize) {
      String filepath = String.format(
          "topics/%s/partition=%d/%s+%d+%010d.%s",
          topic,
          partition,
          topic,
          partition,
          offset,
          extension
      );
      expectedFiles.add(filepath);
    }
    return expectedFiles;
  }

  /**
   * Get a list of the expected filenames containing keys for the tombstone records for the bucket.
   * <p>
   * Format: topics/s3_topic/tombstone/s3_topic+97+0000000001.keys.avro
   *
   * @param topic      the test kafka topic
   * @param partition  the expected partition for the tests
   * @param flushSize  the flush size connector config
   * @param numRecords the number of records produced in the test
   * @param extension  the expected extensions of the files including compression (snappy.parquet)
   * @param tombstonePartition  the expected directory for tombstone records
   * @return the list of expected filenames
   */
  protected List<String> getExpectedTombstoneFilenames(
      String topic,
      int partition,
      int flushSize,
      long numRecords,
      String extension,
      String tombstonePartition
  ) {
    int expectedFileCount = (int) numRecords / flushSize;
    List<String> expectedFiles = new ArrayList<>();
    for (int offset = 0; offset < expectedFileCount * flushSize; offset += flushSize) {
      String filepath = String.format(
          "topics/%s/%s/%s+%d+%010d.keys.%s",
          topic,
          tombstonePartition,
          topic,
          partition,
          offset,
          extension
      );
      expectedFiles.add(filepath);
    }
    return expectedFiles;
  }

  /**
   * Check if the file names in the bucket have the expected namings.
   *
   * @param bucketName    the name of the bucket with the files
   * @param expectedFiles the list of expected filenames for exact comparison
   * @return whether all the files in the bucket match the expected values
   */
  protected void assertFileNamesValid(String bucketName, List<String> expectedFiles) {
    List<String> actualFiles = getBucketFileNames(bucketName);
    assertThat(actualFiles).containsExactlyInAnyOrderElementsOf(expectedFiles);
  }

  /**
   * Recursively query the bucket to get a list of filenames that exist in the bucket.
   *
   * @param bucketName the name of the bucket containing the files.
   * @return list of filenames present in the bucket.
   */
  private List<String> getBucketFileNames(String bucketName) {
    List<String> actualFiles = new ArrayList<>();
    ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(bucketName);
    ListObjectsV2Result result;
    do {
      /*
       Need the result object to extract the continuation token from the request as each request
       to listObjectsV2() returns a maximum of 1000 files.
       */
      result = S3Client.listObjectsV2(request);
      for (S3ObjectSummary file : result.getObjectSummaries()) {
        actualFiles.add(file.getKey());
      }
      String token = result.getNextContinuationToken();
      // To get the next batch of files.
      request.setContinuationToken(token);
    } while(result.isTruncated());
    return actualFiles;
  }
}
