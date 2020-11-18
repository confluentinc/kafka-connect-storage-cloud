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
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(IntegrationTest.class)
public abstract class BaseConnectorIT {

  private static final Logger log = LoggerFactory.getLogger(BaseConnectorIT.class);

  protected static final int MAX_TASKS = 3;
  private static final long CONNECTOR_STARTUP_DURATION_MS = TimeUnit.MINUTES.toMillis(1);
  private static final long S3_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(30);

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
   * Wait up to {@link #CONNECTOR_STARTUP_DURATION_MS maximum time limit} for the connector with the
   * given name to start the specified number of tasks.
   *
   * @param name     the name of the connector
   * @param numTasks the minimum number of tasks that are expected
   * @return the time this method discovered the connector has started, in milliseconds past epoch
   * @throws InterruptedException if this was interrupted
   */
  protected long waitForConnectorToStart(String name, int numTasks) throws InterruptedException {
    TestUtils.waitForCondition(
        () -> assertConnectorAndTasksRunning(name, numTasks).orElse(false),
        CONNECTOR_STARTUP_DURATION_MS,
        "Connector tasks did not start in time."
    );
    return System.currentTimeMillis();
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
    TestUtils.waitForCondition(
        () -> assertFileCountInBucket(bucketName, numFiles).orElse(false),
        S3_TIMEOUT_MS,
        "Files not written to S3 bucket in time."
    );
    return System.currentTimeMillis();
  }

  /**
   * Confirm that a connector with an exact number of tasks is running.
   *
   * @param connectorName the connector
   * @param numTasks      the minimum number of tasks
   * @return true if the connector and tasks are in RUNNING state; false otherwise
   */
  protected Optional<Boolean> assertConnectorAndTasksRunning(String connectorName, int numTasks) {
    try {
      ConnectorStateInfo info = connect.connectorStatus(connectorName);
      boolean result = info != null
          && info.tasks().size() >= numTasks
          && info.connector().state().equals(AbstractStatus.State.RUNNING.toString())
          && info.tasks().stream()
          .allMatch(s -> s.state().equals(AbstractStatus.State.RUNNING.toString()));
      return Optional.of(result);
    } catch (Exception e) {
      log.warn("Could not check connector state info.");
      return Optional.empty();
    }
  }

  /**
   * Confirm that the file count in a bucket matches the expected number of files.
   *
   * @param bucketName the name of the bucket containing the files
   * @param expectedNumFiles the number of files expected
   * @return true if the number of files in the bucket match the expected number; false otherwise
   */
  protected Optional<Boolean> assertFileCountInBucket(String bucketName, int expectedNumFiles) {
    try {
      return Optional.of(getBucketFileCount(bucketName) == expectedNumFiles);
    } catch (Exception e) {
      log.warn("Could not check file count in bucket: {}", bucketName);
      return Optional.empty();
    }
  }

  /**
   * Recursively query the bucket to get the total number of files that exist in the bucket.
   *
   * @param bucketName the name of the bucket containing the files.
   * @return the number of files in the bucket
   */
  private int getBucketFileCount(String bucketName) {
    int totalFilesInBucket = 0;
    ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(bucketName);
    ListObjectsV2Result result;
    do {
      /*
       Need the result object to extract the continuation token from the request as each request
       to listObjectsV2() returns a maximum of 1000 files.
       */
      result = S3Client.listObjectsV2(request);
      totalFilesInBucket += result.getKeyCount();
      String token = result.getNextContinuationToken();
      // To get the next batch of files.
      request.setContinuationToken(token);
    } while(result.isTruncated());
    return totalFilesInBucket;
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
   * Check if the file names in the bucket have the expected namings.
   *
   * @param bucketName    the name of the bucket with the files
   * @param expectedFiles the list of expected filenames for exact comparison
   * @return whether all the files in the bucket match the expected values
   */
  protected boolean fileNamesValid(String bucketName, List<String> expectedFiles) {
    List<String> actualFiles = getBucketFileNames(bucketName);
    return expectedFiles.equals(actualFiles);
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
