/*
 * Copyright [2020 - 2020] Confluent Inc.
 */

package io.confluent.connect.s3.integration;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.confluent.common.utils.IntegrationTest;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.TestUtils;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Category(IntegrationTest.class)
public abstract class BaseConnectorIT {

  private static final Logger log = LoggerFactory.getLogger(BaseConnectorIT.class);

  protected static final long CONSUME_MAX_DURATION_MS = TimeUnit.SECONDS.toMillis(60);
  protected static final long CONNECTOR_STARTUP_DURATION_MS = TimeUnit.SECONDS.toMillis(1200);
  protected static final String JSON_FORMAT_CLASS = "io.confluent.connect.s3.format.json.JsonFormat";
  protected static final String S3_BUCKET = "sink-test-bucket";

  protected static AmazonS3 s3RootClient;

  protected static PumbaPauseContainer pumbaPauseContainer;

  protected EmbeddedConnectCluster connect;

  protected void startConnect() throws IOException {
    connect = new EmbeddedConnectCluster.Builder()
        .name("s3-connect-cluster")
        //.workerProps(props)
        .build();
    connect.start();
  }

  protected void stopConnect() {
    connect.stop();
  }

  /**
   * Wait up to {@link #CONNECTOR_STARTUP_DURATION_MS maximum time limit} for the connector with the
   * given name to start the specified number of tasks.
   *
   * @param name the name of the connector
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
   * Confirm that a connector with an exact number of tasks is running.
   *
   * @param connectorName the connector
   * @param numTasks the minimum number of tasks
   * @return true if the connector and tasks are in RUNNING state; false otherwise
   */
  protected Optional<Boolean> assertConnectorAndTasksRunning(String connectorName, int numTasks) {
    try {
      ConnectorStateInfo info = connect.connectorStatus(connectorName);
      boolean result = info != null
                       && info.tasks().size() >= numTasks
                       && info.connector().state().equals(AbstractStatus.State.RUNNING.toString())
                       && info.tasks().stream().allMatch(s -> s.state().equals(AbstractStatus.State.RUNNING.toString()));
      return Optional.of(result);
    } catch (Exception e) {
      log.error("Could not check connector state info.", e);
      return Optional.empty();
    }
  }

  /**
   * Creates root client that will be used to change bucket permissions.
   * Prerequisite : Access key and Secret access key should be set as environment variables
   */
  protected void createS3RootClient() {
    s3RootClient = AmazonS3ClientBuilder.standard()
        .withCredentials(
            new AWSStaticCredentialsProvider(
                new BasicAWSCredentials(
                    System.getenv("ROOT_USER_ACCESS_KEY_ID"),
                    System.getenv("ROOT_USER_SECRET_ACCESS_KEY"))))
        .withRegion("ap-south-1")
        .build();
  }


  protected void waitForConnectorToCompleteSendingRecords(
      long noOfRecordsProduced,
      int flushSize,
      String bucketname)
      throws InterruptedException {
    TestUtils.waitForCondition(
      () -> assertSuccess(noOfRecordsProduced, flushSize, bucketname).orElse(false),
      CONNECTOR_STARTUP_DURATION_MS,
      "Connector could not send all records in time."
    );
  }

  protected Optional<Boolean> assertSuccess(
      long noOfRecordsProduced,
      int flushSize,
      String bucketname) {
    try {
      int noOfObjectsInS3 = getNoOfObjectsInS3(bucketname);
      boolean result = noOfObjectsInS3 == Math.ceil(noOfRecordsProduced / flushSize);
      return Optional.of(result);
    } catch (Exception e) {
      log.error("Could not check S3 state", e);
      return Optional.empty();
    }
  }

  protected int getNoOfObjectsInS3(String bucketName) {
    ListObjectsV2Request req = new ListObjectsV2Request()
        .withBucketName(bucketName)
        .withMaxKeys(100);
    ListObjectsV2Result result;
    List<String> records = new ArrayList<>();

    do {
      result = s3RootClient.listObjectsV2(req);

      for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
        if (objectSummary.getSize() > 0) {
          records.add(objectSummary.getKey());
        }
      }
      // If there are more than maxKeys keys in the bucket, get a continuation token
      // and list the next objects.
      String token = result.getNextContinuationToken();
      req.setContinuationToken(token);
    } while (result.isTruncated());
    return records.size();
  }

  protected long waitForFetchingStorageObjectsInS3(String bucketName, long expectedObjects)
      throws InterruptedException {
    return waitForFetchingStorageObjectsInS3(bucketName,
        expectedObjects,
        CONSUME_MAX_DURATION_MS);
  }

  protected long waitForFetchingStorageObjectsInS3(
      String bucketName,
      long expectedObjects,
      long maxWaitMs)
      throws InterruptedException {
    long startTime = System.currentTimeMillis();
    long fetchedObjects = -1;
    while (System.currentTimeMillis() - startTime < maxWaitMs) {
      // added intentional sleep to get a connection from the connection pool.
      Thread.sleep(Math.min(maxWaitMs, 5000L));
      fetchedObjects = getNoOfObjectsInS3(bucketName);
      if (fetchedObjects == expectedObjects) {
        break;
      }
    }
    return fetchedObjects;
  }

  protected void startPumbaPauseContainer() {
    pumbaPauseContainer = new PumbaPauseContainer();
    pumbaPauseContainer.start();
  }

}
