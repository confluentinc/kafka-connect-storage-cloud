/*
 * Copyright [2020 - 2020] Confluent Inc.
 */

package io.confluent.connect.s3.integration;

import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.confluent.common.utils.IntegrationTest;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.TestUtils;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Category(IntegrationTest.class)
public abstract class BaseConnectorIT {

  private static final Logger log = LoggerFactory.getLogger(BaseConnectorIT.class);

  protected static final long CONSUME_MAX_DURATION_MS = TimeUnit.SECONDS.toMillis(60);
  protected static final long CONNECTOR_STARTUP_DURATION_MS = TimeUnit.SECONDS.toMillis(60);
  protected static final String JSON_FORMAT_CLASS = "io.confluent.connect.s3.format.json.JsonFormat";
  protected static final String S3_BUCKET = "mytestbucket";

  @ClassRule
  public static TemporaryFolder s3mockRoot = new TemporaryFolder();

  @ClassRule
  public static S3MockRule S3_MOCK_RULE;

  protected static AmazonS3 s3;

  protected EmbeddedConnectCluster connect;

  static {
    try {
      s3mockRoot.create();
      File s3mockDir = s3mockRoot.newFolder("s3-tests-" + UUID.randomUUID().toString());
      log.info("Create folder: " + s3mockDir.getCanonicalPath());
      S3_MOCK_RULE = S3MockRule.builder()
        .withRootFolder(s3mockDir.getAbsolutePath()).silent().build();
    } catch (IOException e) {
      log.error("Erorr while running S3 mock. {}", e);
    }
  }

  protected void startConnect() throws IOException {
    connect = new EmbeddedConnectCluster.Builder()
        .name("my-connect-cluster")
        .build();
    connect.start();
  }

  protected void stopConnect() {
    connect.stop();
  }

  /**
   * Wait up to {@link #CONNECTOR_STARTUP_DURATION_MS maximum time limit} for the connector with the given
   * name to start the specified number of tasks.
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

  protected void waitForConnectorToCompleteSendingRecords(long noOfRecordsProduced, int flushSize) throws InterruptedException {
    TestUtils.waitForCondition(
      () -> assertConnectorAndDestinationRecords(noOfRecordsProduced, flushSize).orElse(false),
      CONNECTOR_STARTUP_DURATION_MS,
      "Connector could not send all records in time."
    );
  }

  protected Optional<Boolean> assertConnectorAndDestinationRecords(long noOfRecordsProduced, int flushSize) {
    try {
    int noOfObjectsInS3 = getNoOfObjectsInS3();
    boolean result = noOfObjectsInS3 == noOfRecordsProduced/flushSize;
    return Optional.of(result);
  } catch (Exception e) {
    log.error("Could not check S3 state", e);
    return Optional.empty();
  }
  }

  protected int getNoOfObjectsInS3() {
    ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(S3_BUCKET).withMaxKeys(2);
    ListObjectsV2Result result;
    List<S3Object> records = new ArrayList<>();

    do {
      result = s3.listObjectsV2(req);

      for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
        if(objectSummary.getSize()>0) {
          records.add(s3.getObject(S3_BUCKET, objectSummary.getKey()));
        }
      }
      // If there are more than maxKeys keys in the bucket, get a continuation token
      // and list the next objects.
      String token = result.getNextContinuationToken();
      req.setContinuationToken(token);
    } while (result.isTruncated());
    return records.size();
  }

}
