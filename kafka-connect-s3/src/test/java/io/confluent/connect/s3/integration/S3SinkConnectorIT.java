/*
 * Copyright [2020 - 2020] Confluent Inc.
 */

package io.confluent.connect.s3.integration;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.actions.S3Actions;
import com.amazonaws.auth.policy.resources.S3ObjectResource;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.VersionListing;
import io.confluent.common.utils.IntegrationTest;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.storage.StringConverter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.runtime.ConnectorConfig.*;
import static org.junit.Assert.assertEquals;

@Category(IntegrationTest.class)
public class S3SinkConnectorIT extends BaseConnectorIT {

  private static final Logger log = LoggerFactory.getLogger(S3SinkConnectorIT.class);

  private static final String CONNECTOR_NAME = "s3-sink-connector";
  private static final long NUM_RECORDS_PRODUCED = 1000;
  private static final int TASKS_MAX = 1;
  private static final List<String> KAFKA_TOPICS = Arrays.asList("kafka1");
  private static final int FLUSH_SIZE = 200;
  private int totalNoOfRecordsProduced = 0;

  @Before
  public void setup() throws IOException {
    startConnect();
    createS3RootClient();
  }

  /**
   * Creates root client that will be used to change bucket permissions.
   * Prerequisite : Access key and Secret access key should be set as environment variables
   */
  private void createS3RootClient() {
    s3RootClient = AmazonS3ClientBuilder.standard()
      .withCredentials(
          new AWSStaticCredentialsProvider(
              new BasicAWSCredentials(
                System.getenv("ROOT_USER_ACCESS_KEY_ID"),
                System.getenv("ROOT_USER_SECRET_ACCESS_KEY"))))
      .withRegion("ap-south-1")
      .build();
  }

  @After
  public void close() {
    stopConnect();
  }

  /**
   * Success scenario : End to end test
   * @throws Throwable
   */
  @Test
  public void testToAssertConnectorAndDestinationRecords() throws Throwable {
    String bucketName = "conn-dest-bucket1";
    // create bucket
    createS3Bucket(bucketName);
    // create topics in Kafka
    KAFKA_TOPICS.forEach(topic -> connect.kafka().createTopic(topic, 1));
    // send records to kafka
    sendRecordsToKafka();

    Map<String, String> props = getProperties();
    props.put("s3.bucket.name", bucketName);

    // start a sink connector
    connect.configureConnector(CONNECTOR_NAME, props);
    // wait for tasks to spin up
    int minimumNumTasks = Math.min(KAFKA_TOPICS.size(), TASKS_MAX);

    waitForConnectorToStart(CONNECTOR_NAME, minimumNumTasks);
    waitForConnectorToCompleteSendingRecords(totalNoOfRecordsProduced, FLUSH_SIZE, bucketName);

    // assert records
    assertEquals(getNoOfObjectsInS3(bucketName), totalNoOfRecordsProduced/FLUSH_SIZE);

    // delete the bucket
    deleteBucket(bucketName);
  }

  /**
   * Case in which bucket permissions/policies are changed while uploading records
   * Prerequisite : Access key and Secret access key should be set as environment variables
   * Blocker : Assert Condition
   * @throws Exception
   */
  @Test
  public void testIfBucketPermissionIsChangedWhileUploading() throws Exception {
    String bucketName = "conn-dest-bucket4";

    createS3Bucket(bucketName);
    addReadWritePolicyToBucket(bucketName);
    KAFKA_TOPICS.forEach(topic -> connect.kafka().createTopic(topic, 1));

    // send records to kafka
    sendRecordsToKafka();

    Map<String, String> props = getProperties();
    props.put("s3.bucket.name", bucketName);
    props.put("aws.access.key.id", System.getenv("SECONDARY_USER_ACCESS_KEY_ID"));
    props.put("aws.secret.access.key", System.getenv("SECONDARY_USER_SECRET_ACCESS_KEY"));

    // start a sink connector
    connect.configureConnector(CONNECTOR_NAME, props);
    // wait for tasks to spin up
    int minimumNumTasks = Math.min(KAFKA_TOPICS.size(), TASKS_MAX);

    waitForConnectorToStart(CONNECTOR_NAME, minimumNumTasks);
    Thread.sleep(5000);

    // revoke read/write permission
    s3RootClient.deleteBucketPolicy(bucketName);

    // produce more records to kafka
    sendRecordsToKafka();
    Thread.sleep(10000);
  }

  private void addReadWritePolicyToBucket(String bucketName) {

    Statement allowRestrictedWriteStatement = new Statement(Statement.Effect.Allow)
      .withPrincipals(new Principal(System.getenv("SECONDARY_USER_ACCOUNT_ID")))
      .withActions(S3Actions.GetObject,S3Actions.PutObject)
      .withResources(new S3ObjectResource(bucketName, "*"));

    Policy policy = new Policy()
      .withStatements(allowRestrictedWriteStatement);

    s3RootClient.setBucketPolicy(bucketName, policy.toJson());
  }

  private void sendRecordsToKafka() {
    // Send records to Kafka
    for (int i = 0; i < NUM_RECORDS_PRODUCED; i++) {
      totalNoOfRecordsProduced++;
      String kafkaTopic = KAFKA_TOPICS.get(i % KAFKA_TOPICS.size());
      String kafkaKey = "simple-key-" + i;
      String kafkaValue = "simple-message-" + i;
      log.debug("Sending message {} with topic {} to Kafka broker {}", kafkaTopic, kafkaValue);
      connect.kafka().produce(kafkaTopic, kafkaKey, kafkaValue);
    }
  }

  private Map<String, String> getProperties() {
    Map<String, String> props = new HashMap<>();
    props.put(SinkConnectorConfig.TOPICS_CONFIG, String.join(",", KAFKA_TOPICS));
    props.put(CONNECTOR_CLASS_CONFIG, "io.confluent.connect.s3.S3SinkConnector");
    props.put(TASKS_MAX_CONFIG, Integer.toString(TASKS_MAX));

    props.put("s3.region", "ap-south-1");
    props.put("s3.part.size", "5242880");
    props.put("s3.bucket.name", S3_BUCKET);
    props.put("flush.size", Integer.toString(FLUSH_SIZE));
    props.put("storage.class","io.confluent.connect.s3.storage.S3Storage");
    props.put("partitioner.class", "io.confluent.connect.storage.partitioner.DefaultPartitioner");

    // converters
    props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());

    props.put("format.class",JSON_FORMAT_CLASS);
    // license properties
    return props;
  }

  private void deleteBucket(String bucketName) {
    emptyBucket(bucketName);
    // After all objects are deleted, delete the bucket.
    s3RootClient.deleteBucket(bucketName);
  }

  private void emptyBucket(String bucketName) {
    // delete all objects to empty bucket
    ObjectListing objectListing = s3RootClient.listObjects(bucketName);
    while (true) {
      Iterator<S3ObjectSummary> objIter = objectListing.getObjectSummaries().iterator();
      while (objIter.hasNext()) {
        s3RootClient.deleteObject(bucketName, objIter.next().getKey());
      }

      if (objectListing.isTruncated()) {
        objectListing = s3RootClient.listNextBatchOfObjects(objectListing);
      } else {
        break;
      }
    }
    // delete versioned objects
    VersionListing versionList = s3RootClient.listVersions(new ListVersionsRequest().withBucketName(bucketName));
    while (true) {
      Iterator<S3VersionSummary> versionIter = versionList.getVersionSummaries().iterator();
      while (versionIter.hasNext()) {
        S3VersionSummary vs = versionIter.next();
        s3RootClient.deleteVersion(bucketName, vs.getKey(), vs.getVersionId());
      }

      if (versionList.isTruncated()) {
        versionList = s3RootClient.listNextBatchOfVersions(versionList);
      } else {
        break;
      }
    }
  }

  private void createS3Bucket(String bucketName) {
    if (!s3RootClient.doesBucketExistV2(bucketName)) {
      s3RootClient.createBucket(new CreateBucketRequest(bucketName));
    }
  }

}
