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
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.CanonicalGrantee;
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
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.File;
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

  @ClassRule
  public static DockerComposeContainer nginx =
    new DockerComposeContainer(
      new File("src/test/nginx/docker-compose.yml"));

  /*@ClassRule
  public static DockerComposeContainer pumba =
    new DockerComposeContainer(
      new File("src/test/pumba/docker-compose.yml"));*/

  @Before
  public void setup() throws IOException {
    startConnect();
    createS3RootClient();
  }

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
    props.put("retry.backoff.ms", "3000");
    props.put("s3.retry.backoff.ms","3000");
    props.put("s3.part.retries", "1");
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
    Thread.sleep(300000);
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

  @Test
  public void testSink() throws Exception {
    createS3Bucket(S3_BUCKET);
    // create topics in Kafka
    KAFKA_TOPICS.forEach(topic -> connect.kafka().createTopic(topic, 1));
    // send records to kafka
    sendRecordsToKafka();

    Map<String, String> props = getProperties();
    props.put("store.url", "http://localhost:9091");
    props.put("aws.access.key.id", System.getenv("ROOT_USER_ACCESS_KEY_ID"));
    props.put("aws.secret.access.key", System.getenv("ROOT_USER_SECRET_ACCESS_KEY"));
    // start a sink connector
    connect.configureConnector(CONNECTOR_NAME, props);
    // wait for tasks to spin up
    int minimumNumTasks = Math.min(KAFKA_TOPICS.size(), TASKS_MAX);

    waitForConnectorToStart(CONNECTOR_NAME, minimumNumTasks);
    waitForConnectorToCompleteSendingRecords(totalNoOfRecordsProduced, FLUSH_SIZE, S3_BUCKET);

    // assert records
    assertEquals(getNoOfObjectsInS3(S3_BUCKET), totalNoOfRecordsProduced/FLUSH_SIZE);
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
    //props.put("s3.retry.backoff.ms","20000");
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

  private void createS3BucketWithNoReadWritePermission(String bucketName) {
    CreateBucketRequest req = new CreateBucketRequest(bucketName);
    AccessControlList controlList = new AccessControlList();
    controlList.revokeAllPermissions(new CanonicalGrantee(s3RootClient.getS3AccountOwner().getId()));
    req.setAccessControlList(controlList);

  }
}
