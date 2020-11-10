/*
 * Copyright [2020 - 2020] Confluent Inc.
 */

package io.confluent.connect.s3.integration;

import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.actions.S3Actions;
import com.amazonaws.auth.policy.resources.S3ObjectResource;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.VersionListing;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.s3.S3SinkConnector;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.format.json.JsonFormat;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.testcontainers.squid.SquidProxy;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.storage.StringConverter;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static io.confluent.connect.s3.S3SinkConnectorConfig.PART_SIZE_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.REGION_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.S3_BUCKET_CONFIG;
import static io.confluent.connect.storage.StorageSinkConnectorConfig.FLUSH_SIZE_CONFIG;
import static io.confluent.connect.storage.StorageSinkConnectorConfig.FORMAT_CLASS_CONFIG;
import static io.confluent.connect.storage.partitioner.PartitionerConfig.PARTITIONER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.*;
import static org.junit.Assert.assertTrue;

/**
 * Integration test for S3 Sink Connector.<p>
 * Refer file /bin/README.md to find prerequisites
 * to run the following tests.
 */
@Category(IntegrationTest.class)
public class S3SinkConnectorNetworkIT extends BaseConnectorNetworkIT {

  private static final Logger log = LoggerFactory.getLogger(S3SinkConnectorIT.class);

  private static final String CONNECTOR_NAME = "s3-sink-connector";
  private static final String STORAGE_CLASS_CONFIG = "storage.class";
  private static final int NUM_RECORDS_PRODUCED = 1000;
  private static final int TASKS_MAX = 1;
  private static final String TEST_TOPIC_NAME = "topic1";
  private static final String EXPECTED_FILE_EXTENSION = "json";
  private static final List<String> KAFKA_TOPICS = Arrays.asList(TEST_TOPIC_NAME);
  private static final int FLUSH_SIZE = 200;
  private static final int EXPECTED_PARTITION = 0;
  private static SquidProxy squid;

  @Before
  public void initializeResources() {
    createS3RootClient();
    // create the test bucket
    createS3Bucket(TEST_BUCKET_NAME);
    setupConnectorProps();
  }

  @After
  public void after(){
    emptyBucket(TEST_BUCKET_NAME);
  }

  @AfterClass
  public static void deleteBucket() {
    // delete the test bucket
    deleteBucket(TEST_BUCKET_NAME);
  }

  /**
   * Success scenario : End to end test
   * @throws InterruptedException
   */
  @Test
  @Ignore
  public void testToAssertConnectorAndDestinationRecords() throws InterruptedException {

    // create topics in Kafka
    KAFKA_TOPICS.forEach(topic -> connect.kafka().createTopic(topic, 1));
    // send records to kafka
    sendRecordsToKafka(NUM_RECORDS_PRODUCED);

    // start a sink connector
    connect.configureConnector(CONNECTOR_NAME, props);
    // wait for tasks to spin up
    int minimumNumTasks = Math.min(KAFKA_TOPICS.size(), TASKS_MAX);

    waitForConnectorToStart(CONNECTOR_NAME, minimumNumTasks);
    int expectedFileCount = NUM_RECORDS_PRODUCED / FLUSH_SIZE;
    waitForFilesInBucket(TEST_BUCKET_NAME, expectedFileCount);

    // assert files in bucket
    List<String> expectedFilenames = getExpectedFilenames(TEST_TOPIC_NAME, EXPECTED_PARTITION,
        FLUSH_SIZE, NUM_RECORDS_PRODUCED, EXPECTED_FILE_EXTENSION);
    assertTrue(fileNamesValid(TEST_BUCKET_NAME, expectedFilenames));
  }

  /**
   * Case in which bucket permissions/policies are changed while uploading records
   * Prerequisite : Access key and Secret access key should be set as environment variables
   * @throws InterruptedException
   */
  @Test
  @Ignore
  public void testWithRevokedWritePermissions() throws InterruptedException {

    addReadWritePolicyToBucket(TEST_BUCKET_NAME);
    KAFKA_TOPICS.forEach(topic -> connect.kafka().createTopic(topic, 1));

    // send records to kafka
    sendRecordsToKafka(NUM_RECORDS_PRODUCED);

    props.put("aws.access.key.id", System.getenv("SECONDARY_USER_ACCESS_KEY_ID"));
    props.put("aws.secret.access.key", System.getenv("SECONDARY_USER_SECRET_ACCESS_KEY"));

    // start a sink connector
    connect.configureConnector(CONNECTOR_NAME, props);
    // wait for tasks to spin up
    int minimumNumTasks = Math.min(KAFKA_TOPICS.size(), TASKS_MAX);

    waitForConnectorToStart(CONNECTOR_NAME, minimumNumTasks);
    int expectedFileCount = NUM_RECORDS_PRODUCED / FLUSH_SIZE;
    waitForFilesInBucket(TEST_BUCKET_NAME, expectedFileCount);
    // assert files in bucket
    List<String> expectedFilenames = getExpectedFilenames(TEST_TOPIC_NAME, EXPECTED_PARTITION,
        FLUSH_SIZE, NUM_RECORDS_PRODUCED, EXPECTED_FILE_EXTENSION);
    assertTrue(fileNamesValid(TEST_BUCKET_NAME, expectedFilenames));

    // revoke read/write permission
    alterReadWritePolicyOfBucket(TEST_BUCKET_NAME);
    /*
     Intentional sleep added in order for the bucket permission to be altered to read only to
     come into affect.
    */
    Thread.sleep(10000);
    // produce more records to kafka
    sendRecordsToKafka(NUM_RECORDS_PRODUCED);

    // Intentional sleep added to assure no additional files are created.
    Thread.sleep(10000);
    // assert files in bucket remains same as earlier since the Write policy has been revoked.
    assertTrue(fileNamesValid(TEST_BUCKET_NAME, expectedFilenames));
  }

  @Test
  @Ignore
  public void testWithNetworkUnavailability() throws Throwable {
    // Setup Squid Proxy Container
    setupSquidProxy();
    // create topics in Kafka
    KAFKA_TOPICS.forEach(topic -> connect.kafka().createTopic(topic, 1));
    // send records to kafka
    sendRecordsToKafka(NUM_RECORDS_PRODUCED);

    props.put(S3SinkConnectorConfig.S3_PROXY_URL_CONFIG, "https://"
        + squid.getContainerIpAddress() + ":" + squid.getMappedPort(3129));

    // start a sink connector
    connect.configureConnector(CONNECTOR_NAME, props);
    // wait for tasks to spin up
    int minimumNumTasks = Math.min(KAFKA_TOPICS.size(), TASKS_MAX);

    waitForConnectorToStart(CONNECTOR_NAME, minimumNumTasks);
    int expectedFileCount = NUM_RECORDS_PRODUCED / FLUSH_SIZE;
    waitForFilesInBucket(TEST_BUCKET_NAME, expectedFileCount);

    // assert files in bucket
    List<String> expectedFilenames = getExpectedFilenames(TEST_TOPIC_NAME, EXPECTED_PARTITION,
        FLUSH_SIZE, NUM_RECORDS_PRODUCED, EXPECTED_FILE_EXTENSION);
    assertTrue(fileNamesValid(TEST_BUCKET_NAME, expectedFilenames));

    // Shutting down proxy to emulate network unavailability
    shutdownSquidProxy();

    sendRecordsToKafka(NUM_RECORDS_PRODUCED);

    // Intentional sleep added to assure no additional files are created.
    Thread.sleep(10000);
    // assert files in bucket remains same as earlier since the connection is unavailable.
    assertTrue(fileNamesValid(TEST_BUCKET_NAME, expectedFilenames));
  }

  @Test
  @Ignore
  public void testWithNetworkInterruption() throws Throwable {
    /*
     A small value is used to create enough request that the pumba container can cause network
     interruptions
    */
    int flushSize = 3;

    setupSquidProxy();
    startPumbaPauseContainer();
    // create topics in Kafka
    KAFKA_TOPICS.forEach(topic -> connect.kafka().createTopic(topic, 1));
    // send records to kafka
    sendRecordsToKafka(NUM_RECORDS_PRODUCED);

    props.put(FLUSH_SIZE_CONFIG, Integer.toString(flushSize));

    // start a sink connector
    connect.configureConnector(CONNECTOR_NAME, props);
    // wait for tasks to spin up
    int minimumNumTasks = Math.min(KAFKA_TOPICS.size(), TASKS_MAX);

    waitForConnectorToStart(CONNECTOR_NAME, minimumNumTasks);
    int expectedFileCount = NUM_RECORDS_PRODUCED / flushSize;
    waitForFilesInBucket(TEST_BUCKET_NAME, expectedFileCount);
    pumbaPauseContainer.close();
    // assert files in bucket
    List<String> expectedFilenames = getExpectedFilenames(TEST_TOPIC_NAME, EXPECTED_PARTITION,
        flushSize, NUM_RECORDS_PRODUCED, EXPECTED_FILE_EXTENSION);
    assertTrue(fileNamesValid(TEST_BUCKET_NAME, expectedFilenames));
    shutdownSquidProxy();
  }

  private void addReadWritePolicyToBucket(String bucketName) {
    Statement allowRestrictedWriteStatement = new Statement(Statement.Effect.Allow)
        .withPrincipals(new Principal(System.getenv("SECONDARY_USER_ACCOUNT_ID")))
        .withActions(S3Actions.GetObject, S3Actions.PutObject)
        .withResources(new S3ObjectResource(bucketName, "*"));
    Policy policy = new Policy().withStatements(allowRestrictedWriteStatement);
    S3Client.setBucketPolicy(bucketName, policy.toJson());
  }

  private void alterReadWritePolicyOfBucket(String bucketName) {
    Statement allowRestrictedWriteStatement = new Statement(Statement.Effect.Allow)
        .withPrincipals(new Principal(System.getenv("SECONDARY_USER_ACCOUNT_ID")))
        .withActions(S3Actions.GetObject)
        .withResources(new S3ObjectResource(bucketName, "*"));
    Policy policy = new Policy().withStatements(allowRestrictedWriteStatement);
    S3Client.setBucketPolicy(bucketName, policy.toJson());
  }

  private void sendRecordsToKafka(int numRecords) {
    // Send records to Kafka
    for (int i = 0; i < numRecords; i++) {
      String kafkaTopic = KAFKA_TOPICS.get(i % KAFKA_TOPICS.size());
      String kafkaKey = "simple-key-" + i;
      String kafkaValue = "simple-message-" + i;
      log.debug("Sending message {} with topic {} to Kafka broker {}", kafkaTopic, kafkaValue);
      connect.kafka().produce(kafkaTopic, kafkaKey, kafkaValue);
    }
  }

  private void setupConnectorProps() {
    props = new HashMap<>();
    props.put(SinkConnectorConfig.TOPICS_CONFIG, String.join(",", KAFKA_TOPICS));
    props.put(CONNECTOR_CLASS_CONFIG, S3SinkConnector.class.getName());
    props.put(TASKS_MAX_CONFIG, Integer.toString(TASKS_MAX));

    props.put(REGION_CONFIG, "ap-south-1");
    props.put(PART_SIZE_CONFIG, "5242880");
    props.put(S3_BUCKET_CONFIG, TEST_BUCKET_NAME);
    props.put(FLUSH_SIZE_CONFIG , Integer.toString(FLUSH_SIZE));
    props.put(STORAGE_CLASS_CONFIG, S3Storage.class.getName());
    props.put(PARTITIONER_CLASS_CONFIG, DefaultPartitioner.class.getName());

    // converters
    props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());

    props.put(FORMAT_CLASS_CONFIG, JsonFormat.class.getName());
  }

  private static void deleteBucket(String bucketName) {
    emptyBucket(bucketName);
    // After all objects are deleted, delete the bucket.
    S3Client.deleteBucket(bucketName);
  }

  private static void emptyBucket(String bucketName) {
    // delete all objects to empty bucket
    ObjectListing objectListing = S3Client.listObjects(bucketName);
    while (true) {
      for (S3ObjectSummary s3ObjectSummary : objectListing.getObjectSummaries()) {
        S3Client.deleteObject(bucketName, s3ObjectSummary.getKey());
      }

      if (objectListing.isTruncated()) {
        objectListing = S3Client.listNextBatchOfObjects(objectListing);
      } else {
        break;
      }
    }
    // delete versioned objects
    VersionListing versionList = S3Client.listVersions(new ListVersionsRequest().withBucketName(bucketName));
    while (true) {
      for (S3VersionSummary vs : versionList.getVersionSummaries()) {
        S3Client.deleteVersion(bucketName, vs.getKey(), vs.getVersionId());
      }

      if (versionList.isTruncated()) {
        versionList = S3Client.listNextBatchOfVersions(versionList);
      } else {
        break;
      }
    }
  }

  private void createS3Bucket(String bucketName) {
    if (S3Client.doesBucketExistV2(bucketName)) {
      emptyBucket(bucketName);
    } else {
      S3Client.createBucket(new CreateBucketRequest(bucketName));
    }
  }

  private static void setupSquidProxy() {
    squid = new SquidProxy("confluent-docker-internal.jfrog.io/confluentinc/connect-squid:1.0.0", "NONE");
    squid.start();
  }

  private static void shutdownSquidProxy() {
    if (squid != null) {
      squid.stop();
    }
  }

}
