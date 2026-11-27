package io.confluent.connect.s3.integration;

import io.confluent.connect.s3.S3SinkConnector;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.auth.AwsAssumeRoleCredentialsProvider;
import io.confluent.connect.s3.format.avro.AvroFormat;
import io.confluent.connect.s3.format.json.JsonFormat;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.s3.util.HelperUtil;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import io.confluent.connect.s3.util.S3FileUtils;

import static io.confluent.connect.s3.S3SinkConnectorConfig.AWS_ACCESS_KEY_ID_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.AWS_SECRET_ACCESS_KEY_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.CREDENTIALS_PROVIDER_CLASS_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.CREDENTIALS_PROVIDER_CONFIG_PREFIX;
import static io.confluent.connect.s3.S3SinkConnectorConfig.S3_BUCKET_CONFIG;
import static io.confluent.connect.s3.auth.AwsAssumeRoleCredentialsProvider.REGION_CONFIG;
import static io.confluent.connect.s3.auth.AwsAssumeRoleCredentialsProvider.ROLE_ARN_CONFIG;
import static io.confluent.connect.s3.auth.AwsAssumeRoleCredentialsProvider.ROLE_EXTERNAL_ID_CONFIG;
import static io.confluent.connect.s3.auth.AwsAssumeRoleCredentialsProvider.ROLE_SESSION_NAME_CONFIG;
import static io.confluent.connect.storage.StorageSinkConnectorConfig.FLUSH_SIZE_CONFIG;
import static io.confluent.connect.storage.StorageSinkConnectorConfig.FORMAT_CLASS_CONFIG;
import static io.confluent.connect.storage.common.StorageCommonConfig.STORE_URL_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;

@Category(IntegrationTest.class)
public class S3SinkConnectorStsAssumeRoleIT extends BaseConnectorIT {
  private static final Logger log = LoggerFactory.getLogger(S3SinkConnectorStsAssumeRoleIT.class);

  private static final String ROLE_EXTERNAL_ID = System.getenv("AWS_STS_ROLE_EXTERNAL_ID");
  private static final String ROLE_ARN = System.getenv("AWS_STS_ROLE_ARN");
  private static final String SESSION_NAME = "session-name";
  private static final String CONNECTOR_NAME = "s3-sink";

  private static final String TEST_REGION = Region.US_EAST_1.id();
  private static final String TEST_BUCKET_NAME =
      "connect-s3-integration-testing-assume-role-" + System.currentTimeMillis();
  private static final String DEFAULT_TEST_TOPIC_NAME = "TestTopic";
  private static final List<String> KAFKA_TOPICS = Collections.singletonList(DEFAULT_TEST_TOPIC_NAME);

  private JsonConverter jsonConverter;
  private Producer<byte[], byte[]> producer;

  @BeforeClass
  public static void setupClient() {
    log.info("Starting ITs...");
    s3Client = getS3Client();
    S3FileUtils fileUtils = new S3FileUtils(s3Client);
    if (fileUtils.bucketExists(TEST_BUCKET_NAME)) {
      clearBucket(TEST_BUCKET_NAME);
    } else {
      s3Client.createBucket(CreateBucketRequest.builder().bucket(TEST_BUCKET_NAME).build());
    }
  }

  @AfterClass
  public static void deleteBucket() {
    s3Client.deleteBucket(DeleteBucketRequest.builder().bucket(TEST_BUCKET_NAME).build());
    log.info("Finished ITs, removed S3 bucket");
  }

  @Before
  public void before() throws InterruptedException {
    jsonConverter = HelperUtil.initializeJsonConverter();
    producer = HelperUtil.initializeCustomProducer(connect);
    setupProperties();
    waitForSchemaRegistryToStart();
    //add class specific props
    props.put(SinkConnectorConfig.TOPICS_CONFIG, String.join(",", KAFKA_TOPICS));
    props.put(FLUSH_SIZE_CONFIG, Integer.toString(FLUSH_SIZE_STANDARD));
    props.put(FORMAT_CLASS_CONFIG, AvroFormat.class.getName());
    props.put(STORAGE_CLASS_CONFIG, S3Storage.class.getName());
    props.put(S3_BUCKET_CONFIG, TEST_BUCKET_NAME);
    // create topics in Kafka
    KAFKA_TOPICS.forEach(topic -> connect.kafka().createTopic(topic, 1));
  }

  @After
  public void after() throws Exception {
    FileUtils.deleteDirectory(new File(TEST_DOWNLOAD_PATH));
    clearBucket(TEST_BUCKET_NAME);
    waitForFilesInBucket(TEST_BUCKET_NAME, 0);
  }

  @Test
  public void testBasicRecordsWritten() throws Throwable {
    props.put(FORMAT_CLASS_CONFIG, JsonFormat.class.getName());
    testBasicRecordsWrittenToSink(JSON_EXTENSION, false, KAFKA_TOPICS,
        CONNECTOR_NAME, jsonConverter, producer, TEST_BUCKET_NAME, false);
  }

  protected static S3Client getS3Client() {
    Map<String, String> awsCredentials = getAWSAssumeRoleCredentials();
    String accessKeyId = awsCredentials.get(AWS_ACCESS_KEY_ID_CONFIG);
    String secretAccessKey = awsCredentials.get(AWS_SECRET_ACCESS_KEY_CONFIG);

    AwsBasicCredentials basicCredentials = AwsBasicCredentials.create(accessKeyId, secretAccessKey);
    StsClientBuilder clientBuilder = StsClient.builder()
        .credentialsProvider(StaticCredentialsProvider.create(basicCredentials))
        .region(Region.of(TEST_REGION));

    StsAssumeRoleCredentialsProvider stsCredentialsProvider =
        StsAssumeRoleCredentialsProvider.builder()
        .stsClient(clientBuilder.build())
        .refreshRequest(
            AssumeRoleRequest.builder()
                .roleArn(ROLE_ARN)
                .roleSessionName(SESSION_NAME)
                .externalId(ROLE_EXTERNAL_ID)
                .build())
        .build();

    return S3Client.builder()
        .region(Region.of(TEST_REGION))
        .credentialsProvider(stsCredentialsProvider)
        .build();
  }

  protected static Map<String, String> getAWSAssumeRoleCredentials() {
    return new HashMap<String, String>() {{
      put(STORE_URL_CONFIG, "https://s3." + TEST_REGION + ".amazonaws.com");
      put(CREDENTIALS_PROVIDER_CLASS_CONFIG, AwsAssumeRoleCredentialsProvider.class.getName());
      put(CREDENTIALS_PROVIDER_CONFIG_PREFIX.concat(ROLE_EXTERNAL_ID_CONFIG), ROLE_EXTERNAL_ID);
      put(CREDENTIALS_PROVIDER_CONFIG_PREFIX.concat(ROLE_ARN_CONFIG), ROLE_ARN);
      put(CREDENTIALS_PROVIDER_CONFIG_PREFIX.concat(ROLE_SESSION_NAME_CONFIG), SESSION_NAME);
      put(CREDENTIALS_PROVIDER_CONFIG_PREFIX.concat(REGION_CONFIG), TEST_REGION);
      put(S3SinkConnectorConfig.REGION_CONFIG, TEST_REGION);
      putAll(getAWSCredentialFromPath());
    }};
  }

  private void setupProperties() {
    props = new HashMap<>();
    props.put(CONNECTOR_CLASS_CONFIG, S3SinkConnector.class.getName());
    props.put(TASKS_MAX_CONFIG, Integer.toString(MAX_TASKS));
    props.put(KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    props.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    props.putAll(getAWSAssumeRoleCredentials());
  }
}
