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

import com.amazonaws.services.s3.model.ObjectMetadata;
import io.confluent.connect.s3.S3SinkConnector;
import io.confluent.connect.s3.format.avro.AvroFormat;
import io.confluent.connect.s3.format.json.JsonFormat;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.s3.util.EmbeddedConnectUtils;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

import static io.confluent.connect.s3.S3SinkConnectorConfig.ENABLE_CONDITIONAL_WRITES_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.S3_BUCKET_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.STORE_KAFKA_HEADERS_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.STORE_KAFKA_KEYS_CONFIG;
import static io.confluent.connect.storage.StorageSinkConnectorConfig.FLUSH_SIZE_CONFIG;
import static io.confluent.connect.storage.StorageSinkConnectorConfig.FORMAT_CLASS_CONFIG;
import static io.confluent.connect.storage.StorageSinkConnectorConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings({"unchecked", "deprecation"})
@Category(IntegrationTest.class)
public class S3SinkConnectorNonDefaultRegionIT extends BaseConnectorIT {

  private static final Logger log = LoggerFactory.getLogger(S3SinkConnectorNonDefaultRegionIT.class);
  // connector and test configs
  private static final String CONNECTOR_NAME = "s3-sink";
  private static final String DEFAULT_TEST_TOPIC_NAME = "TestTopic";

  private static final List<String> KAFKA_TOPICS = Collections.singletonList(DEFAULT_TEST_TOPIC_NAME);
  private static final String NON_DEFAULT_AWS_REGION = "ap-southeast-1";

  private JsonConverter jsonConverter;
  // custom producer to enable sending records with headers
  private Producer<byte[], byte[]> producer;

  @BeforeClass
  public static void setupClient() {
    log.info("Starting ITs...");
    S3Client = getS3Client(NON_DEFAULT_AWS_REGION);
    if (S3Client.doesBucketExistV2(TEST_BUCKET_NAME)) {
      clearBucket(TEST_BUCKET_NAME);
    } else {
      S3Client.createBucket(TEST_BUCKET_NAME);
    }
  }

  @Before
  public void before() throws InterruptedException {
    initializeJsonConverter();
    initializeCustomProducer();
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
    // delete the downloaded test file folder
    FileUtils.deleteDirectory(new File(TEST_DOWNLOAD_PATH));
    // clear for next test
    clearBucket(TEST_BUCKET_NAME);
    // wait for bucket to clear
    waitForFilesInBucket(TEST_BUCKET_NAME, 0);
  }

  @Test
  public void testBasicRecordsWrittenAvro() throws Throwable {
    //add test specific props
    props.put(FORMAT_CLASS_CONFIG, AvroFormat.class.getName());
    testBasicRecordsWritten(AVRO_EXTENSION, false);
  }

  @Test
  public void testConnectorWithConditionalWrites() throws Throwable {
    props.put(ENABLE_CONDITIONAL_WRITES_CONFIG, "true");
    props.put(ROTATE_SCHEDULE_INTERVAL_MS_CONFIG, "60000");
    props.put(STORE_KAFKA_HEADERS_CONFIG, "false");
    props.put(STORE_KAFKA_KEYS_CONFIG, "false");
    props.put(PartitionerConfig.TIMEZONE_CONFIG, "UTC");
    props.put(PartitionerConfig.LOCALE_CONFIG, "en-GB");
    props.put(FORMAT_CLASS_CONFIG, JsonFormat.class.getName());

    testRecordsWrittenWithConditionalWrites(JSON_EXTENSION);
  }

  private void writeDummyFile(String key) {
    String initialFileContents = "{\"ID\":1,\"myBool\":true,\"myInt32\":32,\"myFloat32\":3.2,\"myFloat64\":64.64,\"myString\":\"theStringVal\"}\n"
        + "{\"ID\":1,\"myBool\":true,\"myInt32\":32,\"myFloat32\":3.2,\"myFloat64\":64.64,\"myString\":\"theStringVal\"}";
    S3Client.putObject(TEST_BUCKET_NAME, key, new ByteArrayInputStream(initialFileContents.getBytes()), new ObjectMetadata());
  }

  private void testRecordsWrittenWithConditionalWrites(String expectedFileExtension) throws InterruptedException, ExecutionException {

    // Create some initial file - presumed to be created by another task instance. The file contains the first two records
    String key = String.format("topics/%s/partition=0/%s+0+0000000000.json", DEFAULT_TEST_TOPIC_NAME, DEFAULT_TEST_TOPIC_NAME);
    writeDummyFile(key);

    // start sink connector
    connect.configureConnector(CONNECTOR_NAME, props);
    // wait for tasks to spin up
    EmbeddedConnectUtils.waitForConnectorToStart(connect, CONNECTOR_NAME, Math.min(KAFKA_TOPICS.size(), MAX_TASKS));

    Schema recordValueSchema = getSampleStructSchema();
    Struct recordValueStruct = getSampleStructVal(recordValueSchema);

    for (String topic : KAFKA_TOPICS) {
      // Create and send records to Kafka using the topic name in the current 'thisTopicName'
      SinkRecord sampleRecord = getSampleTopicRecord(topic, recordValueSchema, recordValueStruct);
      produceRecordsNoHeaders(NUM_RECORDS_INSERT, sampleRecord);
    }

    log.info("Waiting for files in S3...");
    int countPerTopic = NUM_RECORDS_INSERT / FLUSH_SIZE_STANDARD;
    // Expected 1 additional file in S3
    int expectedTotalFileCount = countPerTopic * KAFKA_TOPICS.size() + 1;
    waitForFilesInBucket(TEST_BUCKET_NAME, expectedTotalFileCount);

    Set<String> expectedTopicFilenames = new TreeSet<>();
    for (String topic : KAFKA_TOPICS) {
      List<String> expectedFilenames = getExpectedFilenames(
          topic,
          TOPIC_PARTITION,
          FLUSH_SIZE_STANDARD,
          1, // New files in S3 will start from offset 1, since file with offset 0 already exists in S3
          NUM_RECORDS_INSERT,
          expectedFileExtension
      );
      assertEquals(expectedFilenames.size(), countPerTopic);
      expectedTopicFilenames.addAll(expectedFilenames);
    }
    expectedTopicFilenames.add(key);

    assertEquals(expectedTopicFilenames.size(), expectedTotalFileCount);

    // The total number of files allowed in the bucket is number of topics * # records produced for each
    assertFileNamesValid(TEST_BUCKET_NAME, new ArrayList<>(expectedTopicFilenames));
    // verify number of records written to S3
    assertEquals(NUM_RECORDS_INSERT + 1, countNumberOfRecords(TEST_BUCKET_NAME)); // 1 duplicate record will be present in the seed file
  }

  /**
   * Test that the expected records are written for a given file extension
   * Optionally, test that topics which have "*.{expectedFileExtension}*" in them are processed
   * and written.
   * @param expectedFileExtension The file extension to test against
   * @param addExtensionInTopic Add a topic to to the test which contains the extension
   * @throws Throwable
   */
  private void testBasicRecordsWritten(
          String expectedFileExtension,
          boolean addExtensionInTopic
  ) throws Throwable {
    final String topicNameWithExt = "other." + expectedFileExtension + ".topic." + expectedFileExtension;

    // Add an extra topic with this extension inside of the name
    // Use a TreeSet for test determinism
    Set<String> topicNames = new TreeSet<>(KAFKA_TOPICS);

    if (addExtensionInTopic) {
      topicNames.add(topicNameWithExt);
      connect.kafka().createTopic(topicNameWithExt, 1);
      props.replace(
              "topics",
              props.get("topics") + "," + topicNameWithExt
      );
    }

    // start sink connector
    connect.configureConnector(CONNECTOR_NAME, props);
    // wait for tasks to spin up
    EmbeddedConnectUtils.waitForConnectorToStart(connect, CONNECTOR_NAME, Math.min(topicNames.size(), MAX_TASKS));

    Schema recordValueSchema = getSampleStructSchema();
    Struct recordValueStruct = getSampleStructVal(recordValueSchema);

    for (String thisTopicName : topicNames) {
      // Create and send records to Kafka using the topic name in the current 'thisTopicName'
      SinkRecord sampleRecord = getSampleTopicRecord(thisTopicName, recordValueSchema, recordValueStruct);
      produceRecordsNoHeaders(NUM_RECORDS_INSERT, sampleRecord);
    }

    log.info("Waiting for files in S3...");
    int countPerTopic = NUM_RECORDS_INSERT / FLUSH_SIZE_STANDARD;
    int expectedTotalFileCount = countPerTopic * topicNames.size();
    waitForFilesInBucket(TEST_BUCKET_NAME, expectedTotalFileCount);

    Set<String> expectedTopicFilenames = new TreeSet<>();
    for (String thisTopicName : topicNames) {
      List<String> theseFiles = getExpectedFilenames(
              thisTopicName,
              TOPIC_PARTITION,
              FLUSH_SIZE_STANDARD,
              0,
              NUM_RECORDS_INSERT,
              expectedFileExtension
      );
      assertEquals(theseFiles.size(), countPerTopic);
      expectedTopicFilenames.addAll(theseFiles);
    }
    // This check will catch any duplications
    assertEquals(expectedTopicFilenames.size(), expectedTotalFileCount);
    // The total number of files allowed in the bucket is number of topics * # produced for each
    // All topics should have produced the same number of files, so this check should hold.
    assertFileNamesValid(TEST_BUCKET_NAME, new ArrayList<>(expectedTopicFilenames));
    // Now check that all files created by the sink have the contents that were sent
    // to the producer (they're all the same content)
    assertTrue(fileContentsAsExpected(TEST_BUCKET_NAME, FLUSH_SIZE_STANDARD, recordValueStruct));
  }

  private void produceRecordsNoHeaders(int recordCount, SinkRecord record)
      throws ExecutionException, InterruptedException {
    produceRecords(record.topic(), recordCount, record, true, true, false);
  }

  private void produceRecords(
      String topic,
      int recordCount,
      SinkRecord record,
      boolean withKey,
      boolean withValue,
      boolean withHeaders
  ) throws ExecutionException, InterruptedException {
    byte[] kafkaKey = null;
    byte[] kafkaValue = null;
    Iterable<Header> headers = Collections.emptyList();
    if (withKey) {
      kafkaKey = jsonConverter.fromConnectData(topic, Schema.STRING_SCHEMA, record.key());
    }
    if (withValue) {
     kafkaValue = jsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
    }
    if (withHeaders) {
      headers = sampleHeaders();
    }
    ProducerRecord<byte[],byte[]> producerRecord =
        new ProducerRecord<>(topic, TOPIC_PARTITION, kafkaKey, kafkaValue, headers);
    for (long i = 0; i < recordCount; i++) {
      producer.send(producerRecord).get();
    }
  }

  private void initializeJsonConverter() {
    Map<String, Object> jsonConverterProps = new HashMap<>();
    jsonConverterProps.put("schemas.enable", "true");
    jsonConverterProps.put("converter.type", "value");
    jsonConverter = new JsonConverter();
    jsonConverter.configure(jsonConverterProps);
  }

  private void initializeCustomProducer() {
    Map<String, Object> producerProps = new HashMap<>();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers());
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        org.apache.kafka.common.serialization.ByteArraySerializer.class.getName());
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        org.apache.kafka.common.serialization.ByteArraySerializer.class.getName());
    producer = new KafkaProducer<>(producerProps);
  }

  private void setupProperties() {
    props = new HashMap<>();
    props.put(CONNECTOR_CLASS_CONFIG, S3SinkConnector.class.getName());
    props.put(TASKS_MAX_CONFIG, Integer.toString(MAX_TASKS));
    // converters
    props.put(KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    props.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    // aws credential if exists
    props.putAll(getAWSCredentialFromPath());
  }
}
