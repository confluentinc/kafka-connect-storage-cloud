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

import static io.confluent.connect.s3.S3SinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.ENABLE_CONDITIONAL_WRITES_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.KEYS_FORMAT_CLASS_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.S3_BUCKET_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.SEND_DIGEST_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.STORE_KAFKA_HEADERS_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.STORE_KAFKA_KEYS_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.TOMBSTONE_ENCODED_PARTITION;
import static io.confluent.connect.s3.util.HelperUtil.initializeCustomProducer;
import static io.confluent.connect.s3.util.HelperUtil.initializeJsonConverter;
import static io.confluent.connect.storage.StorageSinkConnectorConfig.FLUSH_SIZE_CONFIG;
import static io.confluent.connect.storage.StorageSinkConnectorConfig.FORMAT_CLASS_CONFIG;
import static io.confluent.connect.storage.StorageSinkConnectorConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.amazonaws.services.s3.model.ObjectMetadata;
import io.confluent.connect.s3.S3SinkConnector;
import io.confluent.connect.s3.S3SinkConnectorConfig.IgnoreOrFailBehavior;
import io.confluent.connect.s3.S3SinkConnectorConfig.OutputWriteBehavior;
import io.confluent.connect.s3.format.avro.AvroFormat;
import io.confluent.connect.s3.format.json.JsonFormat;
import io.confluent.connect.s3.format.parquet.ParquetFormat;
import io.confluent.connect.s3.storage.S3Storage;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import io.confluent.connect.s3.util.EmbeddedConnectUtils;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"unchecked", "deprecation"})
@Category(IntegrationTest.class)
public class S3SinkConnectorIT extends BaseConnectorIT {

  private static final Logger log = LoggerFactory.getLogger(S3SinkConnectorIT.class);
  // connector and test configs
  private static final String CONNECTOR_NAME = "s3-sink";
  private static final String DEFAULT_TEST_TOPIC_NAME = "TestTopic";
  // DLQ Tests
  private static final String DLQ_TOPIC_CONFIG = "errors.deadletterqueue.topic.name";
  private static final String DLQ_TOPIC_NAME = "DLQ-topic";

  private static final String TOMBSTONE_PARTITION = "TOMBSTONE_PARTITION";

  private static final List<String> KAFKA_TOPICS = Collections.singletonList(DEFAULT_TEST_TOPIC_NAME);
  private static final long CONSUME_MAX_DURATION_MS = TimeUnit.SECONDS.toMillis(10);

  private JsonConverter jsonConverter;
  // custom producer to enable sending records with headers
  private Producer<byte[], byte[]> producer;

  @Before
  public void before() throws InterruptedException {
    jsonConverter = initializeJsonConverter();
    producer = initializeCustomProducer(connect);
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
  public void testBasicRecordsWrittenParquet() throws Throwable {
    //add test specific props
    props.put(FORMAT_CLASS_CONFIG, ParquetFormat.class.getName());
    testBasicRecordsWritten(PARQUET_EXTENSION, false);
  }

  @Test
  public void testBasicRecordsWrittenJson() throws Throwable {
    //add test specific props
    props.put(FORMAT_CLASS_CONFIG, JsonFormat.class.getName());
    testBasicRecordsWritten(JSON_EXTENSION, false);
  }

  @Test
  public void testBasicRecordsWrittenWithDigestAvro() throws Throwable {
    //add test specific props
    props.put(FORMAT_CLASS_CONFIG, AvroFormat.class.getName());
    props.put(SEND_DIGEST_CONFIG, "true");
    testBasicRecordsWritten(AVRO_EXTENSION, false);
  }

  @Test
  public void testBasicRecordsWrittenWithDigestParquet() throws Throwable {
    //add test specific props
    props.put(FORMAT_CLASS_CONFIG, ParquetFormat.class.getName());
    props.put(SEND_DIGEST_CONFIG, "true");
    testBasicRecordsWritten(PARQUET_EXTENSION, false);
  }

  @Test
  public void testBasicRecordsWrittenWithDigestJson() throws Throwable {
    //add test specific props
    props.put(FORMAT_CLASS_CONFIG, JsonFormat.class.getName());
    props.put(SEND_DIGEST_CONFIG, "true");
    testBasicRecordsWritten(JSON_EXTENSION, false);
  }

  @Test
  public void testTombstoneRecordsWrittenJson() throws Throwable {
    //add test specific props
    props.put(FORMAT_CLASS_CONFIG, JsonFormat.class.getName());
    props.put(BEHAVIOR_ON_NULL_VALUES_CONFIG, OutputWriteBehavior.WRITE.toString());
    props.put(STORE_KAFKA_KEYS_CONFIG, "true");
    props.put(KEYS_FORMAT_CLASS_CONFIG, "io.confluent.connect.s3.format.json.JsonFormat");
    props.put(TOMBSTONE_ENCODED_PARTITION, TOMBSTONE_PARTITION);
    testTombstoneRecordsWritten(JSON_EXTENSION, false);
  }

  @Test
  public void testFilesWrittenToBucketAvroWithExtInTopic() throws Throwable {
    //add test specific props
    props.put(FORMAT_CLASS_CONFIG, AvroFormat.class.getName());
    testBasicRecordsWritten(AVRO_EXTENSION, true);
  }

  @Test
  public void testFilesWrittenToBucketParquetWithExtInTopic() throws Throwable {
    //add test specific props
    props.put(FORMAT_CLASS_CONFIG, ParquetFormat.class.getName());
    testBasicRecordsWritten(PARQUET_EXTENSION, true);
  }

  @Test
  public void testFilesWrittenToBucketJsonWithExtInTopic() throws Throwable {
    //add test specific props
    props.put(FORMAT_CLASS_CONFIG, JsonFormat.class.getName());
    testBasicRecordsWritten(JSON_EXTENSION, true);
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

  private void testTombstoneRecordsWritten(
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

    for (String thisTopicName : topicNames) {
      // Create and send records to Kafka using the topic name in the current 'thisTopicName'
      SinkRecord sampleRecord = getSampleTopicRecord(thisTopicName, null, null);
      produceRecordsWithHeadersNoValue(thisTopicName, NUM_RECORDS_INSERT, sampleRecord);
    }

    log.info("Waiting for files in S3...");
    int countPerTopic = NUM_RECORDS_INSERT / FLUSH_SIZE_STANDARD;
    int expectedTotalFileCount = countPerTopic * topicNames.size();
    waitForFilesInBucket(TEST_BUCKET_NAME, expectedTotalFileCount);

    Set<String> expectedTopicFilenames = new TreeSet<>();
    for (String thisTopicName : topicNames) {
      List<String> theseFiles = getExpectedTombstoneFilenames(
          thisTopicName,
          TOPIC_PARTITION,
          FLUSH_SIZE_STANDARD,
          NUM_RECORDS_INSERT,
          expectedFileExtension,
          TOMBSTONE_PARTITION
      );
      assertEquals(theseFiles.size(), countPerTopic);
      expectedTopicFilenames.addAll(theseFiles);
    }
    // This check will catch any duplications
    assertEquals(expectedTopicFilenames.size(), expectedTotalFileCount);
    assertFileNamesValid(TEST_BUCKET_NAME, new ArrayList<>(expectedTopicFilenames));
    assertTrue(keyfileContentsAsExpected(TEST_BUCKET_NAME, FLUSH_SIZE_STANDARD, "\"key\""));
  }

  @Test
  public void testFaultyRecordsReportedToDLQ() throws Throwable {
    props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    props.put(BEHAVIOR_ON_NULL_VALUES_CONFIG, IgnoreOrFailBehavior.IGNORE.toString());
    props.put(STORE_KAFKA_KEYS_CONFIG, "true");
    props.put(STORE_KAFKA_HEADERS_CONFIG, "true");
    props.put(DLQ_TOPIC_CONFIG, DLQ_TOPIC_NAME);
    props.put("errors.deadletterqueue.context.headers.enable", "true");
    props.put("errors.tolerance", "all");
    props.put("errors.deadletterqueue.topic.replication.factor", "1");

    // start sink connector
    connect.configureConnector(CONNECTOR_NAME, props);
    // wait for tasks to spin up
    EmbeddedConnectUtils.waitForConnectorToStart(connect, CONNECTOR_NAME, Math.min(KAFKA_TOPICS.size(), MAX_TASKS));

    Schema recordValueSchema = getSampleStructSchema();
    Struct recordValueStruct = getSampleStructVal(recordValueSchema);
    SinkRecord sampleRecord = getSampleRecord(recordValueSchema, recordValueStruct, DEFAULT_TEST_TOPIC_NAME);

    // Send first batch of valid records to Kafka
    produceRecordsWithHeaders(DEFAULT_TEST_TOPIC_NAME, NUM_RECORDS_INSERT, sampleRecord);

    // wait for values keys and headers from first batch
    int expectedFileCount = (NUM_RECORDS_INSERT / FLUSH_SIZE_STANDARD) * 3;
    waitForFilesInBucket(TEST_BUCKET_NAME, expectedFileCount);

    // send faulty records
    int numberOfFaultyRecords = 1;
    produceRecordsWithHeadersNoKey(DEFAULT_TEST_TOPIC_NAME, numberOfFaultyRecords, sampleRecord);
    produceRecordsWithHeadersNoValue(DEFAULT_TEST_TOPIC_NAME, numberOfFaultyRecords, sampleRecord);
    produceRecordsNoHeaders(numberOfFaultyRecords, sampleRecord);

    // Send second batch of valid records to Kafka
    produceRecordsWithHeaders(DEFAULT_TEST_TOPIC_NAME, NUM_RECORDS_INSERT, sampleRecord);

    log.info("Waiting for files in S3...");
    expectedFileCount = expectedFileCount * 2;
    waitForFilesInBucket(TEST_BUCKET_NAME, expectedFileCount);

    // verify records in DLQ topic by consuming from topic and checking header messages
    int expectedDLQRecordCount = 3;
    ConsumerRecords<byte[], byte[]> dlqRecords =
        connect.kafka().consume(expectedDLQRecordCount, CONSUME_MAX_DURATION_MS, DLQ_TOPIC_NAME);
    List<String> expectedErrors = Arrays.asList(
        "Key cannot be null for SinkRecord",
        "Skipping null value record",
        "Headers cannot be null for SinkRecord"
    );

    assertEquals(expectedDLQRecordCount, dlqRecords.count());
    assertDLQRecordMessages(expectedErrors, dlqRecords);
    assertTrue(fileContentsAsExpected(TEST_BUCKET_NAME, FLUSH_SIZE_STANDARD, recordValueStruct));
  }

  /**
   * Verify the error messages in the DLQ record headers.
   *
   * @param expectedMessages   the expected list of error messages
   * @param consumedDLQRecords the records consumed from the DLQ topic
   */
  private void assertDLQRecordMessages(
      List<String> expectedMessages,
      ConsumerRecords<byte[], byte[]> consumedDLQRecords
  ) {

    List<String> actualMessages = new ArrayList<>();
    for (ConsumerRecord<byte[], byte[]> dlqRecord : consumedDLQRecords.records(DLQ_TOPIC_NAME)) {
      Header r = dlqRecord.headers().headers("__connect.errors.exception.message").iterator().next();
      String headerErrorMessage = new StringDeserializer().deserialize(DLQ_TOPIC_NAME, r.value());
      actualMessages.add(headerErrorMessage);
    }
    Collections.sort(actualMessages);
    Collections.sort(expectedMessages);

    for (int i = 0; i < expectedMessages.size(); i++) {
      String actualMessage = actualMessages.get(i); // includes record after
      String expectedMessage = expectedMessages.get(i); // message only
      org.hamcrest.MatcherAssert.assertThat(actualMessage, startsWith(expectedMessage));
    }
  }

  private void testBasicRecordsWritten(String expectedFileExtension,
                                       boolean addExtensionInTopic) throws Throwable {
    testBasicRecordsWrittenToSink(expectedFileExtension, addExtensionInTopic, KAFKA_TOPICS,
        CONNECTOR_NAME, jsonConverter, producer, TEST_BUCKET_NAME);
  }

  private void produceRecordsNoHeaders(int recordCount, SinkRecord record)
      throws ExecutionException, InterruptedException {
    produceRecords(record.topic(), recordCount, record, true, true, false, jsonConverter, producer);
  }

  private void produceRecordsWithHeaders(String topic, int recordCount, SinkRecord record) throws Exception {
    produceRecords(topic, recordCount, record, true, true, true, jsonConverter, producer);
  }

  private void produceRecordsWithHeadersNoKey(String topic, int recordCount, SinkRecord record) throws Exception {
    produceRecords(topic, recordCount, record, false, true, true, jsonConverter, producer);
  }

  private void produceRecordsWithHeadersNoValue(String topic, int recordCount, SinkRecord record) throws Exception {
    produceRecords(topic, recordCount, record, true, false, true, jsonConverter, producer);
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
