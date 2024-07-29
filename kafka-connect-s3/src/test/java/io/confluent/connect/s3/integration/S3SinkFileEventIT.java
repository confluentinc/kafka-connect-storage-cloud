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

import static io.confluent.connect.s3.S3SinkConnectorConfig.*;
import static io.confluent.connect.storage.StorageSinkConnectorConfig.FLUSH_SIZE_CONFIG;
import static io.confluent.connect.storage.StorageSinkConnectorConfig.FORMAT_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.junit.Assert.assertEquals;

import io.confluent.connect.s3.S3SinkConnector;
import io.confluent.connect.s3.file.KafkaFileEventConfig;
import io.confluent.connect.s3.file.KafkaFileEventProvider;
import io.confluent.connect.s3.format.avro.AvroFormat;
import io.confluent.connect.s3.format.parquet.ParquetFormat;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.s3.util.EmbeddedConnectUtils;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
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
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"unchecked", "deprecation"})
@Category(IntegrationTest.class)
public class S3SinkFileEventIT extends BaseConnectorIT {

  private static final Logger log = LoggerFactory.getLogger(S3SinkFileEventIT.class);
  // connector and test configs
  private static final String CONNECTOR_NAME = "s3-sink";
  private static final String DEFAULT_TEST_TOPIC_NAME = "TestTopic";

  private static final List<String> KAFKA_TOPICS =
      Collections.singletonList(DEFAULT_TEST_TOPIC_NAME);

  private JsonConverter jsonConverter;
  // custom producer to enable sending records with headers
  private Producer<byte[], byte[]> producer;
  private Map<String, Object> autoCreate =
      new HashMap<String, Object>() {
        {
          put("auto.register.schemas", "true");
          put("auto.create.topics.enable", "true");
        }
      };
  ;

  @Before
  public void before() throws InterruptedException {
    initializeJsonConverter();
    initializeCustomProducer();
    setupProperties();
    waitForSchemaRegistryToStart();
    // add class specific props
    props.put(SinkConnectorConfig.TOPICS_CONFIG, String.join(",", KAFKA_TOPICS));
    props.put(FLUSH_SIZE_CONFIG, Integer.toString(FLUSH_SIZE_STANDARD));
    props.put(FORMAT_CLASS_CONFIG, AvroFormat.class.getName());
    props.put(STORAGE_CLASS_CONFIG, S3Storage.class.getName());
    props.put(S3_BUCKET_CONFIG, TEST_BUCKET_NAME);
    props.put(S3_PROXY_URL_CONFIG, minioContainer.getUrl());
    props.put(AWS_ACCESS_KEY_ID_CONFIG, MinioContainer.MINIO_USERNAME);
    props.put(AWS_SECRET_ACCESS_KEY_CONFIG, MinioContainer.MINIO_PASSWORD);
    // file event
    props.put(FILE_EVENT_ENABLE, "true");
    // TimeBasedPartitioner
    props.put(
        PartitionerConfig.PARTITIONER_CLASS_CONFIG,
        "io.confluent.connect.storage.partitioner.TimeBasedPartitioner");
    props.put(PartitionerConfig.PARTITION_DURATION_MS_CONFIG, "100");
    props.put(PartitionerConfig.PATH_FORMAT_CONFIG, "'event_date'=YYYY-MM-dd/'event_hour'=HH");
    props.put(PartitionerConfig.LOCALE_CONFIG, "FR_fr");
    props.put(PartitionerConfig.TIMEZONE_CONFIG, "UTC");
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
  public void testBasicRecordsWrittenParquetAndRelatedFileEvents() throws Throwable {
    // add test specific props
    props.put(FORMAT_CLASS_CONFIG, ParquetFormat.class.getName());
    String topicFileEvent = "TopicFileEvent";
    props.put(
        FILE_EVENT_CONFIG_JSON,
        new KafkaFileEventConfig(
                topicFileEvent,
                null,
                null,
                connect.kafka().bootstrapServers(),
                restApp.restServer.getURI().toString(),
                this.autoCreate)
            .toJson());
    connect.kafka().createTopic(topicFileEvent);
    testBasicRecordsWrittenAndRelatedFileEvents(PARQUET_EXTENSION, topicFileEvent);
  }

  @Test
  public void testFileEventPartition() {
    String bootstrapServers = connect.kafka().bootstrapServers();
    String fileEventTopic = "file_event_topic";
    connect.kafka().createTopic(fileEventTopic);
    KafkaFileEventConfig kafkaFileEventConfig =
        new KafkaFileEventConfig(
            fileEventTopic,
            null,
            null,
            bootstrapServers,
            restApp.restServer.getURI().toString(),
            this.autoCreate);
    KafkaFileEventProvider fileEvent =
        new KafkaFileEventProvider(kafkaFileEventConfig.toJson(), false);
    fileEvent.call(
        "baz-topic",
        "version/event/hour",
        "file1.avro",
        12,
        new DateTime(1234L),
        new DateTime(123L),
        34,
        new DateTime(1234L).withZone(DateTimeZone.UTC));
    fileEvent.call(
        "foo-topic",
        "version/event/hour",
        "fil2.avro",
        8,
        new DateTime(12345L),
        new DateTime(1234L),
        12,
        new DateTime(12345L));

    // fails if two records are not present in kafka within 1s
    connect.kafka().consume(2, 1000L, fileEventTopic);
  }
  /**
   * Test that the expected records are written for a given file extension Optionally, test that
   * topics which have "*.{expectedFileExtension}*" in them are processed and written.
   *
   * @param expectedFileExtension The file extension to test against
   * @param fileEventTopic The fileEvent topic name
   * @throws Throwable
   */
  private void testBasicRecordsWrittenAndRelatedFileEvents(
      String expectedFileExtension, String fileEventTopic) throws Throwable {
    // Add an extra topic with this extension inside of the name
    // Use a TreeSet for test determinism
    Set<String> topicNames = new TreeSet<>(KAFKA_TOPICS);

    // start sink connector
    connect.configureConnector(CONNECTOR_NAME, props);
    // wait for tasks to spin up
    EmbeddedConnectUtils.waitForConnectorToStart(
        connect, CONNECTOR_NAME, Math.min(topicNames.size(), MAX_TASKS));

    Schema recordValueSchema = getSampleStructSchema();
    Struct recordValueStruct = getSampleStructVal(recordValueSchema);

    for (String thisTopicName : topicNames) {
      // Create and send records to Kafka using the topic name in the current 'thisTopicName'
      SinkRecord sampleRecord =
          getSampleTopicRecord(thisTopicName, recordValueSchema, recordValueStruct);
      produceRecordsNoHeaders(NUM_RECORDS_INSERT, sampleRecord);
    }

    log.info("Waiting for files in S3...");
    int countPerTopic = NUM_RECORDS_INSERT / FLUSH_SIZE_STANDARD;
    int expectedTotalFileCount = countPerTopic * topicNames.size();
    waitForFilesInBucket(TEST_BUCKET_NAME, expectedTotalFileCount);

    Set<String> expectedTopicFilenames = new TreeSet<>();
    for (String thisTopicName : topicNames) {
      List<String> theseFiles =
          getExpectedFilenames(
              thisTopicName,
              TOPIC_PARTITION,
              FLUSH_SIZE_STANDARD,
              NUM_RECORDS_INSERT,
              expectedFileExtension);
      assertEquals(theseFiles.size(), countPerTopic);
      expectedTopicFilenames.addAll(theseFiles);
    }
    // This check will catch any duplications
    assertEquals(expectedTopicFilenames.size(), expectedTotalFileCount);
    // Check whether we get same number of records in fileEvent
    connect.kafka().consume(expectedTotalFileCount, 1000L, fileEventTopic);
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
      boolean withHeaders)
      throws ExecutionException, InterruptedException {
    byte[] kafkaKey = null;
    byte[] kafkaValue = null;
    Iterable<Header> headers = Collections.emptyList();
    if (withKey) {
      kafkaKey = jsonConverter.fromConnectData(topic, Schema.STRING_SCHEMA, record.key());
    }
    if (withValue) {
      kafkaValue =
          jsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
    }
    if (withHeaders) {
      headers = sampleHeaders();
    }
    ProducerRecord<byte[], byte[]> producerRecord =
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
    producerProps.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        org.apache.kafka.common.serialization.ByteArraySerializer.class.getName());
    producerProps.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
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
