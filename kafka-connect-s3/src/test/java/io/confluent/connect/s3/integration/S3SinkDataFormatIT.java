/*
 * Copyright 2020 Confluent Inc.
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

import static io.confluent.connect.s3.S3SinkConnectorConfig.S3_BUCKET_CONFIG;
import static io.confluent.connect.storage.StorageSinkConnectorConfig.FLUSH_SIZE_CONFIG;
import static io.confluent.connect.storage.StorageSinkConnectorConfig.FORMAT_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.confluent.connect.avro.AvroConverter;
import io.confluent.connect.json.JsonSchemaConverter;
import io.confluent.connect.protobuf.ProtobufConverter;
import io.confluent.connect.s3.S3SinkConnector;
import io.confluent.connect.s3.format.avro.AvroFormat;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.s3.util.EmbeddedConnectUtils;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

@RunWith(Parameterized.class)
@Category(IntegrationTest.class)
public class S3SinkDataFormatIT extends BaseConnectorIT {

  private static final Logger log = LoggerFactory.getLogger(S3SinkDataFormatIT.class);
  private static final String KEY_CONVERTER_SCHEMA_REGISTRY_URL = "key.converter.schema.registry.url";
  private static final String VALUE_CONVERTER_SCHEMA_REGISTRY_URL = "value.converter.schema.registry.url";
  private static final String KEY_CONVERTER_SCRUB_INVALID_NAMES = "key.converter.scrub.invalid.names";
  private static final String VALUE_CONVERTER_SCRUB_INVALID_NAMES = "value.converter.scrub.invalid.names";
  private static final String VALUE_CONVERTER_SCHEMAS_ENABLE = "value.converter.schemas.enable";

  public S3SinkDataFormatIT(Class<? extends Converter> converter) throws Exception {
    this.converterClass = converter;
    this.keyConverter = converterClass.getConstructor().newInstance();
    this.valueConverter = converterClass.getConstructor().newInstance();
    this.connectorName = "s3-sink-" + converter.getSimpleName();
    this.topicName = "TestTopic" + converter.getSimpleName();
  }

  private final Converter keyConverter;
  private final Converter valueConverter;
  private final Class<? extends Converter> converterClass;

  private final String connectorName;

  private final String topicName;

  @Parameters
  public static List<Class<? extends Converter>> data() {
    return Arrays.asList(JsonSchemaConverter.class, ProtobufConverter.class, AvroConverter.class);
  }

  @Before
  public void before() throws InterruptedException {
    keyConverter.configure(ImmutableMap.of(
            "schema.registry.url", restApp.restServer.getURI().toString()
        ), true
    );
    valueConverter.configure(ImmutableMap.of(
            "schema.registry.url", restApp.restServer.getURI().toString()
        ), false
    );
    setupProperties();
    //add class specific props
    props.put(SinkConnectorConfig.TOPICS_CONFIG, String.join(",", topicName));
    props.put(FLUSH_SIZE_CONFIG, Integer.toString(FLUSH_SIZE_STANDARD));
    props.put(FORMAT_CLASS_CONFIG, AvroFormat.class.getName());
    props.put(STORAGE_CLASS_CONFIG, S3Storage.class.getName());
    props.put(S3_BUCKET_CONFIG, TEST_BUCKET_NAME);

    props.put(KEY_CONVERTER_CLASS_CONFIG, converterClass.getSimpleName());
    props.put(KEY_CONVERTER_SCHEMA_REGISTRY_URL, restApp.restServer.getURI().toString());
    props.put(KEY_CONVERTER_SCRUB_INVALID_NAMES, "true");

    props.put(VALUE_CONVERTER_CLASS_CONFIG, converterClass.getSimpleName());
    props.put(VALUE_CONVERTER_SCHEMA_REGISTRY_URL, restApp.restServer.getURI().toString());
    props.put(VALUE_CONVERTER_SCRUB_INVALID_NAMES, "true");

    // create topics in Kafka
    connect.kafka().createTopic(topicName, 1);
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

  /**
   * For all SR backed types, write output in AVRO format.
   */
  @Test
  public void testBasicRecordsWrittenAvro() throws Throwable {
    //add test specific props
    props.put(FORMAT_CLASS_CONFIG, AvroFormat.class.getName());
    final String topicNameWithExt = "other." + AVRO_EXTENSION + ".topic." + AVRO_EXTENSION;

    // Add an extra topic with this extension inside of the name
    // Use a TreeSet for test determinism
    Set<String> topicNames = new TreeSet<>(Collections.singletonList(topicName));

    connect.kafka().createTopic(topicNameWithExt, 1);

    // start sink connector
    connect.configureConnector(connectorName, props);
    // wait for tasks to spin up
    EmbeddedConnectUtils.waitForConnectorToStart(connect, connectorName,
        Math.min(topicNames.size(), MAX_TASKS));

    Schema recordValueSchema = getSampleStructSchema();
    Struct recordValueStruct = getSampleStructVal(recordValueSchema);

    KafkaProducer<byte[], byte[]> producer = configureProducer();

    List<SinkRecord> recordList = new ArrayList<>();
    for (String thisTopicName : topicNames) {
      // Create and send records to Kafka using the topic name in the current 'thisTopicName'
      for(int i = 0; i < NUM_RECORDS_INSERT; i++) {
        recordList.add(getSampleTopicRecord(thisTopicName, recordValueSchema, recordValueStruct));
      }
    }
    produceRecords(producer, keyConverter, valueConverter, recordList, topicName);

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
          NUM_RECORDS_INSERT,
          AVRO_EXTENSION
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

  protected void produceRecords(
      KafkaProducer<byte[], byte[]> producer,
      Converter keyConverter,
      Converter valueConverter,
      List<SinkRecord> recordsList,
      String topic
  ) {
    for (int i = 0; i < recordsList.size(); i++) {
      byte[] convertedKey = keyConverter.fromConnectData(topic, Schema.STRING_SCHEMA,
          String.valueOf(i));
      SinkRecord sinkRecord = recordsList.get(i);
      byte[] convertedValue = valueConverter.fromConnectData(topic, sinkRecord.valueSchema(),
          sinkRecord.value());
      ProducerRecord<byte[], byte[]> msg = new ProducerRecord<>(topic, 0,
          convertedKey, convertedValue);
      try {
        producer.send(msg).get(TimeUnit.SECONDS.toMillis(120), TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        throw new KafkaException("Could not produce message: " + msg, e);
      }
    }
  }

  protected KafkaProducer<byte[], byte[]> configureProducer() {
    Map<String, Object> producerProps = new HashMap<>();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers());
    return new KafkaProducer<>(producerProps, new ByteArraySerializer(), new ByteArraySerializer());
  }

  private void setupProperties() {
    props = new HashMap<>();
    props.put(CONNECTOR_CLASS_CONFIG, S3SinkConnector.class.getName());
    props.put(TASKS_MAX_CONFIG, Integer.toString(MAX_TASKS));
    // aws credential if exists
    props.putAll(getAWSCredentialFromPath());
  }
}
