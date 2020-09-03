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

import static io.confluent.connect.s3.S3SinkConnectorConfig.S3_BUCKET_CONFIG;
import static io.confluent.connect.storage.StorageSinkConnectorConfig.FLUSH_SIZE_CONFIG;
import static io.confluent.connect.storage.StorageSinkConnectorConfig.FORMAT_CLASS_CONFIG;
import static io.confluent.connect.storage.common.StorageCommonConfig.STORE_URL_CONFIG;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertTrue;

import io.confluent.connect.formatter.json.JsonFormatter;
import io.confluent.connect.formatter.json.JsonFormatterProvider;

import io.confluent.connect.s3.format.avro.AvroFormat;
import io.confluent.connect.s3.format.json.JsonFormat;
import io.confluent.connect.s3.format.parquet.ParquetFormat;
import io.confluent.connect.s3.storage.S3Storage;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.test.IntegrationTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(IntegrationTest.class)
public class S3SinkConnectorIT extends BaseConnectorIT {

  private static final Logger log = LoggerFactory.getLogger(S3SinkConnectorIT.class);
  private static final String TEST_TOPIC_NAME = "TestTopic";
  private static final String STORAGE_CLASS_CONFIG = "storage.class";
  private static final String AVRO_EXTENSION = "avro";
  private static final String PARQUET_EXTENSION = "parquet";
  private static final String JSON_EXTENSION = "json";
  private static final List<String> KAFKA_TOPICS = Collections.singletonList(TEST_TOPIC_NAME);
  private static final long NUM_RECORDS_INSERT = 20;
  private static final int FLUSH_SIZE_STANDARD = 3;
  private static final int EXPECTED_PARTITION = 0;

  private JsonFormatter formatter;

  @Before
  public void before() {
    JsonFormatterProvider provider = new JsonFormatterProvider();
    formatter = (JsonFormatter) provider.create(Collections.singletonMap("schemas.enable", true));
    //add class specific props
    props.put(SinkConnectorConfig.TOPICS_CONFIG, String.join(",", KAFKA_TOPICS));
    props.put(FLUSH_SIZE_CONFIG, Integer.toString(FLUSH_SIZE_STANDARD));
    props.put(FORMAT_CLASS_CONFIG, AvroFormat.class.getName());
    props.put(STORAGE_CLASS_CONFIG, S3Storage.class.getName());
    props.put(S3_BUCKET_CONFIG, TEST_BUCKET_NAME);
    if (useMockClient()) {
      props.put(STORE_URL_CONFIG, MOCK_S3_URL);
    }
    // create topics in Kafka
    KAFKA_TOPICS.forEach(topic -> connect.kafka().createTopic(topic, 1));
  }

  @Test
  public void testBasicFilesWrittenToBucketAvro() throws Throwable {
    //add test specific props
    props.put(FORMAT_CLASS_CONFIG, AvroFormat.class.getName());
    testBasicRecordsWritten(AVRO_EXTENSION);
  }

  @Test
  public void testBasicFilesWrittenToBucketParquet() throws Throwable {
    //add test specific props
    props.put(FORMAT_CLASS_CONFIG, ParquetFormat.class.getName());
    testBasicRecordsWritten(PARQUET_EXTENSION);
  }

  @Test
  public void testBasicFilesWrittenToBucketJson() throws Throwable {
    //add test specific props
    props.put(FORMAT_CLASS_CONFIG, JsonFormat.class.getName());
    testBasicRecordsWritten(JSON_EXTENSION);
  }

  private void testBasicRecordsWritten(String expectedFileExtension) throws Throwable {
    // start sink connector
    connect.configureConnector(CONNECTOR_NAME, props);
    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, Math.min(KAFKA_TOPICS.size(), MAX_TASKS));

    Schema recordValueSchema = getSampleStructSchema();
    Struct recordValueStruct = getSampleStructVal(recordValueSchema);
    // Send records to Kafka
    for (long i = 0; i < NUM_RECORDS_INSERT; i++) {
      SinkRecord record = new SinkRecord(TEST_TOPIC_NAME, 1, Schema.STRING_SCHEMA, null,
          recordValueSchema, recordValueStruct, i);
      String kafkaValue = new String(formatter.formatValue(record), UTF_8);
      connect.kafka().produce(TEST_TOPIC_NAME, null, kafkaValue);
    }

    log.info("Waiting for files in S3...");
    int expectedFileCount = (int) NUM_RECORDS_INSERT / FLUSH_SIZE_STANDARD;
    waitForFilesInBucket(TEST_BUCKET_NAME, expectedFileCount);

    assertTrue(fileNamesValid(TEST_BUCKET_NAME, TEST_TOPIC_NAME, EXPECTED_PARTITION, expectedFileExtension));
    assertTrue(fileContentsAsExpected(TEST_BUCKET_NAME, FLUSH_SIZE_STANDARD, recordValueStruct));
  }

  private Schema getSampleStructSchema() {
    return SchemaBuilder.struct()
        .field("ID", Schema.INT64_SCHEMA)
        .field("myBool", Schema.BOOLEAN_SCHEMA)
        .field("myInt32", Schema.INT32_SCHEMA)
        .field("myFloat32", Schema.FLOAT32_SCHEMA)
        .field("myFloat64", Schema.FLOAT64_SCHEMA)
        .field("myString", Schema.STRING_SCHEMA)
        .build();
  }

  private Struct getSampleStructVal(Schema structSchema) {
    Date sampleDate = new Date(1111111);
    sampleDate.setTime(0);
    return new Struct(structSchema)
        .put("ID", (long) 1)
        .put("myBool", true)
        .put("myInt32", 32)
        .put("myFloat32", 3.2f)
        .put("myFloat64", 64.64)
        .put("myString", "theStringVal");
  }
}
