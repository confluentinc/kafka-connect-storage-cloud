/*
 * Copyright 2025 Confluent Inc.
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
import static io.confluent.connect.s3.util.HelperUtil.initializeCustomProducer;
import static io.confluent.connect.s3.util.HelperUtil.initializeJsonConverter;
import static io.confluent.connect.storage.StorageSinkConnectorConfig.FLUSH_SIZE_CONFIG;
import static io.confluent.connect.storage.StorageSinkConnectorConfig.FORMAT_CLASS_CONFIG;
import static io.confluent.connect.storage.StorageSinkConnectorConfig.MODE_CONFIG;
import static io.confluent.connect.storage.StorageSinkConnectorConfig.SCHEMA_COMPATIBILITY_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.connect.s3.S3SinkConnector;
import io.confluent.connect.s3.format.avro.AvroFormat;
import io.confluent.connect.s3.format.json.JsonFormat;
import io.confluent.connect.s3.format.parquet.ParquetFormat;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.s3.util.EmbeddedConnectUtils;
import io.confluent.connect.storage.StorageSinkConnectorConfig.Mode;
import io.confluent.connect.storage.backup.BackupEnvelope;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

/**
 * IT for {@link io.confluent.connect.s3.BackupS3SinkTask} in
 * {@code mode=BACKUP_FULL_RECORD}. Covers envelope shape across Avro/JSON/Parquet,
 * headers, tombstones, schema-change rotation, and byte-perfect round-trip.
 *
 * <p>TODO(INIT-5113): schema-backup file production (.entry.json + .avsc) is not
 * covered here. It needs SR 8.3+ converters (Java 17 bytecode) which cannot load
 * in this JDK 8 module. Covered by unit tests on the storage-common side.
 */
@Category(IntegrationTest.class)
public class BackupS3SinkConnectorIT extends BaseConnectorIT {

  private static final Logger log = LoggerFactory.getLogger(BackupS3SinkConnectorIT.class);

  private static final String CONNECTOR_NAME = "backup-s3-sink";
  private static final String DEFAULT_TEST_TOPIC_NAME = "BackupTestTopic";
  private static final List<String> KAFKA_TOPICS =
      Collections.singletonList(DEFAULT_TEST_TOPIC_NAME);

  private static final String JSON_EMBEDDED_SCHEMA = "JSON_EMBEDDED_SCHEMA";
  private static final String TYPE_NONE = "NONE";
  private static final String TYPE_BYTES = "BYTES";
  private static final String ENVELOPE_MISSING_PREFIX = "envelope missing ";

  /**
   * Expected envelope shape passed to {@link #fileMatchesEnvelope} /
   * {@link #envelopeContentsAsExpected} / {@link #rowMatchesEnvelope}. Bundles
   * the six per-row expectations so those helpers stay under the Sonar S107
   * parameter limit.
   */
  private static final class ExpectedEnvelope {
    final int rowsPerFile;
    final String topic;
    final Struct valueStruct;
    final String keySchemaType;
    final String valueSchemaType;
    final int headerCount;

    ExpectedEnvelope(int rowsPerFile, String topic, Struct valueStruct,
                     String keySchemaType, String valueSchemaType, int headerCount) {
      this.rowsPerFile = rowsPerFile;
      this.topic = topic;
      this.valueStruct = valueStruct;
      this.keySchemaType = keySchemaType;
      this.valueSchemaType = valueSchemaType;
      this.headerCount = headerCount;
    }
  }

  private JsonConverter jsonConverter;
  private Producer<byte[], byte[]> producer;

  @Before
  public void before() throws InterruptedException {
    jsonConverter = initializeJsonConverter();
    producer = initializeCustomProducer(connect);
    setupProperties();
    waitForSchemaRegistryToStart();
    props.put(SinkConnectorConfig.TOPICS_CONFIG, String.join(",", KAFKA_TOPICS));
    props.put(FLUSH_SIZE_CONFIG, Integer.toString(FLUSH_SIZE_STANDARD));
    props.put(FORMAT_CLASS_CONFIG, AvroFormat.class.getName());
    props.put(STORAGE_CLASS_CONFIG, S3Storage.class.getName());
    props.put(S3_BUCKET_CONFIG, TEST_BUCKET_NAME);
    props.put(MODE_CONFIG, Mode.BACKUP_FULL_RECORD.name());
    KAFKA_TOPICS.forEach(topic -> connect.kafka().createTopic(topic, 1));
  }

  @After
  public void after() throws Exception {
    FileUtils.deleteDirectory(new File(TEST_DOWNLOAD_PATH));
    clearBucket(TEST_BUCKET_NAME);
    waitForFilesInBucket(TEST_BUCKET_NAME, 0);
  }

  @Test
  public void testBackupWrapsRecordsInEnvelopeAvro() throws Throwable {
    props.put(FORMAT_CLASS_CONFIG, AvroFormat.class.getName());
    runBackupTest(AVRO_EXTENSION);
  }

  @Test
  public void testBackupWrapsRecordsInEnvelopeJson() throws Throwable {
    props.put(FORMAT_CLASS_CONFIG, JsonFormat.class.getName());
    props.put("format.json.schema.enable", "true");
    runBackupTest(JSON_EXTENSION);
  }

  @Test
  public void testBackupWrapsRecordsInEnvelopeParquet() throws Throwable {
    props.put(FORMAT_CLASS_CONFIG, ParquetFormat.class.getName());
    runBackupTest(PARQUET_EXTENSION);
  }

  @Test
  public void testBackupPreservesTombstones() throws Throwable {
    props.put(FORMAT_CLASS_CONFIG, AvroFormat.class.getName());
    props.put(SCHEMA_COMPATIBILITY_CONFIG, "NONE");
    connect.configureConnector(CONNECTOR_NAME, props);
    EmbeddedConnectUtils.waitForConnectorToStart(connect, CONNECTOR_NAME, 1);

    Schema valueSchema = getSampleStructSchema();
    Struct valueStruct = getSampleStructVal(valueSchema);
    SinkRecord sampleRecord = getSampleTopicRecord(
        DEFAULT_TEST_TOPIC_NAME, valueSchema, valueStruct);

    produceRecords(sampleRecord.topic(), FLUSH_SIZE_STANDARD, sampleRecord,
        true, true, true, jsonConverter, producer);
    produceRecords(sampleRecord.topic(), FLUSH_SIZE_STANDARD, sampleRecord,
        true, false, true, jsonConverter, producer);

    waitForFilesInBucket(TEST_BUCKET_NAME, 2);

    List<String> files = new ArrayList<>(getS3FileListValues(
        s3Client.listObjectsV2(ListObjectsV2Request.builder()
            .bucket(TEST_BUCKET_NAME).build())));
    Collections.sort(files);
    assertEquals(2, files.size());

    assertTrue(fileMatchesEnvelope(files.get(0), AVRO_EXTENSION,
        new ExpectedEnvelope(FLUSH_SIZE_STANDARD, DEFAULT_TEST_TOPIC_NAME, valueStruct,
            JSON_EMBEDDED_SCHEMA, JSON_EMBEDDED_SCHEMA, 2)));
    assertTrue(fileMatchesEnvelope(files.get(1), AVRO_EXTENSION,
        new ExpectedEnvelope(FLUSH_SIZE_STANDARD, DEFAULT_TEST_TOPIC_NAME, null,
            JSON_EMBEDDED_SCHEMA, TYPE_NONE, 2)));
  }

  @Test
  public void testBackupRotatesOnValueSchemaChange() throws Throwable {
    props.put(FORMAT_CLASS_CONFIG, AvroFormat.class.getName());
    props.put(SCHEMA_COMPATIBILITY_CONFIG, "NONE");
    connect.configureConnector(CONNECTOR_NAME, props);
    EmbeddedConnectUtils.waitForConnectorToStart(connect, CONNECTOR_NAME, 1);

    Schema schema1 = getSampleStructSchema();
    Struct value1 = getSampleStructVal(schema1);
    Schema schema2 = SchemaBuilder.struct().name("BackupAltSchema")
        .field("altInt", Schema.INT32_SCHEMA)
        .field("altString", Schema.STRING_SCHEMA)
        .build();
    Struct value2 = new Struct(schema2)
        .put("altInt", 42)
        .put("altString", "rotated");

    SinkRecord rec1 = getSampleTopicRecord(DEFAULT_TEST_TOPIC_NAME, schema1, value1);
    SinkRecord rec2 = getSampleTopicRecord(DEFAULT_TEST_TOPIC_NAME, schema2, value2);

    produceRecords(DEFAULT_TEST_TOPIC_NAME, FLUSH_SIZE_STANDARD, rec1,
        true, true, true, jsonConverter, producer);
    produceRecords(DEFAULT_TEST_TOPIC_NAME, FLUSH_SIZE_STANDARD, rec2,
        true, true, true, jsonConverter, producer);

    waitForFilesInBucket(TEST_BUCKET_NAME, 2);

    List<String> files = new ArrayList<>(getS3FileListValues(
        s3Client.listObjectsV2(ListObjectsV2Request.builder()
            .bucket(TEST_BUCKET_NAME).build())));
    Collections.sort(files);
    assertEquals(2, files.size());

    assertTrue(fileMatchesEnvelope(files.get(0), AVRO_EXTENSION,
        new ExpectedEnvelope(FLUSH_SIZE_STANDARD, DEFAULT_TEST_TOPIC_NAME, value1,
            JSON_EMBEDDED_SCHEMA, JSON_EMBEDDED_SCHEMA, 2)));
    assertTrue(fileMatchesEnvelope(files.get(1), AVRO_EXTENSION,
        new ExpectedEnvelope(FLUSH_SIZE_STANDARD, DEFAULT_TEST_TOPIC_NAME, value2,
            JSON_EMBEDDED_SCHEMA, JSON_EMBEDDED_SCHEMA, 2)));
  }

  @Test
  public void testBackupPreservesRawBytesWithByteArrayConverter() throws Throwable {
    props.put(FORMAT_CLASS_CONFIG, AvroFormat.class.getName());
    props.put(KEY_CONVERTER_CLASS_CONFIG, ByteArrayConverter.class.getName());
    props.put(VALUE_CONVERTER_CLASS_CONFIG, ByteArrayConverter.class.getName());
    connect.configureConnector(CONNECTOR_NAME, props);
    EmbeddedConnectUtils.waitForConnectorToStart(connect, CONNECTOR_NAME, 1);

    byte[] rawKey = new byte[] {0x00, 0x01, (byte) 0xFE, (byte) 0xFF, 0x42};
    byte[] rawValue = new byte[] {
        (byte) 0x89, (byte) 0xC0, 0x00, 0x00, 0x0A, 0x1B, 0x2C, 0x3D
    };
    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
        DEFAULT_TEST_TOPIC_NAME, TOPIC_PARTITION, null, rawKey, rawValue, sampleHeaders());
    for (int i = 0; i < FLUSH_SIZE_STANDARD; i++) {
      producer.send(record).get();
    }

    waitForFilesInBucket(TEST_BUCKET_NAME, 1);

    List<String> files = new ArrayList<>(getS3FileListValues(
        s3Client.listObjectsV2(ListObjectsV2Request.builder()
            .bucket(TEST_BUCKET_NAME).build())));
    assertEquals(1, files.size());
    assertByteEnvelopeFileMatches(files.get(0), FLUSH_SIZE_STANDARD, rawKey, rawValue);
  }

  private void runBackupTest(String expectedFileExtension) throws Throwable {
    connect.configureConnector(CONNECTOR_NAME, props);
    EmbeddedConnectUtils.waitForConnectorToStart(connect, CONNECTOR_NAME,
        Math.min(KAFKA_TOPICS.size(), MAX_TASKS));

    Schema valueSchema = getSampleStructSchema();
    Struct valueStruct = getSampleStructVal(valueSchema);

    for (String topic : KAFKA_TOPICS) {
      SinkRecord sampleRecord = getSampleTopicRecord(topic, valueSchema, valueStruct);
      produceRecords(sampleRecord.topic(), NUM_RECORDS_INSERT, sampleRecord,
          true, true, true, jsonConverter, producer);
    }

    log.info("Waiting for files in S3...");
    int countPerTopic = NUM_RECORDS_INSERT / FLUSH_SIZE_STANDARD;
    int expectedTotalFileCount = countPerTopic * KAFKA_TOPICS.size();
    waitForFilesInBucket(TEST_BUCKET_NAME, expectedTotalFileCount);

    Set<String> expectedTopicFilenames = new TreeSet<>();
    for (String topic : KAFKA_TOPICS) {
      List<String> theseFiles = getExpectedFilenames(topic, TOPIC_PARTITION,
          FLUSH_SIZE_STANDARD, 0, NUM_RECORDS_INSERT, expectedFileExtension);
      assertEquals(theseFiles.size(), countPerTopic);
      expectedTopicFilenames.addAll(theseFiles);
    }
    assertEquals(expectedTopicFilenames.size(), expectedTotalFileCount);
    assertFileNamesValid(TEST_BUCKET_NAME, new ArrayList<>(expectedTopicFilenames));

    assertTrue(envelopeContentsAsExpected(TEST_BUCKET_NAME, expectedFileExtension,
        new ExpectedEnvelope(FLUSH_SIZE_STANDARD, DEFAULT_TEST_TOPIC_NAME, valueStruct,
            JSON_EMBEDDED_SCHEMA, JSON_EMBEDDED_SCHEMA, 2)));
  }

  private boolean envelopeContentsAsExpected(String bucketName, String extension,
                                             ExpectedEnvelope expected) throws IOException {
    for (String fileName : getS3FileListValues(
        s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).build()))) {
      if (!fileMatchesEnvelope(fileName, extension, expected)) {
        return false;
      }
    }
    return true;
  }

  private boolean fileMatchesEnvelope(String fileName, String extension,
                                      ExpectedEnvelope expected) throws IOException {
    String destinationPath = TEST_DOWNLOAD_PATH + fileName;
    File downloadedFile = new File(destinationPath);
    log.info("Reading envelope file {}", destinationPath);
    ResponseInputStream<GetObjectResponse> is = s3Client.getObject(
        GetObjectRequest.builder().bucket(TEST_BUCKET_NAME).key(fileName).build());
    FileUtils.copyInputStreamToFile(is, downloadedFile);
    try {
      List<JsonNode> rows = getFileContents(destinationPath, extension);
      if (rows.size() != expected.rowsPerFile) {
        log.error("Row count {} != expected {} in {}",
            rows.size(), expected.rowsPerFile, fileName);
        return false;
      }
      for (JsonNode row : rows) {
        if (!rowMatchesEnvelope(row, expected)) {
          return false;
        }
      }
      return true;
    } finally {
      downloadedFile.delete();
    }
  }

  private boolean rowMatchesEnvelope(JsonNode row, ExpectedEnvelope expected) {
    String expectedTopic = expected.topic;
    Struct expectedValueStruct = expected.valueStruct;
    String expectedKeySchemaType = expected.keySchemaType;
    String expectedValueSchemaType = expected.valueSchemaType;
    int expectedHeaderCount = expected.headerCount;
    // JsonFormat with format.json.schema.enable=true wraps rows as {schema,payload}.
    if (row.has("payload") && row.has("schema")) {
      row = row.get("payload");
    }
    JsonNode topicNode = row.get(BackupEnvelope.FIELD_TOPIC);
    JsonNode partitionNode = row.get(BackupEnvelope.FIELD_PARTITION);
    JsonNode offsetNode = row.get(BackupEnvelope.FIELD_OFFSET);
    JsonNode keyNode = row.get(BackupEnvelope.FIELD_KEY);
    JsonNode valueNode = row.get(BackupEnvelope.FIELD_VALUE);
    JsonNode headersNode = row.get(BackupEnvelope.FIELD_HEADERS);
    JsonNode keySchemaTypeNode = row.get(BackupEnvelope.FIELD_KEY_SCHEMA_TYPE);
    JsonNode valueSchemaTypeNode = row.get(BackupEnvelope.FIELD_VALUE_SCHEMA_TYPE);
    assertNotNull(ENVELOPE_MISSING_PREFIX +BackupEnvelope.FIELD_TOPIC, topicNode);
    assertNotNull(ENVELOPE_MISSING_PREFIX +BackupEnvelope.FIELD_PARTITION, partitionNode);
    assertNotNull(ENVELOPE_MISSING_PREFIX +BackupEnvelope.FIELD_OFFSET, offsetNode);
    assertNotNull(ENVELOPE_MISSING_PREFIX +BackupEnvelope.FIELD_KEY, keyNode);
    assertNotNull(ENVELOPE_MISSING_PREFIX +BackupEnvelope.FIELD_HEADERS, headersNode);
    assertNotNull(ENVELOPE_MISSING_PREFIX +BackupEnvelope.FIELD_KEY_SCHEMA_TYPE, keySchemaTypeNode);
    assertNotNull(ENVELOPE_MISSING_PREFIX +BackupEnvelope.FIELD_VALUE_SCHEMA_TYPE,
        valueSchemaTypeNode);

    if (!expectedTopic.equals(topicNode.asText())) {
      log.error("topic mismatch: got={}, expected={}", topicNode.asText(), expectedTopic);
      return false;
    }
    if (partitionNode.asInt() != TOPIC_PARTITION) {
      log.error("partition mismatch: got={}, expected={}",
          partitionNode.asInt(), TOPIC_PARTITION);
      return false;
    }
    if (!offsetNode.isNumber() || offsetNode.asLong() < 0) {
      log.error("offset invalid: {}", offsetNode);
      return false;
    }
    if (!"key".equals(keyNode.asText())) {
      log.error("key mismatch: got={}, expected=key", keyNode.asText());
      return false;
    }
    if (!expectedKeySchemaType.equals(keySchemaTypeNode.asText())) {
      log.error("keySchemaType mismatch: got={}, expected={}",
          keySchemaTypeNode.asText(), expectedKeySchemaType);
      return false;
    }
    if (!expectedValueSchemaType.equals(valueSchemaTypeNode.asText())) {
      log.error("valueSchemaType mismatch: got={}, expected={}",
          valueSchemaTypeNode.asText(), expectedValueSchemaType);
      return false;
    }
    if (!headersNode.isArray() || headersNode.size() != expectedHeaderCount) {
      log.error("headers count mismatch: got={}, expected={}",
          headersNode.size(), expectedHeaderCount);
      return false;
    }
    if (expectedHeaderCount > 0 && !headersMatchSample(headersNode)) {
      return false;
    }
    if (expectedValueStruct == null) {
      if (!valueNode.isNull()) {
        log.error("expected tombstone value=null, got={}", valueNode);
        return false;
      }
      return true;
    }
    for (Field f : expectedValueStruct.schema().fields()) {
      String expectedStr = expectedValueStruct.get(f).toString();
      JsonNode actualNode = valueNode.get(f.name());
      if (actualNode == null) {
        log.error("value.{} missing from envelope row", f.name());
        return false;
      }
      String actual = actualNode.asText();
      if (!actual.equals(expectedStr)) {
        log.error("value.{} mismatch: got={}, expected={}", f.name(), actual, expectedStr);
        return false;
      }
    }
    return true;
  }

  private boolean headersMatchSample(JsonNode headersNode) {
    java.util.Map<String, String> got = new java.util.HashMap<>();
    for (JsonNode h : headersNode) {
      JsonNode k = h.get(BackupEnvelope.FIELD_HEADER_KEY);
      JsonNode v = h.get(BackupEnvelope.FIELD_HEADER_VALUE);
      if (k == null || v == null) {
        log.error("header entry missing key/value: {}", h);
        return false;
      }
      got.put(k.asText(), v.asText());
    }
    java.util.Map<String, String> expected = new java.util.HashMap<>();
    expected.put("first-header-key", "first-header-value");
    expected.put("second-header-key", "second-header-value");
    if (!expected.equals(got)) {
      log.error("headers content mismatch: got={}, expected={}", got, expected);
      return false;
    }
    return true;
  }

  // Reads the Avro file directly - Avro bytes fields don't survive GenericRecord.toString().
  private void assertByteEnvelopeFileMatches(String fileName, int expectedRows,
                                             byte[] expectedKey, byte[] expectedValue)
      throws IOException {
    String destinationPath = TEST_DOWNLOAD_PATH + fileName;
    File downloadedFile = new File(destinationPath);
    ResponseInputStream<GetObjectResponse> is = s3Client.getObject(
        GetObjectRequest.builder().bucket(TEST_BUCKET_NAME).key(fileName).build());
    FileUtils.copyInputStreamToFile(is, downloadedFile);
    try {
      DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
      try (DataFileReader<GenericRecord> reader =
               new DataFileReader<>(downloadedFile, datumReader)) {
        int count = 0;
        while (reader.hasNext()) {
          GenericRecord row = reader.next();
          count++;
          assertEquals(DEFAULT_TEST_TOPIC_NAME,
              row.get(BackupEnvelope.FIELD_TOPIC).toString());
          assertEquals((long) TOPIC_PARTITION,
              ((Integer) row.get(BackupEnvelope.FIELD_PARTITION)).longValue());
          assertEquals(TYPE_BYTES,
              row.get(BackupEnvelope.FIELD_KEY_SCHEMA_TYPE).toString());
          assertEquals(TYPE_BYTES,
              row.get(BackupEnvelope.FIELD_VALUE_SCHEMA_TYPE).toString());
          assertArrayEquals("key bytes mismatch",
              expectedKey, toByteArray(row.get(BackupEnvelope.FIELD_KEY)));
          assertArrayEquals("value bytes mismatch",
              expectedValue, toByteArray(row.get(BackupEnvelope.FIELD_VALUE)));
          @SuppressWarnings("unchecked")
          List<GenericRecord> headers =
              (List<GenericRecord>) row.get(BackupEnvelope.FIELD_HEADERS);
          assertEquals("header count", 2, headers.size());
          java.util.Map<String, String> got = new java.util.HashMap<>();
          for (GenericRecord h : headers) {
            got.put(h.get(BackupEnvelope.FIELD_HEADER_KEY).toString(),
                h.get(BackupEnvelope.FIELD_HEADER_VALUE).toString());
          }
          assertEquals("first-header-value", got.get("first-header-key"));
          assertEquals("second-header-value", got.get("second-header-key"));
        }
        assertEquals(expectedRows, count);
      }
    } finally {
      downloadedFile.delete();
    }
  }

  private static byte[] toByteArray(Object avroBytes) {
    if (avroBytes instanceof ByteBuffer) {
      ByteBuffer bb = ((ByteBuffer) avroBytes).duplicate();
      byte[] out = new byte[bb.remaining()];
      bb.get(out);
      return out;
    }
    if (avroBytes instanceof byte[]) {
      return (byte[]) avroBytes;
    }
    throw new IllegalStateException(
        "unexpected Avro bytes representation: " + avroBytes.getClass());
  }

  private void setupProperties() {
    props = new HashMap<>();
    props.put(CONNECTOR_CLASS_CONFIG, S3SinkConnector.class.getName());
    props.put(TASKS_MAX_CONFIG, Integer.toString(MAX_TASKS));
    props.put(KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    props.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    props.putAll(getAWSCredentialFromPath());
  }
}
