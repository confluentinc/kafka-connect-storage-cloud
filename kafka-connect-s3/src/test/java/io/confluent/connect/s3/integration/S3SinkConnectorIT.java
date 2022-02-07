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
import static io.confluent.connect.s3.S3SinkConnectorConfig.S3_BUCKET_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.STORE_KAFKA_HEADERS_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.STORE_KAFKA_KEYS_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.AWS_ACCESS_KEY_ID_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.AWS_SECRET_ACCESS_KEY_CONFIG;
import static io.confluent.connect.storage.StorageSinkConnectorConfig.FLUSH_SIZE_CONFIG;
import static io.confluent.connect.storage.StorageSinkConnectorConfig.FORMAT_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

import io.confluent.connect.s3.S3SinkConnector;
import io.confluent.connect.s3.S3SinkConnectorConfig.BehaviorOnNullValues;
import io.confluent.connect.s3.format.avro.AvroFormat;
import io.confluent.connect.s3.format.json.JsonFormat;
import io.confluent.connect.s3.format.parquet.ParquetFormat;
import io.confluent.connect.s3.storage.S3Storage;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.test.IntegrationTest;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.tools.json.JsonRecordFormatter;
import org.apache.parquet.tools.read.SimpleReadSupport;
import org.apache.parquet.tools.read.SimpleRecord;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"unchecked", "deprecation"})
@Category(IntegrationTest.class)
public class S3SinkConnectorIT extends BaseConnectorIT {

  private static final Logger log = LoggerFactory.getLogger(S3SinkConnectorIT.class);
  private static final ObjectMapper jsonMapper = new ObjectMapper();
  // AWS configs
  private static final String AWS_REGION = "us-west-2";
  private static final String AWS_CREDENTIALS_PATH = "AWS_CREDENTIALS_PATH";
  // local dir configs
  private static final String TEST_RESOURCES_PATH = "src/test/resources/";
  private static final String TEST_DOWNLOAD_PATH = TEST_RESOURCES_PATH + "downloaded-files/";
  // connector and test configs
  private static final String CONNECTOR_NAME = "s3-sink";
  private static final String DEFAULT_TEST_TOPIC_NAME = "TestTopic";
  private static final String STORAGE_CLASS_CONFIG = "storage.class";
  private static final String AVRO_EXTENSION = "avro";
  private static final String PARQUET_EXTENSION = "snappy.parquet";
  private static final String JSON_EXTENSION = "json";
  // DLQ Tests
  private static final String DLQ_TOPIC_CONFIG = "errors.deadletterqueue.topic.name";
  private static final String DLQ_TOPIC_NAME = "DLQ-topic";

  private static final List<String> KAFKA_TOPICS = Collections.singletonList(DEFAULT_TEST_TOPIC_NAME);
  private static final long CONSUME_MAX_DURATION_MS = TimeUnit.SECONDS.toMillis(10);
  private static final int NUM_RECORDS_INSERT = 30;
  private static final int FLUSH_SIZE_STANDARD = 3;
  private static final int TOPIC_PARTITION = 0;
  private static final int DEFAULT_OFFSET = 0;

  private static final Map<String, Function<String, List<JsonNode>>> contentGetters =
      ImmutableMap.of(
          JSON_EXTENSION, S3SinkConnectorIT::getContentsFromJson,
          AVRO_EXTENSION, S3SinkConnectorIT::getContentsFromAvro,
          PARQUET_EXTENSION, S3SinkConnectorIT::getContentsFromParquet
      );

  private JsonConverter jsonConverter;
  // custom producer to enable sending records with headers
  private Producer<byte[], byte[]> producer;

  @BeforeClass
  public static void setupClient() {
    log.info("Starting ITs...");
    S3Client = getS3Client();
    if (S3Client.doesBucketExistV2(TEST_BUCKET_NAME)) {
      clearBucket(TEST_BUCKET_NAME);
    } else {
      S3Client.createBucket(TEST_BUCKET_NAME);
    }
  }

  @AfterClass
  public static void deleteBucket() {
    S3Client.deleteBucket(TEST_BUCKET_NAME);
    log.info("Finished ITs, removed S3 bucket");
  }

  @Before
  public void before() {
    initializeJsonConverter();
    initializeCustomProducer();
    setupProperties();
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
    waitForConnectorToStart(CONNECTOR_NAME, Math.min(topicNames.size(), MAX_TASKS));

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

  @Test
  public void testFaultyRecordsReportedToDLQ() throws Throwable {
    props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    props.put(BEHAVIOR_ON_NULL_VALUES_CONFIG, BehaviorOnNullValues.IGNORE.toString());
    props.put(STORE_KAFKA_KEYS_CONFIG, "true");
    props.put(STORE_KAFKA_HEADERS_CONFIG, "true");
    props.put(DLQ_TOPIC_CONFIG, DLQ_TOPIC_NAME);
    props.put("errors.deadletterqueue.context.headers.enable", "true");
    props.put("errors.tolerance", "all");
    props.put("errors.deadletterqueue.topic.replication.factor", "1");

    // start sink connector
    connect.configureConnector(CONNECTOR_NAME, props);
    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, Math.min(KAFKA_TOPICS.size(), MAX_TASKS));

    Schema recordValueSchema = getSampleStructSchema();
    Struct recordValueStruct = getSampleStructVal(recordValueSchema);
    SinkRecord sampleRecord = getSampleRecord(recordValueSchema, recordValueStruct);

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
   * @param expectedMessages    the expected list of error messages
   * @param consumedDLQRecords  the records consumed from the DLQ topic
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

  private void produceRecordsNoHeaders(int recordCount, SinkRecord record)
      throws ExecutionException, InterruptedException {
    produceRecords(record.topic(), recordCount, record, true, true, false);
  }

  private void produceRecordsWithHeaders(String topic, int recordCount, SinkRecord record) throws Exception {
   produceRecords(topic, recordCount, record, true, true, true);
  }

  private void produceRecordsWithHeadersNoKey(String topic, int recordCount, SinkRecord record) throws Exception {
    produceRecords(topic, recordCount, record, false, true, true);
  }

  private void produceRecordsWithHeadersNoValue(String topic, int recordCount, SinkRecord record) throws Exception{
    produceRecords(topic, recordCount, record, true, false, true);
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

  private SinkRecord getSampleTopicRecord(String topicName, Schema recordValueSchema, Struct recordValueStruct ) {
    return new SinkRecord(
        topicName,
        TOPIC_PARTITION,
        Schema.STRING_SCHEMA,
        "key",
        recordValueSchema,
        recordValueStruct,
        DEFAULT_OFFSET
    );
  }

  private SinkRecord getSampleRecord(Schema recordValueSchema, Struct recordValueStruct ) {
    return getSampleTopicRecord(DEFAULT_TEST_TOPIC_NAME, recordValueSchema, recordValueStruct);
  }

  private Iterable<Header> sampleHeaders() {
    return Arrays.asList(
        new RecordHeader("first-header-key", "first-header-value".getBytes()),
        new RecordHeader("second-header-key", "second-header-value".getBytes())
    );
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

  /**
   * Get an S3 client based on existing credentials
   *
   * @return an authenticated S3 client
   */
  private static AmazonS3 getS3Client() {
     Map<String, String> creds = getAWSCredentialFromPath();
     // If AWS credentials found on AWS_CREDENTIALS_PATH, use them (Jenkins)
     if (creds.size() == 2) {
         BasicAWSCredentials awsCreds = new BasicAWSCredentials(
             creds.get(AWS_ACCESS_KEY_ID_CONFIG),
             creds.get(AWS_SECRET_ACCESS_KEY_CONFIG));
         return AmazonS3ClientBuilder.standard()
             .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
             .build();
     }
     // DefaultAWSCredentialsProviderChain,
     // For local testing,  ~/.aws/credentials needs to be defined or other environment variables
     return AmazonS3ClientBuilder.standard().withRegion(AWS_REGION).build();
  }

  /**
   * Clear the given S3 bucket. Removes the contents, keeps the bucket.
   *
   * @param bucketName the name of the bucket to clear.
   */
  private static void clearBucket(String bucketName) {
    for (S3ObjectSummary file : S3Client.listObjectsV2(bucketName).getObjectSummaries()) {
      S3Client.deleteObject(bucketName, file.getKey());
    }
  }

  /**
   * Check the contents of the record value files in the S3 bucket compared to the expected row.
   *
   * @param bucketName          the name of the s3 test bucket
   * @param expectedRowsPerFile the number of rows a file should have
   * @param expectedRow         the expected row data in each file
   * @return whether every row of the files read equals the expected row
   */
  private boolean fileContentsAsExpected(
      String bucketName,
      int expectedRowsPerFile,
      Struct expectedRow
  ) {
    log.info("expectedRow: {}", expectedRow);
    for (String fileName :
        getS3FileListValues(S3Client.listObjectsV2(bucketName).getObjectSummaries())) {
      String destinationPath = TEST_DOWNLOAD_PATH + fileName;
      File downloadedFile = new File(destinationPath);
      log.info("Saving file to : {}", destinationPath);
      S3Client.getObject(new GetObjectRequest(bucketName, fileName), downloadedFile);

      String fileExtension = getExtensionFromKey(fileName);
      List<JsonNode> downloadedFileContents = contentGetters.get(fileExtension)
          .apply(destinationPath);
      if (!fileContentsMatchExpected(downloadedFileContents, expectedRowsPerFile, expectedRow)) {
        return false;
      }
      downloadedFile.delete();
    }
    return true;
  }

  /**
   * Check if the contents of a downloaded file match the expected row.
   *
   * @param fileContents        the file contents as a list of JsonNodes
   * @param expectedRowsPerFile the number of rows expected in the file
   * @param expectedRow         the expected values of each row
   * @return whether the file contents match the expected row
   */
  private boolean fileContentsMatchExpected(
      List<JsonNode> fileContents,
      int expectedRowsPerFile,
      Struct expectedRow
  ) {
    if (fileContents.size() != expectedRowsPerFile) {
      log.error("Number of rows in file do not match the expected count, actual: {}, expected: {}",
          fileContents.size(), expectedRowsPerFile);
      return false;
    }
    for (JsonNode row : fileContents) {
      if (!fileRowMatchesExpectedRow(row, expectedRow)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Compare the row in the file and its values to the expected row's values.
   *
   * @param fileRow     the row read from the file as a JsonNode
   * @param expectedRow the expected contents of the row
   * @return whether the file row matches the expected row
   */
  private boolean fileRowMatchesExpectedRow(JsonNode fileRow, Struct expectedRow) {
    log.debug("Comparing rows: file: {}, expected: {}", fileRow, expectedRow);
    // compare the field values
    for (Field key : expectedRow.schema().fields()) {
      String expectedValue = expectedRow.get(key).toString();
      String rowValue = fileRow.get(key.name()).toString().replaceAll("^\"|\"$", "");
      log.debug("Comparing values: {}, {}", expectedValue, rowValue);
      if (!rowValue.equals(expectedValue)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Get the contents of an AVRO file at a given filepath.
   *
   * @param filePath the path of the downloaded file
   * @return the rows of the file as JsonNodes
   */
  private static List<JsonNode> getContentsFromAvro(String filePath) {
    try {
      DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
      DataFileReader<GenericRecord> dataFileReader = new DataFileReader(new File(filePath),
          datumReader);
      List<JsonNode> fileRows = new ArrayList<>();
      while (dataFileReader.hasNext()) {
        GenericRecord row = dataFileReader.next();
        JsonNode jsonNode = jsonMapper.readTree(row.toString());
        fileRows.add(jsonNode);
      }
      return fileRows;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the contents of a parquet file at a given filepath.
   *
   * @param filePath the path of the downloaded parquet file
   * @return the rows of the file as JsonNodes
   */
  private static List<JsonNode> getContentsFromParquet(String filePath) {
    try {
      ParquetReader<SimpleRecord> reader = ParquetReader
          .builder(new SimpleReadSupport(), new Path(filePath)).build();
      ParquetMetadata metadata = ParquetFileReader
          .readFooter(new Configuration(), new Path(filePath));
      JsonRecordFormatter.JsonGroupFormatter formatter = JsonRecordFormatter
          .fromSchema(metadata.getFileMetaData().getSchema());
      List<JsonNode> fileRows = new ArrayList<>();
      for (SimpleRecord value = reader.read(); value != null; value = reader.read()) {
        JsonNode jsonNode = jsonMapper.readTree(formatter.formatRecord(value));
        fileRows.add(jsonNode);
      }
      return fileRows;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the contents of a json file at a given filepath.
   *
   * @param filePath the path of the downloaded json file
   * @return the rows of the file as JsonNodes
   */
  private static List<JsonNode> getContentsFromJson(String filePath) {
    try {
      FileReader fileReader = new FileReader(new File(filePath));
      BufferedReader bufferedReader = new BufferedReader(fileReader);
      List<JsonNode> fileRows = new ArrayList<>();
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        fileRows.add(jsonMapper.readTree(line));
      }
      return fileRows;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the file extension from the S3 Object key
   * <p>
   * ex.: /topics/s3_topic/partition=97/s3_topic+97+0000000001.avro
   *
   * @param s3FileKey the object file key
   * @return the extension, may be .avro, .json, or .snappy.parquet,
   */
  private static String getExtensionFromKey(String s3FileKey) {
    String[] pathTokens = s3FileKey.split("/");
    // The last one is (presumably) the file name
    String fileName = pathTokens[pathTokens.length - 1];
    // The extension ".snappy.parquet" is a special case of a two-dot
    // extension, so check for that so that we can generalize the rest
    // of the checks in case there is a dot in the filename portion itself.
    if (fileName.endsWith("." + PARQUET_EXTENSION)) {
      return PARQUET_EXTENSION;
    }
    // Now on to the more generalized version
    int lastDot = fileName.lastIndexOf('.');
    if (lastDot < 0) {
      // no extension
      throw new RuntimeException("Could not parse extension from filename: " + s3FileKey);
    }

    return fileName.substring(lastDot + 1);
  }

  private void initializeJsonConverter() {
    Map<String, Object> jsonConverterProps = new HashMap<>();
    jsonConverterProps.put("schemas.enable", "true");
    jsonConverterProps.put("converter.type", "value");
    jsonConverter = new JsonConverter();
    jsonConverter.configure(jsonConverterProps);
  }

  // whether a filename contains any of the extensions
  private boolean filenameContainsExtensions(String filename, List<String> extensions) {
    for (String extension : extensions){
      if (filename.endsWith(extension)) {
        return true;
      }
    }
    return false;
  }

  // filter for values only.
  private List<String> getS3FileListValues(List<S3ObjectSummary> summaries) {
    List<String> excludeExtensions = Arrays.asList(".headers.avro", ".keys.avro");
    return summaries.stream()
        .filter(summary -> !filenameContainsExtensions(summary.getKey(), excludeExtensions))
        .map(S3ObjectSummary::getKey)
        .collect(Collectors.toList());
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

  private static Map<String, String> getAWSCredentialFromPath() {
    Map<String, String> map = new HashMap<>();
    if  (!System.getenv().containsKey(AWS_CREDENTIALS_PATH)) {
        return map;
    }
    String path = System.getenv().get(AWS_CREDENTIALS_PATH);
    try {
        Map<String, String> creds = new ObjectMapper()
            .readValue(new FileReader(path), Map.class);
        String value = creds.get("aws_access_key_id");
        if (value != null && !value.isEmpty()) {
          map.put(AWS_ACCESS_KEY_ID_CONFIG, value);
        }
        value = creds.get("aws_secret_access_key");
        if (value != null && !value.isEmpty()) {
          map.put(AWS_SECRET_ACCESS_KEY_CONFIG,value);
        }
    } catch (Exception e) {
        e.printStackTrace();
        throw new IllegalArgumentException(
                "AWS credentials file not found." + AWS_CREDENTIALS_PATH
            );
    }
    return map;
  }
}
