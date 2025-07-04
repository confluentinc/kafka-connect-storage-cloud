package io.confluent.connect.s3.integration;

import io.confluent.connect.s3.S3SinkConnector;
import io.confluent.connect.s3.format.json.JsonFormat;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.s3.util.EmbeddedConnectUtils;

import org.junit.Test;
import io.confluent.connect.storage.partitioner.PartitionerConfig;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.experimental.categories.Category;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;

import static io.confluent.connect.s3.S3SinkConnectorConfig.ENABLE_CONDITIONAL_WRITES_KEYS_HEADERS_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.HEADERS_FORMAT_CLASS_CONFIG;
import static org.junit.Assert.assertEquals;


import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.amazonaws.services.s3.model.ObjectMetadata;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static io.confluent.connect.s3.S3SinkConnectorConfig.ENABLE_CONDITIONAL_WRITES_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.KEYS_FORMAT_CLASS_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.S3_BUCKET_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.STORE_KAFKA_HEADERS_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.STORE_KAFKA_KEYS_CONFIG;
import static io.confluent.connect.storage.StorageSinkConnectorConfig.FLUSH_SIZE_CONFIG;
import static io.confluent.connect.storage.StorageSinkConnectorConfig.FORMAT_CLASS_CONFIG;
import static io.confluent.connect.storage.StorageSinkConnectorConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG;

@RunWith(Parameterized.class)
@Category(IntegrationTest.class)
public class S3SinkConditionalWriteIT extends BaseConnectorIT {

  private static final String DEFAULT_TEST_TOPIC_NAME = "TestTopic";
  private static final String CONNECTOR_NAME = "s3-sink";
  private static final List<String> KAFKA_TOPICS = Collections.singletonList(DEFAULT_TEST_TOPIC_NAME);

  @Parameterized.Parameter(0)
  public boolean storeHeaders;

  @Parameterized.Parameter(1)
  public boolean storeKeys;

  @Parameterized.Parameter(2)
  public List<DummyFileWriter> fileWriters;

  @Parameterized.Parameter(3)
  public List<String>[] expectedFileNames;

  @Parameterized.Parameter(4)
  public int expectedRecordCount;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    boolean storeHeaders = true;
    boolean storeKeys = true;
    return Arrays.asList(new Object[][]{
        //Tests with value file present will skip overwriting values file and will write keys/headers file as applicable
        {!storeHeaders, storeKeys, Collections.singletonList(valueWriter), new List[]{getExpectedFilenamesWithValues(), getExpectedFilenamesWithKeys()}, NUM_RECORDS_INSERT * 2 + 3},
        {!storeHeaders, !storeKeys, Collections.singletonList(valueWriter), new List[]{getExpectedFilenamesWithValues()}, NUM_RECORDS_INSERT + 1},

        {storeHeaders, !storeKeys, Collections.singletonList(valueWriter), new List[]{getExpectedFilenamesWithValues(), getExpectedFilenamesWithHeaders()}, NUM_RECORDS_INSERT * 2 + 3},
        {storeHeaders, storeKeys, Collections.singletonList(valueWriter), new List[]{getExpectedFilenamesWithValues(), getExpectedFilenamesWithKeys(), getExpectedFilenamesWithHeaders()}, NUM_RECORDS_INSERT * 3 + 5},

        // Tests with value and key file present will skip overwriting values and keys file and will write headers file as applicable
        {!storeHeaders, storeKeys, Arrays.asList(valueWriter, keyWriter), new List[]{getExpectedFilenamesWithValues(), getExpectedFilenamesWithKeys()}, NUM_RECORDS_INSERT * 2 + 2},
        {storeHeaders, storeKeys, Arrays.asList(valueWriter, keyWriter), new List[]{getExpectedFilenamesWithValues(), getExpectedFilenamesWithKeys(), getExpectedFilenamesWithHeaders()}, NUM_RECORDS_INSERT * 3 + 4},

        // Tests with value, key and header files present will not overwrite any files
        {storeHeaders, storeKeys, Arrays.asList(valueWriter, keyWriter, headerWriter), new List[]{getExpectedFilenamesWithValues(), getExpectedFilenamesWithKeys(), getExpectedFilenamesWithHeaders()}, NUM_RECORDS_INSERT * 3 + 3}
    });
  }

  @Before
  public void setUp() throws Exception {
    initializeJsonConverter();
    initializeCustomProducer();
    setupProperties();
    waitForSchemaRegistryToStart();
    //add class specific props
    props.put(SinkConnectorConfig.TOPICS_CONFIG, String.join(",", KAFKA_TOPICS));
    props.put(FLUSH_SIZE_CONFIG, Integer.toString(FLUSH_SIZE_STANDARD));
    props.put(FORMAT_CLASS_CONFIG, JsonFormat.class.getName());
    props.put(STORAGE_CLASS_CONFIG, S3Storage.class.getName());
    props.put(S3_BUCKET_CONFIG, TEST_BUCKET_NAME);
    props.put("s3.region", "us-west-1");
    props.put(ROTATE_SCHEDULE_INTERVAL_MS_CONFIG, "60000");
    props.put(ENABLE_CONDITIONAL_WRITES_CONFIG, "true");
    props.put(ENABLE_CONDITIONAL_WRITES_KEYS_HEADERS_CONFIG, "true");

    props.put(STORE_KAFKA_HEADERS_CONFIG, String.valueOf(storeHeaders));
    props.put(STORE_KAFKA_KEYS_CONFIG, String.valueOf(storeKeys));
    props.put(KEYS_FORMAT_CLASS_CONFIG, JsonFormat.class.getName());
    props.put(HEADERS_FORMAT_CLASS_CONFIG, JsonFormat.class.getName());

    props.put(PartitionerConfig.TIMEZONE_CONFIG, "UTC");
    props.put(PartitionerConfig.LOCALE_CONFIG, "en-GB");

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

  @FunctionalInterface
  public interface DummyFileWriter<T> {
    void write(T t);
  }

  @Test
  public void testConditionalWrites() throws Exception {

    fileWriters.forEach( writer -> writer.write(null));

    // Start connector
    connect.configureConnector(CONNECTOR_NAME, props);
    EmbeddedConnectUtils.waitForConnectorToStart(connect, CONNECTOR_NAME, Math.min(KAFKA_TOPICS.size(), MAX_TASKS));

    Schema recordValueSchema = getSampleStructSchema();
    Struct recordValueStruct = getSampleStructVal(recordValueSchema);

    for (String topic : KAFKA_TOPICS) {
      // Create and send records to Kafka using the topic name in the current 'thisTopicName'
      SinkRecord sampleRecord = getSampleTopicRecord(topic, recordValueSchema, recordValueStruct);
      produceRecords(NUM_RECORDS_INSERT, sampleRecord);
    }

    List<String> expectedFiles = Arrays.stream(expectedFileNames).flatMap(List::stream).collect(Collectors.toList());
    // Validate files in S3
    waitForFilesInBucket(TEST_BUCKET_NAME, expectedFiles.size());
    assertFileNamesValid(TEST_BUCKET_NAME, expectedFiles);
    assertEquals(expectedRecordCount, countNumberOfRecords(TEST_BUCKET_NAME)); // 1 duplicate record will be present in the seed file, 2 duplicate record will be present in key file
  }

  protected static List<String> getExpectedFilenames(
      String format,
      int startOffset
  ) {
    return getExpectedFilenames(
        format,
        DEFAULT_TEST_TOPIC_NAME,
        TOPIC_PARTITION,
        FLUSH_SIZE_STANDARD,
        startOffset,
        NUM_RECORDS_INSERT,
        JSON_EXTENSION
    );
  }

  protected void produceRecords(int recordCount, SinkRecord record)
      throws ExecutionException, InterruptedException {
    produceRecords(record.topic(), recordCount, record, true, true, true);
  }

  private static List<String> getExpectedFilenamesWithValues() {
    List<String> expectedFilenames = getExpectedFilenames(
        "topics/%s/partition=%d/%s+%d+%010d.%s",
        1 // New files in S3 will start from offset 1, since file with offset 0 already exists in S3
    );

    String valueFilename = String.format("topics/%s/partition=0/%s+0+0000000000.json", DEFAULT_TEST_TOPIC_NAME, DEFAULT_TEST_TOPIC_NAME);
    expectedFilenames.add(valueFilename);
    return expectedFilenames;
  }

  private static List<String> getExpectedFilenamesWithKeys() {
    List<String> expectedFilenames = getExpectedFilenames(
        "topics/%s/partition=%d/%s+%d+%010d.keys.%s",
        1 // New files in S3 will start from offset 1, since file with offset 0 already exists in S3
    );

    String keyFilename = String.format("topics/%s/partition=0/%s+0+0000000000.keys.json", DEFAULT_TEST_TOPIC_NAME, DEFAULT_TEST_TOPIC_NAME);
    expectedFilenames.add(keyFilename);
    return expectedFilenames;
  }

  private static List<String> getExpectedFilenamesWithHeaders() {
    List<String> expectedFilenames = getExpectedFilenames(
        "topics/%s/partition=%d/%s+%d+%010d.headers.%s",
        1 // New files in S3 will start from offset 1, since file with offset 0 already exists in S3
    );

    String keyFilename = String.format("topics/%s/partition=0/%s+0+0000000000.headers.json", DEFAULT_TEST_TOPIC_NAME, DEFAULT_TEST_TOPIC_NAME);
    expectedFilenames.add(keyFilename);
    return expectedFilenames;
  }

  private static DummyFileWriter keyWriter = new FileWriter("topics/%s/partition=0/%s+0+0000000000.keys.json");
  private static DummyFileWriter valueWriter = new FileWriter("topics/%s/partition=0/%s+0+0000000000.json");
  private static DummyFileWriter headerWriter = new FileWriter("topics/%s/partition=0/%s+0+0000000000.headers.json");

  static class FileWriter implements DummyFileWriter {
    private final String key;

    public FileWriter(String key) {
      this.key = key;
    }

    @Override
    public void write(Object o) {
      String keyFilename = String.format(key, DEFAULT_TEST_TOPIC_NAME, DEFAULT_TEST_TOPIC_NAME);
      writeDummyFile(keyFilename);
    }
  }

  private static void writeDummyFile(String key) {
    String initialFileContents = "{\"ID\":1,\"myBool\":true,\"myInt32\":32,\"myFloat32\":3.2,\"myFloat64\":64.64,\"myString\":\"theStringVal\"}\n"
        + "{\"ID\":1,\"myBool\":true,\"myInt32\":32,\"myFloat32\":3.2,\"myFloat64\":64.64,\"myString\":\"theStringVal\"}";
    S3Client.putObject(TEST_BUCKET_NAME, key, new ByteArrayInputStream(initialFileContents.getBytes()), new ObjectMetadata());
  }
}