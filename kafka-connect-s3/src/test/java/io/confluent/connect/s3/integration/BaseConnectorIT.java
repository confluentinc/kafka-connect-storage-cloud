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

import static io.confluent.connect.s3.S3SinkConnectorConfig.AWS_ACCESS_KEY_ID_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.AWS_SECRET_ACCESS_KEY_CONFIG;
import static io.confluent.kafka.schemaregistry.ClusterTestHarness.KAFKASTORE_TOPIC;
import org.apache.avro.generic.GenericData;
import org.apache.commons.io.FileUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Tagging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.RestApp;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import java.nio.file.Paths;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import io.confluent.connect.s3.util.S3Utils;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;

import org.apache.avro.io.DatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.TestUtils;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.S3Object;

import static org.assertj.core.api.Assertions.assertThat;

@Category(IntegrationTest.class)
public abstract class BaseConnectorIT {

  private static final Logger log = LoggerFactory.getLogger(BaseConnectorIT.class);

  protected static final int MAX_TASKS = 3;
  private static final long S3_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(120);

  protected static final long CONNECTOR_STARTUP_DURATION_MS = TimeUnit.MINUTES.toMillis(60);

  protected RestApp restApp;

  protected static final int TOPIC_PARTITION = 0;

  protected static final int DEFAULT_OFFSET = 0;

  protected static final int NUM_RECORDS_INSERT = 30;

  protected static final int FLUSH_SIZE_STANDARD = 3;

  protected static S3Client s3Client;

  protected static final String AVRO_EXTENSION = "avro";
  protected static final String PARQUET_EXTENSION = "snappy.parquet";
  protected static final String JSON_EXTENSION = "json";

  protected static final ObjectMapper jsonMapper = new ObjectMapper();
  // AWS configs
  protected static final String AWS_REGION = "us-west-2";
  protected static final String AWS_CREDENTIALS_PATH = "AWS_CREDENTIALS_PATH";
  // local dir configs
  protected static final String TEST_RESOURCES_PATH = "src/test/resources/";
  protected static final String TEST_DOWNLOAD_PATH = TEST_RESOURCES_PATH + "downloaded-files/";

  protected static final String STORAGE_CLASS_CONFIG = "storage.class";

  private static final Map<String, Function<String, List<JsonNode>>> contentGetters =
      ImmutableMap.of(
          JSON_EXTENSION, BaseConnectorIT::getContentsFromJson,
          AVRO_EXTENSION, BaseConnectorIT::getContentsFromAvro,
          PARQUET_EXTENSION, BaseConnectorIT::getContentsFromParquet
      );

  protected static final String TEST_BUCKET_NAME =
      "connect-s3-integration-testing-" + System.currentTimeMillis();
  protected EmbeddedConnectCluster connect;
  protected Map<String, String> props;

  @BeforeClass
  public static void setupClient() {
    log.info("Starting ITs...");
    s3Client = getS3Client();

    if (bucketExists(TEST_BUCKET_NAME)) {
      clearBucket(TEST_BUCKET_NAME);
    } else {
      s3Client.createBucket(CreateBucketRequest.builder().bucket(TEST_BUCKET_NAME)
          .build());
    }
  }

  private static boolean bucketExists(String bucket) {
    try {
      log.info("Checking bucket exists");
      s3Client.headBucket(HeadBucketRequest.builder().bucket(bucket).build());
      return true;
    } catch (NoSuchBucketException e) {
      log.info("No bucket found");
      return false;
    }
  }

  @AfterClass
  public static void deleteBucket() {
    s3Client.deleteBucket(DeleteBucketRequest.builder().bucket(TEST_BUCKET_NAME)
        .build());
    log.info("Finished ITs, removed S3 bucket");
  }

  @Before
  public void setup() throws Exception {
    startConnect();
    startSchemaRegistry();
  }

  @After
  public void close() throws Exception {
    // stop all SR, Connect, Kafka and Zk threads.
    stopSchemaRegistry();
    connect.stop();
  }

  protected void startConnect() {
    Map<String, String> workerProps = new HashMap<>();
    workerProps.put("plugin.discovery","hybrid_warn");
    connect = new EmbeddedConnectCluster.Builder()
        .name("s3-connect-cluster").workerProps(workerProps)
        .build();

    // start the clusters
    connect.start();
  }


  /**
   * Wait up to {@link #S3_TIMEOUT_MS maximum time limit} for the connector to write the specified
   * number of files.
   *
   * @param bucketName S3 bucket name
   * @param numFiles   expected number of files in the bucket
   * @return the time this method discovered the connector has written the files
   * @throws InterruptedException if this was interrupted
   */
  protected long waitForFilesInBucket(String bucketName, int numFiles) throws InterruptedException {
    return S3Utils.waitForFilesInBucket(s3Client, bucketName, numFiles, S3_TIMEOUT_MS);
  }

  /**
   * Get a list of the expected filenames for the bucket.
   * <p>
   * Format: topics/s3_topic/partition=97/s3_topic+97+0000000001.avro
   *
   * @param topic      the test kafka topic
   * @param partition  the expected partition for the tests
   * @param flushSize  the flush size connector config
   * @param numRecords the number of records produced in the test
   * @param extension  the expected extensions of the files including compression (snappy.parquet)
   * @return the list of expected filenames
   */
  protected List<String> getExpectedFilenames(
      String topic,
      int partition,
      int flushSize,
      int startOffset,
      long numRecords,
      String extension
  ) {
    int expectedFileCount = (int) numRecords / flushSize;
    List<String> expectedFiles = new ArrayList<>();
    for (int offset = startOffset; offset < expectedFileCount * flushSize; offset += flushSize) {
      String filepath = getFilePath(topic, partition, offset, extension);
      expectedFiles.add(filepath);
    }
    return expectedFiles;
  }

  private String getFilePath(String topic,
                             int partition,
                             int offset,
                             String extension) {
    return String.format(
        "topics/%s/partition=%d/%s+%d+%010d.%s",
        topic,
        partition,
        topic,
        partition,
        offset,
        extension
    );
  }
  protected Map<String, Tagging> getExpectedTags(
      String topic,
                                    int partition,
                                    int flushSize,
                                    int startOffset,
                                    long numRecords,
      String extension) {
    int expectedFileCount = (int) numRecords / flushSize;
    Map<String, Tagging> tags = new HashMap<>();
    for (int offset = startOffset; offset < expectedFileCount * flushSize; offset += flushSize) {
      String filepath = getFilePath(topic, partition, offset, extension);
      Tag startOffsetTag = Tag.builder()
          .key("startOffset")
          .value(String.valueOf(offset))
          .build();

      Tag endOffset = Tag.builder()
          .key("endOffset")
          .value(String.valueOf(offset + flushSize - 1))
          .build();

      Tag recordCount = Tag.builder()
          .key("recordCount")
          .value(String.valueOf(flushSize))
          .build();
      tags.put(filepath, Tagging.builder()
          .tagSet(Arrays.asList(startOffsetTag, endOffset, recordCount))
          .build());
    }
    return tags;
  }

  /**
   * Get a list of the expected filenames containing keys for the tombstone records for the bucket.
   * <p>
   * Format: topics/s3_topic/tombstone/s3_topic+97+0000000001.keys.avro
   *
   * @param topic      the test kafka topic
   * @param partition  the expected partition for the tests
   * @param flushSize  the flush size connector config
   * @param numRecords the number of records produced in the test
   * @param extension  the expected extensions of the files including compression (snappy.parquet)
   * @param tombstonePartition  the expected directory for tombstone records
   * @return the list of expected filenames
   */
  protected List<String> getExpectedTombstoneFilenames(
      String topic,
      int partition,
      int flushSize,
      long numRecords,
      String extension,
      String tombstonePartition
  ) {
    int expectedFileCount = (int) numRecords / flushSize;
    List<String> expectedFiles = new ArrayList<>();
    for (int offset = 0; offset < expectedFileCount * flushSize; offset += flushSize) {
      String filepath = String.format(
          "topics/%s/%s/%s+%d+%010d.keys.%s",
          topic,
          tombstonePartition,
          topic,
          partition,
          offset,
          extension
      );
      expectedFiles.add(filepath);
    }
    return expectedFiles;
  }

  /**
   * Check if the file names in the bucket have the expected namings.
   *
   * @param bucketName    the name of the bucket with the files
   * @param expectedFiles the list of expected filenames for exact comparison
   * @return whether all the files in the bucket match the expected values
   */
  protected void assertFileNamesValid(String bucketName, List<String> expectedFiles) {
    List<String> actualFiles = getBucketFileNames(bucketName);
    assertThat(actualFiles).containsExactlyInAnyOrderElementsOf(expectedFiles);
  }

  /**
   * Recursively query the bucket to get a list of filenames that exist in the bucket.
   *
   * @param bucketName the name of the bucket containing the files.
   * @return list of filenames present in the bucket.
   */
  private List<String> getBucketFileNames(String bucketName) {
    List<String> actualFiles = new ArrayList<>();
    ListObjectsV2Request.Builder request = ListObjectsV2Request.builder().bucket(bucketName);

    ListObjectsV2Response result;
    do {
      /*
       Need the result object to extract the continuation token from the request as each request
       to listObjectsV2() returns a maximum of 1000 files.
       */
      result = s3Client.listObjectsV2(request.build());
      for (S3Object file : result.contents()) {
        actualFiles.add(file.key());
      }
      String token = result.nextContinuationToken();
      // To get the next batch of files.
      request.continuationToken(token);
    } while(result.isTruncated());
    return actualFiles;
  }

  protected void startSchemaRegistry() throws Exception {
    int port = findAvailableOpenPort();
    restApp = new RestApp(port, null, connect.kafka().bootstrapServers(),
        KAFKASTORE_TOPIC, CompatibilityLevel.NONE.name, true, new Properties());
    restApp.start();
    waitForSchemaRegistryToStart();
  }

  protected void stopSchemaRegistry() throws Exception {
    if (restApp != null) {
      restApp.stop();
    }
  }

  protected void waitForSchemaRegistryToStart() throws InterruptedException {
    TestUtils.waitForCondition(
        () -> restApp.restServer.isRunning(),
        CONNECTOR_STARTUP_DURATION_MS,
        "Schema-registry server did not start in time."
    );
  }

  private Integer findAvailableOpenPort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  protected Schema getSampleStructSchema() {
    return SchemaBuilder.struct()
        .field("ID", Schema.INT64_SCHEMA)
        .field("myBool", Schema.BOOLEAN_SCHEMA)
        .field("myInt32", Schema.INT32_SCHEMA)
        .field("myFloat32", Schema.FLOAT32_SCHEMA)
        .field("myFloat64", Schema.FLOAT64_SCHEMA)
        .field("myString", Schema.STRING_SCHEMA)
        .build();
  }

  protected Struct getSampleStructVal(Schema structSchema) {
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

  protected SinkRecord getSampleTopicRecord(String topicName, Schema recordValueSchema,
      Struct recordValueStruct) {
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

  protected SinkRecord getSampleRecord(Schema recordValueSchema, Struct recordValueStruct, String topic) {
    return getSampleTopicRecord(topic, recordValueSchema, recordValueStruct);
  }

  protected Iterable<Header> sampleHeaders() {
    return Arrays.asList(
        new RecordHeader("first-header-key", "first-header-value".getBytes()),
        new RecordHeader("second-header-key", "second-header-value".getBytes())
    );
  }

  /**
   * Get an S3 client based on existing credentials
   *
   * @return an authenticated S3 client
   */
  protected static S3Client getS3Client() {
    Map<String, String> creds = getAWSCredentialFromPath();
    // If AWS credentials found on AWS_CREDENTIALS_PATH, use them (Jenkins)
    if (creds.size() == 2) {
      AwsBasicCredentials awsCreds = AwsBasicCredentials.create(creds.get(AWS_ACCESS_KEY_ID_CONFIG), creds.get(AWS_SECRET_ACCESS_KEY_CONFIG));
      return S3Client.builder()
          .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
          .region(Region.of(AWS_REGION))
          .build();
    }
    // DefaultAWSCredentialsProviderChain,
    // For local testing,  ~/.aws/credentials needs to be defined or other environment variables
    return S3Client.builder().region(Region.of(AWS_REGION)).build();
  }

  /**
   * Clear the given S3 bucket. Removes the contents, keeps the bucket.
   *
   * @param bucketName the name of the bucket to clear.
   */
  protected static void clearBucket(String bucketName) {
    for (S3Object file : s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).build()).contents()) {
      s3Client.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(file.key()).build());
    }
  }

  /**
   * Count total number of records stored in S3
   *
   * @param bucketName          the name of the s3 test bucket
   * @return number of records in S3
   */
  protected int countNumberOfRecords(
      String bucketName
  ) throws IOException {
    int rowCount = 0;
    for (String fileName :
        getS3FileListValues(s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).build()))) {
      String destinationPath = TEST_DOWNLOAD_PATH + fileName;
      File downloadedFile = downloadFile(bucketName, fileName, destinationPath);

      String fileExtension = getExtensionFromKey(fileName);
      List<JsonNode> downloadedFileContents = contentGetters.get(fileExtension)
          .apply(destinationPath);
      rowCount += downloadedFileContents.size();
      downloadedFile.delete();
    }
    return rowCount;
  }

  /**
   * Check the contents of the record value files in the S3 bucket compared to the expected row.
   *
   * @param bucketName          the name of the s3 test bucket
   * @param expectedRowsPerFile the number of rows a file should have
   * @param expectedRow         the expected row data in each file
   * @return whether every row of the files read equals the expected row
   */
  protected boolean fileContentsAsExpected(
      String bucketName,
      int expectedRowsPerFile,
      Struct expectedRow
  ) throws IOException {
    log.info("expectedRow: {}", expectedRow);
    for (String fileName :
        getS3FileListValues(s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).build()))) {
      String destinationPath = TEST_DOWNLOAD_PATH + fileName;
      File downloadedFile = new File(destinationPath);
      log.info("Saving file to : {}", destinationPath);
      ResponseInputStream<GetObjectResponse> is = s3Client.getObject(
          GetObjectRequest.builder()
              .bucket(bucketName).key(fileName)
              .build());

      FileUtils.copyInputStreamToFile(is, downloadedFile);

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

  private File downloadFile(String bucketName, String s3Filename, String destinationPath) throws IOException {

    log.info("Saving file to : {}", destinationPath);

    ResponseInputStream<GetObjectResponse> is = s3Client.getObject(
        GetObjectRequest.builder()
            .bucket(bucketName).key(s3Filename)
            .build());
    File downloadedFile = new File(destinationPath);
    FileUtils.copyInputStreamToFile(is, downloadedFile);
    return downloadedFile;
  }

  protected boolean keyfileContentsAsExpected(
      String bucketName,
      int expectedRowsPerFile,
      String expectedKey
  ) throws IOException {
    log.info("expectedKey: {}", expectedKey);

    ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
        .bucket(bucketName)
        .build();

    for (String fileName :
        getS3KeyFileList(s3Client.listObjectsV2Paginator(listObjectsV2Request))) {
      String destinationPath = TEST_DOWNLOAD_PATH + fileName;
      File downloadedFile = new File(destinationPath);
      log.info("Saving file to : {}", destinationPath);

      ResponseInputStream<GetObjectResponse> is = s3Client.getObject(
          GetObjectRequest.builder()
              .bucket(bucketName
              ).key(fileName)
              .build());

      FileUtils.copyInputStreamToFile(is, downloadedFile);

      List<String> keyContent = new ArrayList<>();
      try (FileReader fileReader = new FileReader(destinationPath);
          BufferedReader bufferedReader = new BufferedReader(fileReader)) {
        String line;
        while ((line = bufferedReader.readLine()) != null) {
          keyContent.add(line);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      if (keyContent.size() != expectedRowsPerFile) {
        log.error("Actual number of records in the key file {}, Expected number of records {}",
            keyContent.size(), expectedRowsPerFile);
        return false;
      }
      for (String actualKey: keyContent) {
        if (!expectedKey.equals(actualKey)) {
          log.error("Key {} did not match the contents in the key file {}", expectedKey, actualKey);
          return false;
        } else {
          log.info("Key {} matched the contents in the key file {}", expectedKey, actualKey);
        }
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
  protected boolean fileContentsMatchExpected(
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

  private List<String> getS3KeyFileList(ListObjectsV2Iterable response) {
    final String includeExtensions = ".keys.";
    return response.contents().stream()
        .filter(summary -> summary.key().contains(includeExtensions))
        .map(S3Object::key)
        .collect(Collectors.toList());
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
  private List<String> getS3FileListValues(ListObjectsV2Response summaries) {
    List<String> excludeExtensions = Arrays.asList(".headers.avro", ".keys.avro");
    return summaries.contents().stream()
        .filter(summary -> !filenameContainsExtensions(summary.key(), excludeExtensions))
        .map(S3Object::key)
        .collect(Collectors.toList());
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
    List<JsonNode> fileRows = new ArrayList<JsonNode>();
    try {
      InputFile inputFile = HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(filePath), new Configuration());
      try (ParquetReader<GenericData.Record> recordAvroParquetReader =
               AvroParquetReader.<GenericData.Record>builder(inputFile)
                   .build()) {
        GenericData.Record record;
        while ((record = recordAvroParquetReader.read()) != null) {
          fileRows.add(jsonMapper.readTree(record.toString()));
        }
        return fileRows;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
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
      FileReader fileReader = new FileReader(filePath);
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

  protected static Map<String, String> getAWSCredentialFromPath() {
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
