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

import static io.confluent.connect.utils.licensing.LicenseConfigUtil.CONFLUENT_TOPIC_BOOTSTRAP_SERVERS_CONFIG;
import static io.confluent.connect.utils.licensing.LicenseConfigUtil.CONFLUENT_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.common.utils.IntegrationTest;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.mapred.InvalidFileTypeException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.TestUtils;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.tools.json.JsonRecordFormatter;
import org.apache.parquet.tools.read.SimpleReadSupport;
import org.apache.parquet.tools.read.SimpleRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(IntegrationTest.class)
public abstract class BaseConnectorIT {

  protected static final String TEST_BUCKET_NAME = "confluent-kafka-connect-s3-testing";
  protected static final String CONNECTOR_CLASS_NAME = "S3SinkConnector";
  protected static final String CONNECTOR_NAME = "s3-sink";
  protected static final int MAX_TASKS = 3;

  private static final Logger log = LoggerFactory.getLogger(BaseConnectorIT.class);
  private static final String TEST_RESOURCES_PATH = "src/test/resources/";
  private static final String AWS_REGION = "us-west-2";
  private static final String TEST_DOWNLOAD_PATH = TEST_RESOURCES_PATH + "downloaded-files/";
  private static final long CONNECTOR_STARTUP_DURATION_MS = TimeUnit.MINUTES.toMillis(1);
  private static final long S3_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(30);

  protected AmazonS3 S3Client;
  protected EmbeddedConnectCluster connect;
  protected Map<String, String> props;
  private ObjectMapper jsonMapper = new ObjectMapper();

  @Before
  public void setup() {
    startConnect();
    setupProperties();
    // DefaultAWSCredentialsProviderChain, assumes .aws/credentials is setup
    S3Client = AmazonS3ClientBuilder.standard().withRegion(AWS_REGION).build();
    clearBucket(TEST_BUCKET_NAME);
  }

  @After
  public void close() throws Throwable {
    // stop all Connect, Kafka and Zk threads.
    connect.stop();
    // delete the downloaded test file folder
    FileUtils.deleteDirectory(new File(TEST_DOWNLOAD_PATH));
    clearBucket(TEST_BUCKET_NAME);
  }

  private void setupProperties() {
    props = new HashMap<>();
    props.put(CONNECTOR_CLASS_CONFIG, CONNECTOR_CLASS_NAME);
    props.put(TASKS_MAX_CONFIG, Integer.toString(MAX_TASKS));
    // converters
    props.put(KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    props.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    // license properties
    props.put(CONFLUENT_TOPIC_BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers());
    props.put(CONFLUENT_TOPIC_REPLICATION_FACTOR_CONFIG, "1");
  }

  protected void startConnect() {
    connect = new EmbeddedConnectCluster.Builder()
        .name("s3-connect-cluster")
        .build();

    // start the clusters
    connect.start();
  }

  /**
   * Wait up to {@link #CONNECTOR_STARTUP_DURATION_MS maximum time limit} for the connector with the
   * given name to start the specified number of tasks.
   *
   * @param name     the name of the connector
   * @param numTasks the minimum number of tasks that are expected
   * @return the time this method discovered the connector has started, in milliseconds past epoch
   * @throws InterruptedException if this was interrupted
   */
  protected long waitForConnectorToStart(String name, int numTasks) throws InterruptedException {
    TestUtils.waitForCondition(
        () -> assertConnectorAndTasksRunning(name, numTasks).orElse(false),
        CONNECTOR_STARTUP_DURATION_MS,
        "Connector tasks did not start in time."
    );
    return System.currentTimeMillis();
  }

  /**
   * Wait up to {@link #S3_TIMEOUT_MS maximum time limit} for the connector with the given name to
   * write the specified number of files.
   *
   * @param bucketName the name of the S3 destination bucket
   * @param numFiles   the number of files expected
   * @return the time this method discovered the connector has written the files
   * @throws InterruptedException if this was interrupted
   */
  protected long waitForFilesInBucket(String bucketName, int numFiles) throws InterruptedException {
    TestUtils.waitForCondition(
        () -> assertFileCountInBucket(bucketName, numFiles).orElse(false),
        S3_TIMEOUT_MS,
        "Files not written to S3 bucket in time."
    );
    return System.currentTimeMillis();
  }

  /**
   * Confirm that a connector with an exact number of tasks is running.
   *
   * @param connectorName the connector
   * @param numTasks      the minimum number of tasks
   * @return true if the connector and tasks are in RUNNING state; false otherwise
   */
  protected Optional<Boolean> assertConnectorAndTasksRunning(String connectorName, int numTasks) {
    try {
      ConnectorStateInfo info = connect.connectorStatus(connectorName);
      boolean result = info != null
          && info.tasks().size() >= numTasks
          && info.connector().state().equals(AbstractStatus.State.RUNNING.toString())
          && info.tasks().stream()
          .allMatch(s -> s.state().equals(AbstractStatus.State.RUNNING.toString()));
      return Optional.of(result);
    } catch (Exception e) {
      log.warn("Could not check connector state info.");
      return Optional.empty();
    }
  }

  /**
   * Confirm that the file count in a bucket matches the expected number of files.
   *
   * @param bucketName the name of the bucket containing the files
   * @param numFiles   the number of files expected
   * @return true if the number of files in the bucket match the expected number; false otherwise
   */
  protected Optional<Boolean> assertFileCountInBucket(String bucketName, int numFiles) {
    try {
      int fileCount = S3Client.listObjectsV2(bucketName).getKeyCount();
      boolean result = fileCount == numFiles;
      return Optional.of(result);
    } catch (Exception e) {
      log.warn("Could not check file count in bucket: {}", bucketName);
      return Optional.empty();
    }
  }

  /**
   * Clear the given S3 bucket. Removes the contents, keeps the bucket.
   *
   * @param bucketName the name of the bucket to clear.
   */
  protected void clearBucket(String bucketName) {
    for (S3ObjectSummary file : S3Client.listObjectsV2(bucketName).getObjectSummaries()) {
      S3Client.deleteObject(bucketName, file.getKey());
    }
  }

  /**
   * Check if the file names in the bucket have the expected namings.
   *
   * @param bucketName        the name of the bucket with the files
   * @param expectedTopic     the expected topic in the file path and file name
   * @param expectedPartition the expected partition number in the file path and name
   * @param expectedExtension the expected extensions of the files
   * @return whether all the files in the bucket match the expected values
   */
  protected boolean fileNamesValid(String bucketName, String expectedTopic, int expectedPartition,
      String expectedExtension) {
    for (S3ObjectSummary file : S3Client.listObjectsV2(bucketName).getObjectSummaries()) {
      S3FileInfo fileInfo = new S3FileInfo(file.getKey());
      if (!fileInfo.namingIsValid()
          || !fileInfo.filenameTopic.equals(expectedTopic)
          || fileInfo.directoryPartition != expectedPartition
          || !fileInfo.extension.equals(expectedExtension)
      ) {
        return false;
      }
    }
    return true;
  }

  /**
   * Check the contents of the files in the S3 bucket compared to the expected row.
   *
   * @param bucketName          the name of the s3 test bucket
   * @param expectedRowsPerFile the number of rows a file should have
   * @param expectedRow         the expected row data in each file
   * @return whether every row of the files read equals the expected row
   */
  protected boolean fileContentsAsExpected(String bucketName, int expectedRowsPerFile,
      Struct expectedRow) throws IOException {
    log.info("expectedRow: {}", expectedRow);
    for (S3ObjectSummary file : S3Client.listObjectsV2(bucketName).getObjectSummaries()) {
      String destinationPath = TEST_DOWNLOAD_PATH + file.getKey();
      File downloadedFile = new File(destinationPath);
      log.info("Saving file to : {}", destinationPath);
      S3Client.getObject(new GetObjectRequest(bucketName, file.getKey()), downloadedFile);

      List<JsonNode> downloadedFileContents = getFileContents(destinationPath);
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
  private boolean fileContentsMatchExpected(List<JsonNode> fileContents, int expectedRowsPerFile,
      Struct expectedRow) {
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
   * Get the contents of a downloaded file from S3 based on its file extension.
   *
   * @param filePath the path of the file to read
   * @return the rows of the file as JsonNodes
   */
  private List<JsonNode> getFileContents(String filePath) throws IOException {
    if (filePath.endsWith(".avro")) {
      return getContentsFromAvro(filePath);
    } else if (filePath.endsWith(".parquet")) {
      return getContentsFromParquet(filePath);
    } else if (filePath.endsWith(".json")) {
      return getContentsFromJson(filePath);
    } else {
      throw new InvalidFileTypeException(
          String.format("Downloaded file has unsupported extension: %s", filePath)
      );
    }
  }

  /**
   * Get the contents of an AVRO file at a given filepath.
   *
   * @param filePath the path of the downloaded file
   * @return the rows of the file as JsonNodes
   */
  private List<JsonNode> getContentsFromAvro(String filePath) throws IOException {
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader(new File(filePath),
        datumReader);
    List<JsonNode> fileRows = new ArrayList<>();
    while (dataFileReader.hasNext()) {
      GenericRecord row = dataFileReader.next();
      log.debug("Avro row: {}", row);
      JsonNode jsonNode = jsonMapper.readTree(row.toString());
      fileRows.add(jsonNode);
    }
    return fileRows;
  }

  /**
   * Get the contents of a parquet file at a given filepath.
   *
   * @param filePath the path of the downloaded parquet file
   * @return the rows of the file as JsonNodes
   */
  private List<JsonNode> getContentsFromParquet(String filePath) throws IOException {
    ParquetReader<SimpleRecord> reader = ParquetReader
        .builder(new SimpleReadSupport(), new Path(filePath)).build();
    ParquetMetadata metadata = ParquetFileReader
        .readFooter(new Configuration(), new Path(filePath));
    JsonRecordFormatter.JsonGroupFormatter formatter = JsonRecordFormatter
        .fromSchema(metadata.getFileMetaData().getSchema());
    List<JsonNode> fileRows = new ArrayList<>();
    for (SimpleRecord value = reader.read(); value != null; value = reader.read()) {
      log.debug("Parquet row: {}", formatter.formatRecord(value));
      JsonNode jsonNode = jsonMapper.readTree(formatter.formatRecord(value));
      fileRows.add(jsonNode);
    }
    return fileRows;
  }

  /**
   * Get the contents of a json file at a given filepath.
   *
   * @param filePath the path of the downloaded json file
   * @return the rows of the file as JsonNodes
   */
  private List<JsonNode> getContentsFromJson(String filePath) throws IOException {
    FileReader fileReader = new FileReader(new File(filePath));
    BufferedReader bufferedReader = new BufferedReader(fileReader);
    List<JsonNode> fileRows = new ArrayList<>();
    String line;
    while ((line = bufferedReader.readLine()) != null) {
      fileRows.add(jsonMapper.readTree(line));
    }
    return fileRows;
  }

  /**
   * A utility class to keep tack of the details about a file in S3.
   * <p>
   * Parse an S3 key file into member fields. ex.: /topics/s3_topic/partition=97/s3_topic+97+0000000001.avro
   */
  class S3FileInfo {

    /* The topic name parsed from the file path */
    String directoryTopic;
    /* The topic name parsed from the file name */
    String filenameTopic;
    /* The extension parsed from the file name */
    String extension;
    /* The partition number parsed from the path */
    int directoryPartition;
    /* The partition number parsed from the file name */
    int filenamePartition;
    /* The offset parsed from the file name */
    long offset;

    public S3FileInfo(String S3FileKey) {
      String[] path = S3FileKey.trim().split("/");
      directoryTopic = path[1];
      directoryPartition = Integer.parseInt(path[2].split("=")[1]);
      String fileName = path[3];
      String[] tokens = fileName.split("[+.]");
      filenameTopic = tokens[0];
      filenamePartition = Integer.parseInt(tokens[1]);
      offset = Long.parseLong(tokens[2]);
      extension = tokens[tokens.length - 1]; //parquets have .snappy.parquet, use last token
    }

    /**
     * Check whether the file name is consistent with the namings in the path.
     */
    public boolean namingIsValid() {
      return directoryTopic.equals(filenameTopic) && directoryPartition == filenamePartition;
    }
  }
}
