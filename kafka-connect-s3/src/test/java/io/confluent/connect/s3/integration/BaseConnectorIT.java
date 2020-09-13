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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.common.utils.IntegrationTest;
import io.findify.s3mock.S3Mock;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.ConnectException;
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
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(IntegrationTest.class)
public abstract class BaseConnectorIT {

  protected static final String CONNECTOR_CLASS_NAME = "S3SinkConnector";
  protected static final int MAX_TASKS = 3;

  private static final Logger log = LoggerFactory.getLogger(BaseConnectorIT.class);
  private static final long CONNECTOR_STARTUP_DURATION_MS = TimeUnit.MINUTES.toMillis(1);
  private static final long S3_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(30);

  protected static AmazonS3 S3Client;
  protected EmbeddedConnectCluster connect;
  protected Map<String, String> props;

  @Before
  public void setup() {
    startConnect();
    setupProperties();
  }

  @After
  public void close() {
    // stop all Connect, Kafka and Zk threads.
    connect.stop();
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
   * @param expectedNumFiles the number of files expected
   * @return true if the number of files in the bucket match the expected number; false otherwise
   */
  protected Optional<Boolean> assertFileCountInBucket(String bucketName, int expectedNumFiles) {
    try {
      int fileCount = S3Client.listObjectsV2(bucketName).getKeyCount();
      return Optional.of(fileCount == expectedNumFiles);
    } catch (Exception e) {
      log.warn("Could not check file count in bucket: {}", bucketName);
      return Optional.empty();
    }
  }
}
