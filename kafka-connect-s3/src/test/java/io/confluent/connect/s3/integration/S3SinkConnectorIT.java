/*
 * Copyright [2020 - 2020] Confluent Inc.
 */

package io.confluent.connect.s3.integration;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Region;
import io.confluent.common.utils.IntegrationTest;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.storage.StringConverter;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.runtime.ConnectorConfig.*;
import static org.junit.Assert.assertEquals;

@Category(IntegrationTest.class)
public class S3SinkConnectorIT extends BaseConnectorIT {

  private static final Logger log = LoggerFactory.getLogger(S3SinkConnectorIT.class);

  private static final String CONNECTOR_NAME = "s3-sink-connector";
  private static final long NUM_RECORDS_PRODUCED = 10;
  private static final int TASKS_MAX = 1;
  private static final List<String> KAFKA_TOPICS = Arrays.asList("kafka1");
  private static final int FLUSH_SIZE = 1;

  @ClassRule
  public static DockerComposeContainer compose =
    new DockerComposeContainer(
      new File("src/test/docker/docker-compose.yml"));

  @Before
  public void setup() throws IOException {
    startConnect();
    createS3Client();
    s3.createBucket(S3_BUCKET);
  }

  private void createS3Client() {
    AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard()
      .withPathStyleAccessEnabled(true)
      .withEndpointConfiguration(
        new AwsClientBuilder.EndpointConfiguration(
          "http://localhost:9090",
          Region.US_East_2.toString()));
    s3 = builder.build();
    System.out.println(S3_MOCK_RULE.getHttpPort());
  }

  @After
  public void close() {
    stopConnect();
  }

  @Test
  public void testSink() throws Throwable {

    // create topics in Kafka
    KAFKA_TOPICS.forEach(topic -> connect.kafka().createTopic(topic, 1));

    // setup up props for the sink connector
    Map<String, String> props = getProperties();

    // Send records to Kafka
    for (int i = 0; i < NUM_RECORDS_PRODUCED; i++) {
      String kafkaTopic = KAFKA_TOPICS.get(i % KAFKA_TOPICS.size());
      String kafkaKey = "simple-key-" + i;
      String kafkaValue = "simple-message-" + i;
      log.debug("Sending message {} with topic {} to Kafka broker {}", kafkaTopic, kafkaValue);
      connect.kafka().produce(kafkaTopic, kafkaKey, kafkaValue);
    }

    // start a sink connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    int minimumNumTasks = Math.min(KAFKA_TOPICS.size(), TASKS_MAX);

    waitForConnectorToStart(CONNECTOR_NAME, minimumNumTasks);
    waitForConnectorToCompleteSendingRecords(NUM_RECORDS_PRODUCED, FLUSH_SIZE);

    assertEquals(getNoOfObjectsInS3(), NUM_RECORDS_PRODUCED/FLUSH_SIZE);
  }

  private Map<String, String> getProperties() {
    Map<String, String> props = new HashMap<>();
    props.put(SinkConnectorConfig.TOPICS_CONFIG, String.join(",", KAFKA_TOPICS));
    props.put(CONNECTOR_CLASS_CONFIG, "io.confluent.connect.s3.S3SinkConnector");
    props.put(TASKS_MAX_CONFIG, Integer.toString(TASKS_MAX));

    props.put("s3.region", "us-east-2");
    props.put("s3.part.size", "5242880");
    props.put("s3.bucket.name", S3_BUCKET);
    props.put("flush.size", Integer.toString(FLUSH_SIZE));
    props.put("storage.class","io.confluent.connect.s3.storage.S3Storage");
    props.put("partitioner.class", "io.confluent.connect.storage.partitioner.DefaultPartitioner");
    props.put("store.url", "http://localhost:" + S3_MOCK_RULE.getHttpPort());
    // converters
    props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());

    props.put("format.class",JSON_FORMAT_CLASS);
    // license properties
    return props;
  }
}
