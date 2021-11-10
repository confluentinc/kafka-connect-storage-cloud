package io.confluent.connect.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import io.confluent.connect.s3.format.bytearray.ByteArrayFormat;
import io.confluent.connect.storage.StorageSinkConnectorConfig;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.put;

public class S3SinkConnectorFaultyS3Test extends TestWithMockedFaultyS3 {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3SinkConnectorFaultyS3Test.class);

    protected static final int MAX_TASKS = 1;
    protected static final int FLUSH_SIZE = 3;
    protected static final int TOPIC_PARTITIONS = 2;

    protected static final String CONNECTOR_NAME = "s3-sink";
    protected static final long CONNECTOR_STARTUP_DURATION_MS = TimeUnit.MINUTES.toMillis(1);
    protected static final long S3_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(60);

    protected Map<String, String> localProps = new HashMap<>();
    protected EmbeddedConnectCluster connect;
    protected AmazonS3 s3;

    @Override
    protected Map<String, String> createProps() {
        Map<String, String> props = super.createProps();

        props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, S3SinkConnector.class.getName());
        props.put(ConnectorConfig.TASKS_MAX_CONFIG, Integer.toString(MAX_TASKS));
        props.put(StorageSinkConnectorConfig.FLUSH_SIZE_CONFIG, Integer.toString(FLUSH_SIZE));

        // since S3 is mocked, credentials don't matter
        props.put(S3SinkConnectorConfig.AWS_ACCESS_KEY_ID_CONFIG, "12345");
        props.put(S3SinkConnectorConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "12345");

        props.put(SinkConnectorConfig.TOPICS_CONFIG, TOPIC);

        // add per-test overrides
        props.putAll(localProps);

        return props;
    }

    //@Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        s3 = newS3Client(connectorConfig);
        s3.createBucket(S3_TEST_BUCKET_NAME);
        startConnect();
    }

    @After
    public void tearDown() throws Exception {
        connect.stop();
        localProps.clear();
        super.tearDown();
    }

    protected void startConnect() throws InterruptedException {
        connect = new EmbeddedConnectCluster.Builder()
                .name("s3-connect-cluster")
                .build();
        connect.start();

        connect.kafka().createTopic(TOPIC, TOPIC_PARTITIONS);

        connect.configureConnector(CONNECTOR_NAME, properties);
        waitForConnectorToStart(CONNECTOR_NAME, Math.min(TOPIC_PARTITIONS, MAX_TASKS));
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
            LOGGER.warn("Could not check connector state info.");
            return Optional.empty();
        }
    }

    /**
     * Wait up to {@link #S3_TIMEOUT_MS maximum time limit} for the connector to write the specified
     * number of files.
     *
     * @param bucketName  S3 bucket name
     * @param numFiles    expected number of files in the bucket
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
     * Confirm that the file count in a bucket matches the expected number of files.
     *
     * @param bucketName the name of the bucket containing the files
     * @param expectedNumFiles the number of files expected
     * @return true if the number of files in the bucket match the expected number; false otherwise
     */
    protected Optional<Boolean> assertFileCountInBucket(String bucketName, int expectedNumFiles) {
        try {
            return Optional.of(getBucketFileCount(bucketName) == expectedNumFiles);
        } catch (Exception e) {
            LOGGER.warn("Could not check file count in bucket: {}", bucketName);
            return Optional.empty();
        }
    }

    /**
     * Recursively query the bucket to get the total number of files that exist in the bucket.
     *
     * @param bucketName the name of the bucket containing the files.
     * @return the number of files in the bucket
     */
    private int getBucketFileCount(String bucketName) {
        int totalFilesInBucket = 0;
        ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(bucketName);
        ListObjectsV2Result result;
        do {
       /*
       Need the result object to extract the continuation token from the request as each request
       to listObjectsV2() returns a maximum of 1000 files.
       */
            result = s3.listObjectsV2(request);
            totalFilesInBucket += result.getKeyCount();
            String token = result.getNextContinuationToken();
            // To get the next batch of files.
            request.setContinuationToken(token);
        } while(result.isTruncated());
        return totalFilesInBucket;
    }

    // TODO: add test for different S3 places: when initiating multipart upload, when uploading part and when completing the upload
    // TODO: and also: depending on part.size, exceptions may be thrown from write() or from commit()

    @Test
    public void test4xxErrorIsRetriedByConnectFramework() throws Exception {
        localProps.put(S3SinkConnectorConfig.FORMAT_CLASS_CONFIG, ByteArrayFormat.class.getName());
        localProps.put(SinkConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, ByteArrayConverter.class.getName());

        localProps.put(S3SinkConnectorConfig.S3_PART_RETRIES_CONFIG, "0"); // disable AWS SDK retries
        localProps.put(StorageSinkConnectorConfig.RETRY_BACKOFF_CONFIG, "1000"); // lower Connect Framework retry backoff

        setUp();

        injectS3FailureFor(put(anyUrl())
                .withQueryParam("partNumber", matching(".*"))
                .withQueryParam("uploadId", matching(".*"))
                .willReturn(
                        aResponse().withStatus(429)  // "too many requests"
                )
        );

        connect.kafka().produce(TOPIC, 0, null, "Message1");
        connect.kafka().produce(TOPIC, 0, null, "Message2");
        connect.kafka().produce(TOPIC, 0, null, "Message3");

        waitForFilesInBucket(S3_TEST_BUCKET_NAME, 1);
    }
}
