package io.confluent.connect.s3;

import com.amazonaws.services.s3.AmazonS3;
import io.confluent.connect.s3.format.bytearray.ByteArrayFormat;
import io.confluent.connect.s3.util.EmbeddedConnectUtils;
import io.confluent.connect.s3.util.S3Utils;
import io.confluent.connect.storage.StorageSinkConnectorConfig;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.junit.After;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.put;

public class S3SinkConnectorFaultyS3Test extends TestWithMockedFaultyS3 {
    protected static final int MAX_TASKS = 1;
    protected static final int FLUSH_SIZE = 3;
    protected static final int TOPIC_PARTITIONS = 2;

    protected static final String CONNECTOR_NAME = "s3-sink";
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
        EmbeddedConnectUtils.waitForConnectorToStart(connect, CONNECTOR_NAME, Math.min(TOPIC_PARTITIONS, MAX_TASKS));
    }

    // TODO: add test for different S3 places: when initiating multipart upload, when uploading part and when completing the upload
    // TODO: and also: depending on part.size, exceptions may be thrown from write() or from commit()

    @Test
    public void test4xxErrorDuringPartUploadIsRetriedByConnectFramework() throws Exception {
        localProps.put(S3SinkConnectorConfig.FORMAT_CLASS_CONFIG, ByteArrayFormat.class.getName());
        localProps.put(SinkConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, ByteArrayConverter.class.getName());

        localProps.put(S3SinkConnectorConfig.S3_PART_RETRIES_CONFIG, "0"); // disable AWS SDK retries
        localProps.put(StorageSinkConnectorConfig.RETRY_BACKOFF_CONFIG, "1000"); // lower Connect Framework retry backoff

        setUp();

        // inject S3 failure for UploadPart request
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

        S3Utils.waitForFilesInBucket(s3, S3_TEST_BUCKET_NAME, 1, S3_TIMEOUT_MS);
    }

    @Test
    public void test4xxErrorDuringCompleteMultipartUploadIsRetriedByConnectFramework() throws Exception {
        localProps.put(S3SinkConnectorConfig.FORMAT_CLASS_CONFIG, ByteArrayFormat.class.getName());
        localProps.put(SinkConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, ByteArrayConverter.class.getName());

        localProps.put(S3SinkConnectorConfig.S3_PART_RETRIES_CONFIG, "0"); // disable AWS SDK retries
        localProps.put(StorageSinkConnectorConfig.RETRY_BACKOFF_CONFIG, "1000"); // lower Connect Framework retry backoff

        setUp();

        // inject S3 failure for CompleteMultipartUpload request
        injectS3FailureFor(post(anyUrl())
                .withQueryParam("uploadId", matching(".*"))
                .willReturn(
                        aResponse().withStatus(429)  // "too many requests"
                )
        );

        connect.kafka().produce(TOPIC, 0, null, "Message1");
        connect.kafka().produce(TOPIC, 0, null, "Message2");
        connect.kafka().produce(TOPIC, 0, null, "Message3");

        S3Utils.waitForFilesInBucket(s3, S3_TEST_BUCKET_NAME, 1, S3_TIMEOUT_MS);
    }
}
