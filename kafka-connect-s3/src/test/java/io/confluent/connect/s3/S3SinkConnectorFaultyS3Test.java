package io.confluent.connect.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.http.Fault;
import io.confluent.connect.s3.format.avro.AvroFormat;
import io.confluent.connect.s3.format.bytearray.ByteArrayFormat;
import io.confluent.connect.s3.format.json.JsonFormat;
import io.confluent.connect.s3.format.parquet.ParquetFormat;
import io.confluent.connect.s3.util.EmbeddedConnectUtils;
import io.confluent.connect.s3.util.S3Utils;
import io.confluent.connect.storage.StorageSinkConnectorConfig;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.put;

@RunWith(Parameterized.class)
public class S3SinkConnectorFaultyS3Test extends TestWithMockedFaultyS3 {
    protected static final int MAX_TASKS = 1;
    protected static final int PART_SIZE = 5 * 1024 * 1024;
    protected static final int TOPIC_PARTITIONS = 2;

    protected static final String CONNECTOR_NAME = "s3-sink-";
    protected static final long S3_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(60);

    protected static final String[] TEST_MESSAGES = generateTestMessages(70); // ~ 7 MB of data
    protected static final int FLUSH_SIZE_SMALL = 30; // ~ 3 MB (less than PART_SIZE, trigger single part upload - during commit)
    protected static final int FLUSH_SIZE_BIG = 70; // ~ 7 MB (more than PART_SIZE, trigger two part uploads - during write and commit)
    protected static final int FLUSH_SIZE_HUGE = 1400; // ~ 140 MB (more than 128 MB (default block size for parquet), trigger two part uploads for parquet)

    protected static EmbeddedConnectCluster connect;
    protected static Admin kafkaAdmin;
    protected String connectorName;
    protected String topicName;
    protected AmazonS3 s3;

    // test parameters
    private final Failure failure;
    private final Class formatClass;
    private final Class converterClass;
    private final int flushSize;

    public S3SinkConnectorFaultyS3Test(
            Class formatClass,
            Class converterClass,
            Failure failure,
            int flushSize
    ) {
        this.failure = failure;
        this.formatClass = formatClass;
        this.converterClass = converterClass;
        this.flushSize = flushSize;
    }

    @Override
    protected Map<String, String> createProps() {
        Map<String, String> props = super.createProps();

        props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, S3SinkConnector.class.getName());
        props.put(ConnectorConfig.TASKS_MAX_CONFIG, Integer.toString(MAX_TASKS));

        props.put(S3SinkConnectorConfig.FORMAT_CLASS_CONFIG, formatClass.getName());
        props.put(SinkConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, converterClass.getName());

        props.put(StorageSinkConnectorConfig.PARQUET_CODEC_CONFIG, "none"); // disable compressor to lower CPU usage for parquet tests
        props.put(S3SinkConnectorConfig.S3_PART_RETRIES_CONFIG, "0"); // disable AWS SDK retries
        props.put(StorageSinkConnectorConfig.RETRY_BACKOFF_CONFIG, "100"); // lower Connect Framework retry backoff

        // If flushSize > PART_SIZE, then first uploadPart() is called from S3OutputStream::write() method.
        // If flushSize < PART_SIZE, then uploadPart() is called only from S3OutputStream::commit() method.
        props.put(S3SinkConnectorConfig.PART_SIZE_CONFIG, Integer.toString(PART_SIZE));
        props.put(StorageSinkConnectorConfig.FLUSH_SIZE_CONFIG, Integer.toString(flushSize));

        // since S3 is mocked, credentials don't matter
        props.put(S3SinkConnectorConfig.AWS_ACCESS_KEY_ID_CONFIG, "12345");
        props.put(S3SinkConnectorConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "12345");

        props.put(SinkConnectorConfig.TOPICS_CONFIG, topicName);

        return props;
    }

    @Before
    @Override
    public void setUp() throws Exception {
        topicName = TOPIC + "-" + UUID.randomUUID();
        connect.kafka().createTopic(topicName, TOPIC_PARTITIONS);

        super.setUp();

        s3 = newS3Client(connectorConfig);
        s3.createBucket(S3_TEST_BUCKET_NAME);

        connectorName = CONNECTOR_NAME + UUID.randomUUID();
        connect.configureConnector(connectorName, properties);
        EmbeddedConnectUtils.waitForConnectorToStart(connect, connectorName, Math.min(TOPIC_PARTITIONS, MAX_TASKS));
    }

    @After
    public void tearDown() throws Exception {
        connect.deleteConnector(connectorName);
        kafkaAdmin.deleteTopics(Collections.singleton(topicName));

        super.tearDown();
    }

    @BeforeClass
    public static void startConnect() {
        connect = new EmbeddedConnectCluster.Builder()
                .name("s3-connect-cluster")
                .build();
        connect.start();
        kafkaAdmin = connect.kafka().createAdminClient();
    }

    @AfterClass
    public static void stopConnect() {
        connect.stop();
    }

    @Parameterized.Parameters
    public static Collection<Object[]> tests() {
        return Arrays.asList(new Object[][]{
                // tests for error 429 Too Many Requests
                {ByteArrayFormat.class, ByteArrayConverter.class, Failures.FAIL_CREATE_MULTIPART_UPLOAD_REQUEST_WITH_ERROR_429, FLUSH_SIZE_SMALL},
                {ByteArrayFormat.class, ByteArrayConverter.class, Failures.FAIL_UPLOAD_PART_REQUEST_WITH_ERROR_429, FLUSH_SIZE_SMALL},
                {ByteArrayFormat.class, ByteArrayConverter.class, Failures.FAIL_COMPLETE_MULTIPART_UPLOAD_REQUEST_WITH_ERROR_429, FLUSH_SIZE_SMALL},

                {ByteArrayFormat.class, ByteArrayConverter.class, Failures.FAIL_CREATE_MULTIPART_UPLOAD_REQUEST_WITH_ERROR_429, FLUSH_SIZE_BIG},
                {ByteArrayFormat.class, ByteArrayConverter.class, Failures.FAIL_UPLOAD_PART_REQUEST_WITH_ERROR_429, FLUSH_SIZE_BIG},
                {ByteArrayFormat.class, ByteArrayConverter.class, Failures.FAIL_COMPLETE_MULTIPART_UPLOAD_REQUEST_WITH_ERROR_429, FLUSH_SIZE_BIG},

                {JsonFormat.class, ByteArrayConverter.class, Failures.FAIL_CREATE_MULTIPART_UPLOAD_REQUEST_WITH_ERROR_429, FLUSH_SIZE_SMALL},
                {JsonFormat.class, ByteArrayConverter.class, Failures.FAIL_UPLOAD_PART_REQUEST_WITH_ERROR_429, FLUSH_SIZE_SMALL},
                {JsonFormat.class, ByteArrayConverter.class, Failures.FAIL_COMPLETE_MULTIPART_UPLOAD_REQUEST_WITH_ERROR_429, FLUSH_SIZE_SMALL},

                {JsonFormat.class, ByteArrayConverter.class, Failures.FAIL_CREATE_MULTIPART_UPLOAD_REQUEST_WITH_ERROR_429, FLUSH_SIZE_BIG},
                {JsonFormat.class, ByteArrayConverter.class, Failures.FAIL_UPLOAD_PART_REQUEST_WITH_ERROR_429, FLUSH_SIZE_BIG},
                {JsonFormat.class, ByteArrayConverter.class, Failures.FAIL_COMPLETE_MULTIPART_UPLOAD_REQUEST_WITH_ERROR_429, FLUSH_SIZE_BIG},

                {AvroFormat.class, ByteArrayConverter.class, Failures.FAIL_CREATE_MULTIPART_UPLOAD_REQUEST_WITH_ERROR_429, FLUSH_SIZE_SMALL},
                {AvroFormat.class, ByteArrayConverter.class, Failures.FAIL_UPLOAD_PART_REQUEST_WITH_ERROR_429, FLUSH_SIZE_SMALL},
                {AvroFormat.class, ByteArrayConverter.class, Failures.FAIL_COMPLETE_MULTIPART_UPLOAD_REQUEST_WITH_ERROR_429, FLUSH_SIZE_SMALL},

                {AvroFormat.class, ByteArrayConverter.class, Failures.FAIL_CREATE_MULTIPART_UPLOAD_REQUEST_WITH_ERROR_429, FLUSH_SIZE_BIG},
                {AvroFormat.class, ByteArrayConverter.class, Failures.FAIL_UPLOAD_PART_REQUEST_WITH_ERROR_429, FLUSH_SIZE_BIG},
                {AvroFormat.class, ByteArrayConverter.class, Failures.FAIL_COMPLETE_MULTIPART_UPLOAD_REQUEST_WITH_ERROR_429, FLUSH_SIZE_BIG},

                {ParquetFormat.class, JsonConverter.class, Failures.FAIL_CREATE_MULTIPART_UPLOAD_REQUEST_WITH_ERROR_429, FLUSH_SIZE_SMALL},
                {ParquetFormat.class, JsonConverter.class, Failures.FAIL_UPLOAD_PART_REQUEST_WITH_ERROR_429, FLUSH_SIZE_SMALL},
                {ParquetFormat.class, JsonConverter.class, Failures.FAIL_COMPLETE_MULTIPART_UPLOAD_REQUEST_WITH_ERROR_429, FLUSH_SIZE_SMALL},

                {ParquetFormat.class, JsonConverter.class, Failures.FAIL_CREATE_MULTIPART_UPLOAD_REQUEST_WITH_ERROR_429, FLUSH_SIZE_HUGE},
                {ParquetFormat.class, JsonConverter.class, Failures.FAIL_UPLOAD_PART_REQUEST_WITH_ERROR_429, FLUSH_SIZE_HUGE},
                {ParquetFormat.class, JsonConverter.class, Failures.FAIL_COMPLETE_MULTIPART_UPLOAD_REQUEST_WITH_ERROR_429, FLUSH_SIZE_BIG},

                // tests for error 500 Server Error
                {ByteArrayFormat.class, ByteArrayConverter.class, Failures.FAIL_CREATE_MULTIPART_UPLOAD_REQUEST_WITH_ERROR_500, FLUSH_SIZE_SMALL},
                {ByteArrayFormat.class, ByteArrayConverter.class, Failures.FAIL_UPLOAD_PART_REQUEST_WITH_ERROR_500, FLUSH_SIZE_SMALL},
                {ByteArrayFormat.class, ByteArrayConverter.class, Failures.FAIL_COMPLETE_MULTIPART_UPLOAD_REQUEST_WITH_ERROR_500, FLUSH_SIZE_SMALL},

                {ByteArrayFormat.class, ByteArrayConverter.class, Failures.FAIL_CREATE_MULTIPART_UPLOAD_REQUEST_WITH_ERROR_500, FLUSH_SIZE_BIG},
                {ByteArrayFormat.class, ByteArrayConverter.class, Failures.FAIL_UPLOAD_PART_REQUEST_WITH_ERROR_500, FLUSH_SIZE_BIG},
                {ByteArrayFormat.class, ByteArrayConverter.class, Failures.FAIL_COMPLETE_MULTIPART_UPLOAD_REQUEST_WITH_ERROR_500, FLUSH_SIZE_BIG},

                {JsonFormat.class, ByteArrayConverter.class, Failures.FAIL_CREATE_MULTIPART_UPLOAD_REQUEST_WITH_ERROR_500, FLUSH_SIZE_SMALL},
                {JsonFormat.class, ByteArrayConverter.class, Failures.FAIL_UPLOAD_PART_REQUEST_WITH_ERROR_500, FLUSH_SIZE_SMALL},
                {JsonFormat.class, ByteArrayConverter.class, Failures.FAIL_COMPLETE_MULTIPART_UPLOAD_REQUEST_WITH_ERROR_500, FLUSH_SIZE_SMALL},

                {JsonFormat.class, ByteArrayConverter.class, Failures.FAIL_CREATE_MULTIPART_UPLOAD_REQUEST_WITH_ERROR_500, FLUSH_SIZE_BIG},
                {JsonFormat.class, ByteArrayConverter.class, Failures.FAIL_UPLOAD_PART_REQUEST_WITH_ERROR_500, FLUSH_SIZE_BIG},
                {JsonFormat.class, ByteArrayConverter.class, Failures.FAIL_COMPLETE_MULTIPART_UPLOAD_REQUEST_WITH_ERROR_500, FLUSH_SIZE_BIG},

                {AvroFormat.class, ByteArrayConverter.class, Failures.FAIL_CREATE_MULTIPART_UPLOAD_REQUEST_WITH_ERROR_500, FLUSH_SIZE_SMALL},
                {AvroFormat.class, ByteArrayConverter.class, Failures.FAIL_UPLOAD_PART_REQUEST_WITH_ERROR_500, FLUSH_SIZE_SMALL},
                {AvroFormat.class, ByteArrayConverter.class, Failures.FAIL_COMPLETE_MULTIPART_UPLOAD_REQUEST_WITH_ERROR_500, FLUSH_SIZE_SMALL},

                {AvroFormat.class, ByteArrayConverter.class, Failures.FAIL_CREATE_MULTIPART_UPLOAD_REQUEST_WITH_ERROR_500, FLUSH_SIZE_BIG},
                {AvroFormat.class, ByteArrayConverter.class, Failures.FAIL_UPLOAD_PART_REQUEST_WITH_ERROR_500, FLUSH_SIZE_BIG},
                {AvroFormat.class, ByteArrayConverter.class, Failures.FAIL_COMPLETE_MULTIPART_UPLOAD_REQUEST_WITH_ERROR_500, FLUSH_SIZE_BIG},

                {ParquetFormat.class, JsonConverter.class, Failures.FAIL_CREATE_MULTIPART_UPLOAD_REQUEST_WITH_ERROR_500, FLUSH_SIZE_SMALL},
                {ParquetFormat.class, JsonConverter.class, Failures.FAIL_UPLOAD_PART_REQUEST_WITH_ERROR_500, FLUSH_SIZE_SMALL},
                {ParquetFormat.class, JsonConverter.class, Failures.FAIL_COMPLETE_MULTIPART_UPLOAD_REQUEST_WITH_ERROR_500, FLUSH_SIZE_SMALL},

                {ParquetFormat.class, JsonConverter.class, Failures.FAIL_CREATE_MULTIPART_UPLOAD_REQUEST_WITH_ERROR_500, FLUSH_SIZE_HUGE},
                {ParquetFormat.class, JsonConverter.class, Failures.FAIL_UPLOAD_PART_REQUEST_WITH_ERROR_500, FLUSH_SIZE_HUGE},
                {ParquetFormat.class, JsonConverter.class, Failures.FAIL_COMPLETE_MULTIPART_UPLOAD_REQUEST_WITH_ERROR_500, FLUSH_SIZE_BIG},

                // tests for error Connection Reset by Peer
                {ByteArrayFormat.class, ByteArrayConverter.class, Failures.FAIL_CREATE_MULTIPART_UPLOAD_REQUEST_WITH_RESET, FLUSH_SIZE_SMALL},
                {ByteArrayFormat.class, ByteArrayConverter.class, Failures.FAIL_UPLOAD_PART_REQUEST_WITH_RESET, FLUSH_SIZE_SMALL},
                {ByteArrayFormat.class, ByteArrayConverter.class, Failures.FAIL_COMPLETE_MULTIPART_UPLOAD_REQUEST_WITH_RESET, FLUSH_SIZE_SMALL},

                {ByteArrayFormat.class, ByteArrayConverter.class, Failures.FAIL_CREATE_MULTIPART_UPLOAD_REQUEST_WITH_RESET, FLUSH_SIZE_BIG},
                {ByteArrayFormat.class, ByteArrayConverter.class, Failures.FAIL_UPLOAD_PART_REQUEST_WITH_RESET, FLUSH_SIZE_BIG},
                {ByteArrayFormat.class, ByteArrayConverter.class, Failures.FAIL_COMPLETE_MULTIPART_UPLOAD_REQUEST_WITH_RESET, FLUSH_SIZE_BIG},

                {JsonFormat.class, ByteArrayConverter.class, Failures.FAIL_CREATE_MULTIPART_UPLOAD_REQUEST_WITH_RESET, FLUSH_SIZE_SMALL},
                {JsonFormat.class, ByteArrayConverter.class, Failures.FAIL_UPLOAD_PART_REQUEST_WITH_RESET, FLUSH_SIZE_SMALL},
                {JsonFormat.class, ByteArrayConverter.class, Failures.FAIL_COMPLETE_MULTIPART_UPLOAD_REQUEST_WITH_RESET, FLUSH_SIZE_SMALL},

                {JsonFormat.class, ByteArrayConverter.class, Failures.FAIL_CREATE_MULTIPART_UPLOAD_REQUEST_WITH_RESET, FLUSH_SIZE_BIG},
                {JsonFormat.class, ByteArrayConverter.class, Failures.FAIL_UPLOAD_PART_REQUEST_WITH_RESET, FLUSH_SIZE_BIG},
                {JsonFormat.class, ByteArrayConverter.class, Failures.FAIL_COMPLETE_MULTIPART_UPLOAD_REQUEST_WITH_RESET, FLUSH_SIZE_BIG},

                {AvroFormat.class, ByteArrayConverter.class, Failures.FAIL_CREATE_MULTIPART_UPLOAD_REQUEST_WITH_RESET, FLUSH_SIZE_SMALL},
                {AvroFormat.class, ByteArrayConverter.class, Failures.FAIL_UPLOAD_PART_REQUEST_WITH_RESET, FLUSH_SIZE_SMALL},
                {AvroFormat.class, ByteArrayConverter.class, Failures.FAIL_COMPLETE_MULTIPART_UPLOAD_REQUEST_WITH_RESET, FLUSH_SIZE_SMALL},

                {AvroFormat.class, ByteArrayConverter.class, Failures.FAIL_CREATE_MULTIPART_UPLOAD_REQUEST_WITH_RESET, FLUSH_SIZE_BIG},
                {AvroFormat.class, ByteArrayConverter.class, Failures.FAIL_UPLOAD_PART_REQUEST_WITH_RESET, FLUSH_SIZE_BIG},
                {AvroFormat.class, ByteArrayConverter.class, Failures.FAIL_COMPLETE_MULTIPART_UPLOAD_REQUEST_WITH_RESET, FLUSH_SIZE_BIG},

                {ParquetFormat.class, JsonConverter.class, Failures.FAIL_CREATE_MULTIPART_UPLOAD_REQUEST_WITH_RESET, FLUSH_SIZE_SMALL},
                {ParquetFormat.class, JsonConverter.class, Failures.FAIL_UPLOAD_PART_REQUEST_WITH_RESET, FLUSH_SIZE_SMALL},
                {ParquetFormat.class, JsonConverter.class, Failures.FAIL_COMPLETE_MULTIPART_UPLOAD_REQUEST_WITH_RESET, FLUSH_SIZE_SMALL},

                {ParquetFormat.class, JsonConverter.class, Failures.FAIL_CREATE_MULTIPART_UPLOAD_REQUEST_WITH_RESET, FLUSH_SIZE_HUGE},
                {ParquetFormat.class, JsonConverter.class, Failures.FAIL_UPLOAD_PART_REQUEST_WITH_RESET, FLUSH_SIZE_HUGE},
                {ParquetFormat.class, JsonConverter.class, Failures.FAIL_COMPLETE_MULTIPART_UPLOAD_REQUEST_WITH_RESET, FLUSH_SIZE_BIG},
        });
    }

    @Test
    public void testErrorIsRetriedByConnectFramework() throws Exception {
        // inject failure
        failure.inject();

        // produce enough messages to generate file commit
        for (int i = 0; i < flushSize; i++) {
            connect.kafka().produce(topicName, 0, null, TEST_MESSAGES[i % TEST_MESSAGES.length]);
        }

        // check that the file is written to S3
        S3Utils.waitForFilesInBucket(s3, S3_TEST_BUCKET_NAME, 1, S3_TIMEOUT_MS);
    }

    interface Failure {
        void inject();
    }

    private static class CreateMultipartUploadRequestFailure implements Failure {
        private final ResponseDefinitionBuilder response;

        public CreateMultipartUploadRequestFailure(ResponseDefinitionBuilder response) {
            this.response = response;
        }

        @Override
        public void inject() {
            injectS3FailureFor(post(anyUrl())
                    .withQueryParam("uploads", matching("$^"))
                    .willReturn(response)
            );
        }
    }

    private static class PartUploadRequestFailure implements Failure {
        private final ResponseDefinitionBuilder response;

        public PartUploadRequestFailure(ResponseDefinitionBuilder response) {
            this.response = response;
        }

        @Override
        public void inject() {
                injectS3FailureFor(put(anyUrl())
                        .withQueryParam("partNumber", matching(".*"))
                        .withQueryParam("uploadId", matching(".*"))
                        .willReturn(response)
                );
        }
    }

    private static class CompleteMultipartUploadRequestFailure implements Failure {
        private final ResponseDefinitionBuilder response;

        public CompleteMultipartUploadRequestFailure(ResponseDefinitionBuilder response) {
            this.response = response;
        }

        @Override
        public void inject() {
                injectS3FailureFor(post(anyUrl())
                        .withQueryParam("uploadId", matching(".*"))
                        .willReturn(response)
                );
        }
    }

    enum Failures implements Failure {
        FAIL_CREATE_MULTIPART_UPLOAD_REQUEST_WITH_ERROR_429(new CreateMultipartUploadRequestFailure(aResponse().withStatus(429))),
        FAIL_UPLOAD_PART_REQUEST_WITH_ERROR_429(new PartUploadRequestFailure(aResponse().withStatus(429))),
        FAIL_COMPLETE_MULTIPART_UPLOAD_REQUEST_WITH_ERROR_429(new CompleteMultipartUploadRequestFailure(aResponse().withStatus(429))),

        FAIL_CREATE_MULTIPART_UPLOAD_REQUEST_WITH_ERROR_500(new CreateMultipartUploadRequestFailure(aResponse().withStatus(500))),
        FAIL_UPLOAD_PART_REQUEST_WITH_ERROR_500(new PartUploadRequestFailure(aResponse().withStatus(500))),
        FAIL_COMPLETE_MULTIPART_UPLOAD_REQUEST_WITH_ERROR_500(new CompleteMultipartUploadRequestFailure(aResponse().withStatus(500))),

        FAIL_CREATE_MULTIPART_UPLOAD_REQUEST_WITH_RESET(new CreateMultipartUploadRequestFailure(aResponse().withFault(Fault.CONNECTION_RESET_BY_PEER))),
        FAIL_UPLOAD_PART_REQUEST_WITH_RESET(new PartUploadRequestFailure(aResponse().withFault(Fault.CONNECTION_RESET_BY_PEER))),
        FAIL_COMPLETE_MULTIPART_UPLOAD_REQUEST_WITH_RESET(new CompleteMultipartUploadRequestFailure(aResponse().withFault(Fault.CONNECTION_RESET_BY_PEER)));

        private final Failure failure;
        public void inject() {
            failure.inject();
        }

        Failures(Failure failure) {
            this.failure = failure;
        }
    }

    private static String[] generateTestMessages(int count) {
        String[] messages = new String[count];
        for (int i = 0; i < count; i++) {
            messages[i] = generateTestMessage();
        }
        return messages;
    }

    private static String generateTestMessage() {
        return envelopeStringToJsonWithSchema(generateLongString(100 * 1024)); // ~100 KB
    }

    private static String generateLongString(int sizeInCharacters) {
        // random string is needed to trick ParquetFormat compressor,
        // so that resulting file size is big enough to trigger part upload
        return RandomStringUtils.random(sizeInCharacters);
    }

    private static String envelopeStringToJsonWithSchema(String string) {
        ObjectMapper mapper = new ObjectMapper();

        ObjectNode fieldDescription = mapper.createObjectNode();
        fieldDescription.put("type", "string");
        fieldDescription.put("optional", false);
        fieldDescription.put("field", "string");

        ArrayNode fields = mapper.createArrayNode();
        fields.add(fieldDescription);

        ObjectNode schema = mapper.createObjectNode();
        schema.put("type", "struct");
        schema.set("fields", fields);

        ObjectNode payload = mapper.createObjectNode();
        payload.put("string", string);

        ObjectNode root = mapper.createObjectNode();
        root.set("payload", payload);
        root.set("schema", schema);

        try {
            return mapper.writeValueAsString(root);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
