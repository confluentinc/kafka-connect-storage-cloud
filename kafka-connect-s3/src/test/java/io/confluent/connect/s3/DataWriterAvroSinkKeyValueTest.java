/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.SchemaProjector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.confluent.connect.s3.format.avro.AvroFormat;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.s3.util.FileUtils;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.kafka.serializers.NonRecordContainer;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class DataWriterAvroSinkKeyValueTest extends TestWithMockedS3 {

    private static final String ZERO_PAD_FMT = "%010d";

    private final String extension = ".avro";
    protected S3Storage storage;
    protected AmazonS3 s3;
    protected static final String SINK_KEY = "true";
    AvroFormat format;
    Partitioner<FieldSchema> partitioner;
    S3SinkTask task;
    Map<String, String> localProps = new HashMap<>();

    @Override
    protected Map<String, String> createProps() {
        Map<String, String> props = super.createProps();
        props.put(S3SinkConnectorConfig.SINK_KEY_CONFIG, SINK_KEY);
        props.putAll(localProps);
        return props;
    }

    //@Before should be omitted in order to be able to add properties per test.
    public void setUp() throws Exception {
        super.setUp();

        s3 = newS3Client(connectorConfig);

        storage = new S3Storage(connectorConfig, url, S3_TEST_BUCKET_NAME, s3);

        partitioner = new DefaultPartitioner<>();
        partitioner.configure(parsedConfig);
        format = new AvroFormat(storage);

        s3.createBucket(S3_TEST_BUCKET_NAME);
        assertTrue(s3.doesBucketExist(S3_TEST_BUCKET_NAME));
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        localProps.clear();
    }

    @Test
    public void testWriteRecords() throws Exception {
        setUp();
        task = new S3SinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);

        List<SinkRecord> sinkRecords = createRecords(7,0, Collections.singleton(new TopicPartition(TOPIC, PARTITION)));
        List<SinkRecord> expectedSinkRecords = createExpectedRecords(7, 0, Collections.singleton(new TopicPartition(TOPIC, PARTITION)));
        // Perform write
        task.put(sinkRecords);
        task.close(context.assignment());
        task.stop();

        long[] validOffsets = {0, 3, 6};
        verify(expectedSinkRecords, validOffsets);
    }

    @Test
    public void testWriteRecordsKeyIsString() throws Exception {
        setUp();
        task = new S3SinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);

        List<SinkRecord> sinkRecords = createRecordsKeyIsString(7, 0, Collections.singleton(new TopicPartition(TOPIC, PARTITION)));
        List<SinkRecord> expectedSinkRecords = createExpectedRecordsKeyIsString(7, 0, Collections.singleton(new TopicPartition(TOPIC, PARTITION)));
        // Perform write
        task.put(sinkRecords);
        task.close(context.assignment());
        task.stop();

        long[] validOffsets = {0, 3, 6};
        verify(expectedSinkRecords, validOffsets);
    }

    @Test
    public void testWriteRecordsKeyIsInt() throws Exception {
        setUp();
        task = new S3SinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);

        List<SinkRecord> sinkRecords = createRecordsKeyIsInt(7, 0, Collections.singleton(new TopicPartition(TOPIC, PARTITION)));
        List<SinkRecord> expectedSinkRecords = createExpectedRecordsKeyIsInt(7, 0, Collections.singleton(new TopicPartition(TOPIC, PARTITION)));
        // Perform write
        task.put(sinkRecords);
        task.close(context.assignment());
        task.stop();

        long[] validOffsets = {0, 3, 6};
        verify(expectedSinkRecords, validOffsets);
    }

    @Test
    public void testWriteRecordsValueIsNested() throws Exception {
        setUp();
        task = new S3SinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);

        List<SinkRecord> sinkRecords = createRecordsValueIsNested(7, 0, Collections.singleton(new TopicPartition(TOPIC, PARTITION)));
        List<SinkRecord> expectedSinkRecords = createExpectedRecordsValueIsNested(7, 0, Collections.singleton(new TopicPartition(TOPIC, PARTITION)));
        // Perform write
        task.put(sinkRecords);
        task.close(context.assignment());
        task.stop();

        long[] validOffsets = {0, 3, 6};
        verify(expectedSinkRecords, validOffsets);
    }

    // Regular test case
    protected Schema createKeySchema() {
        return SchemaBuilder
                .struct()
                .field("remote_addr", Schema.STRING_SCHEMA)
                .field("http_x_forwarded_for", Schema.STRING_SCHEMA)
                .field("time_iso8601", Schema.STRING_SCHEMA)
                .field("uid_set", Schema.STRING_SCHEMA)
                .field("uid_got", Schema.STRING_SCHEMA)
                .field("request_method", Schema.STRING_SCHEMA)
                .field("server_protocol", Schema.STRING_SCHEMA)
                .field("status", Schema.STRING_SCHEMA)
                .field("body_bytes_sent", Schema.STRING_SCHEMA)
                .field("http_referer", Schema.STRING_SCHEMA)
                .field("http_user_agent", Schema.STRING_SCHEMA)
                .build();
    }

    protected Struct createKeyRecord(Schema schema) {
        return (new Struct(schema)
                .put("remote_addr", "10.0.0.94")
                .put("http_x_forwarded_for", "205.144.219.182")
                .put("time_iso8601", "2017-03-06T21:22:20-05:00")
                .put("uid_set", "3D02000A5C19BE58F00D3E68029E17A8")
                .put("uid_got", "-")
                .put("request_method", "GET")
                .put("server_protocol", "HTTP/1.1")
                .put("status", "200")
                .put("body_bytes_sent", "35")
                .put("http_referer", "-")
                .put("http_user_agent", "Mozilla/5.0"));
    }

    protected Schema createValueSchema() {
        return SchemaBuilder
                .struct()
                .field("boolean", Schema.BOOLEAN_SCHEMA)
                .field("int", Schema.INT32_SCHEMA)
                .field("long", Schema.INT64_SCHEMA)
                .field("float", Schema.FLOAT32_SCHEMA)
                .field("double", Schema.FLOAT64_SCHEMA)
                .field("string", Schema.STRING_SCHEMA)
                .build();
    }

    protected Struct createValueRecord(Schema schema) {
        return (new Struct(schema)
                .put("boolean", true)
                .put("int", Integer.valueOf(12))
                .put("long", 12L)
                .put("float", 12.2F)
                .put("double", 12.2D)
                .put("string", "string"));
    }

    protected Schema createExpectedValueSchema() {
        return SchemaBuilder
                .struct()
                .field("remote_addr", Schema.STRING_SCHEMA)
                .field("http_x_forwarded_for", Schema.STRING_SCHEMA)
                .field("time_iso8601", Schema.STRING_SCHEMA)
                .field("uid_set", Schema.STRING_SCHEMA)
                .field("uid_got", Schema.STRING_SCHEMA)
                .field("request_method", Schema.STRING_SCHEMA)
                .field("server_protocol", Schema.STRING_SCHEMA)
                .field("status", Schema.STRING_SCHEMA)
                .field("body_bytes_sent", Schema.STRING_SCHEMA)
                .field("http_referer", Schema.STRING_SCHEMA)
                .field("http_user_agent", Schema.STRING_SCHEMA)
                .field("boolean", Schema.BOOLEAN_SCHEMA)
                .field("int", Schema.INT32_SCHEMA)
                .field("long", Schema.INT64_SCHEMA)
                .field("float", Schema.FLOAT32_SCHEMA)
                .field("double", Schema.FLOAT64_SCHEMA)
                .field("string", Schema.STRING_SCHEMA)
                .build();
    }

    protected Struct createExpectedValueRecord(Schema schema) {
        return (new Struct(schema)
                .put("remote_addr", new String("10.0.0.94"))
                .put("http_x_forwarded_for", new String("205.144.219.182"))
                .put("time_iso8601", "2017-03-06T21:22:20-05:00")
                .put("uid_set", "3D02000A5C19BE58F00D3E68029E17A8")
                .put("uid_got", "-")
                .put("request_method", "GET")
                .put("server_protocol", "HTTP/1.1")
                .put("status", "200")
                .put("body_bytes_sent", "35")
                .put("http_referer", "-")
                .put("http_user_agent", "Mozilla/5.0")
                .put("boolean", true)
                .put("int", Integer.valueOf(12))
                .put("long", 12L)
                .put("float", 12.2F)
                .put("double", 12.2D)
                .put("string", "string"));
    }

    // Key is a primitive string type
    protected Schema createKeySchemaKeyIsString() {
        return SchemaBuilder.string().build();
    }

    protected String createKeyRecordKeyIsString() {
        return "This is string primitive";
    }

    protected Schema createExpectedValueSchemaKeyIsString() {
        return SchemaBuilder
                .struct()
                .field("key", Schema.STRING_SCHEMA)
                .field("boolean", Schema.BOOLEAN_SCHEMA)
                .field("int", Schema.INT32_SCHEMA)
                .field("long", Schema.INT64_SCHEMA)
                .field("float", Schema.FLOAT32_SCHEMA)
                .field("double", Schema.FLOAT64_SCHEMA)
                .field("string", Schema.STRING_SCHEMA)
                .build();
    }

    protected Struct createExpectedValueRecordKeyIsString(Schema schema) {
        return (new Struct(schema)
                .put("key", "This is string primitive")
                .put("boolean", true)
                .put("int", Integer.valueOf(12))
                .put("long", 12L)
                .put("float", 12.2F)
                .put("double", 12.2D)
                .put("string", "string"));
    }

    // Key is a primitive int type
    protected Schema createKeySchemaKeyIsInt() {
        return SchemaBuilder.int32().build();
    }

    protected Integer createKeyRecordKeyIsInt() {
        return Integer.valueOf(12);
    }

    protected Schema createExpectedValueSchemaKeyIsInt() {
        return SchemaBuilder
                .struct()
                .field("key", Schema.INT32_SCHEMA)
                .field("boolean", Schema.BOOLEAN_SCHEMA)
                .field("int", Schema.INT32_SCHEMA)
                .field("long", Schema.INT64_SCHEMA)
                .field("float", Schema.FLOAT32_SCHEMA)
                .field("double", Schema.FLOAT64_SCHEMA)
                .field("string", Schema.STRING_SCHEMA)
                .build();
    }

    protected Struct createExpectedValueRecordKeyIsInt(Schema schema) {
        return (new Struct(schema)
                .put("key", Integer.valueOf(12))
                .put("boolean", true)
                .put("int", Integer.valueOf(12))
                .put("long", 12L)
                .put("float", 12.2F)
                .put("double", 12.2D)
                .put("string", "string"));
    }

    // Value is a nested type
    protected Schema createValueSchemaValueIsNested() {
        return SchemaBuilder
                .struct()
                .name("layer1")
                .field("base",
                        SchemaBuilder
                                .struct()
                                .name("layer2")
                                .field("layer2_string", Schema.STRING_SCHEMA)
                                .field("layer2_int", Schema.INT32_SCHEMA)
                                .field("layer2_long", Schema.INT64_SCHEMA)
                                .field("layer2_base",
                                        SchemaBuilder
                                                .struct()
                                                .name("layer3")
                                                .field("layer3_string", Schema.STRING_SCHEMA)
                                                .field("layer3_int", Schema.INT32_SCHEMA)
                                                .field("layer3_long", Schema.INT64_SCHEMA)
                                                .build()
                                ).build()
                )
                .field("layer1_boolean", Schema.BOOLEAN_SCHEMA)
                .field("layer1_int", Schema.INT32_SCHEMA)
                .field("layer1_long", Schema.INT64_SCHEMA)
                .field("layer1_float", Schema.FLOAT32_SCHEMA)
                .field("layer1_double", Schema.FLOAT64_SCHEMA)
                .field("layer1_string", Schema.STRING_SCHEMA)
                .build();
    }

    protected Schema createValueSchemaValueIsNestedBase() {
        return SchemaBuilder
                .struct()
                .name("layer2")
                .field("layer2_string", Schema.STRING_SCHEMA)
                .field("layer2_int", Schema.INT32_SCHEMA)
                .field("layer2_long", Schema.INT64_SCHEMA)
                .field("layer2_base",
                        SchemaBuilder
                                .struct()
                                .name("layer3")
                                .field("layer3_string", Schema.STRING_SCHEMA)
                                .field("layer3_int", Schema.INT32_SCHEMA)
                                .field("layer3_long", Schema.INT64_SCHEMA)
                                .build()
                ).build();
    }

    protected Schema createValueSchemaValueIsNestedSubBase() {
        return SchemaBuilder
                .struct()
                .name("layer3")
                .field("layer3_string", Schema.STRING_SCHEMA)
                .field("layer3_int", Schema.INT32_SCHEMA)
                .field("layer3_long", Schema.INT64_SCHEMA)
                .build();
    }

    protected Struct createValueRecordValueIsNested(Schema schema, Schema baseSchema, Schema subBaseSchema) {
        return (new Struct(schema)
                .put("base",
                        new Struct(baseSchema)
                                .put("layer2_string", "string")
                                .put("layer2_int", Integer.valueOf(12))
                                .put("layer2_long", 12L)
                                .put("layer2_base",
                                        new Struct(subBaseSchema)
                                                .put("layer3_string", "string")
                                                .put("layer3_int", Integer.valueOf(12))
                                                .put("layer3_long", 12L)
                                )
                )
                .put("layer1_boolean", true)
                .put("layer1_int", Integer.valueOf(12))
                .put("layer1_long", 12L)
                .put("layer1_float", 12.2F)
                .put("layer1_double", 12.2D)
                .put("layer1_string", "string"));
    }

    protected Schema createExpectedValueSchemaValueIsNested() {
        return SchemaBuilder
                .struct()
                .name("layer1")
                .field("remote_addr", Schema.STRING_SCHEMA)
                .field("http_x_forwarded_for", Schema.STRING_SCHEMA)
                .field("time_iso8601", Schema.STRING_SCHEMA)
                .field("uid_set", Schema.STRING_SCHEMA)
                .field("uid_got", Schema.STRING_SCHEMA)
                .field("request_method", Schema.STRING_SCHEMA)
                .field("server_protocol", Schema.STRING_SCHEMA)
                .field("status", Schema.STRING_SCHEMA)
                .field("body_bytes_sent", Schema.STRING_SCHEMA)
                .field("http_referer", Schema.STRING_SCHEMA)
                .field("http_user_agent", Schema.STRING_SCHEMA)
                .field("base",
                        SchemaBuilder
                                .struct()
                                .name("layer2")
                                .field("layer2_string", Schema.STRING_SCHEMA)
                                .field("layer2_int", Schema.INT32_SCHEMA)
                                .field("layer2_long", Schema.INT64_SCHEMA)
                                .field("layer2_base",
                                        SchemaBuilder
                                                .struct()
                                                .name("layer3")
                                                .field("layer3_string", Schema.STRING_SCHEMA)
                                                .field("layer3_int", Schema.INT32_SCHEMA)
                                                .field("layer3_long", Schema.INT64_SCHEMA).build()
                                ).build()
                )
                .field("layer1_boolean", Schema.BOOLEAN_SCHEMA)
                .field("layer1_int", Schema.INT32_SCHEMA)
                .field("layer1_long", Schema.INT64_SCHEMA)
                .field("layer1_float", Schema.FLOAT32_SCHEMA)
                .field("layer1_double", Schema.FLOAT64_SCHEMA)
                .field("layer1_string", Schema.STRING_SCHEMA)
                .build();
    }

    protected Struct createExpectedValueRecordValueIsNested(Schema schema, Schema baseSchema, Schema subBaseSchema) {
        return (new Struct(schema)
                .put("remote_addr", "10.0.0.94")
                .put("http_x_forwarded_for", "205.144.219.182")
                .put("time_iso8601", "2017-03-06T21:22:20-05:00")
                .put("uid_set", "3D02000A5C19BE58F00D3E68029E17A8")
                .put("uid_got", "-")
                .put("request_method", "GET")
                .put("server_protocol", "HTTP/1.1")
                .put("status", "200")
                .put("body_bytes_sent", "35")
                .put("http_referer", "-")
                .put("http_user_agent", "Mozilla/5.0")
                .put("base",
                        new Struct(baseSchema)
                                .put("layer2_string", "string")
                                .put("layer2_int", Integer.valueOf(12))
                                .put("layer2_long", 12L)
                                .put("layer2_base",
                                        new Struct(subBaseSchema)
                                                .put("layer3_string", "string")
                                                .put("layer3_int", Integer.valueOf(12))
                                                .put("layer3_long", 12L)
                                )
                )
                .put("layer1_boolean", true)
                .put("layer1_int", Integer.valueOf(12))
                .put("layer1_long", 12L)
                .put("layer1_float", 12.2F)
                .put("layer1_double", 12.2D)
                .put("layer1_string", "string"));
    }

    /**
     * Return a list of new records starting at zero offset.
     *
     * @param size the number of records to return.
     * @return the list of records.
     */
    protected List<SinkRecord> createRecords(int size, long startOffset, Set<TopicPartition> partitions) {
        Schema keySchema = createKeySchema();
        Struct keyRecord = createKeyRecord(keySchema);
        Schema valueSchema = createValueSchema();
        Struct valueRecord = createValueRecord(valueSchema);

        List<SinkRecord> sinkRecords = new ArrayList<>();
        for (TopicPartition tp : partitions) {
            for (long offset = startOffset; offset < startOffset + size; ++offset) {
                sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), keySchema, keyRecord, valueSchema, valueRecord, offset));
            }
        }
        return sinkRecords;
    }

    /**
     * Return a list of expected records starting at zero offset.
     *
     * @param size the number of records to return.
     * @return the list of records.
     */
    protected List<SinkRecord> createExpectedRecords(int size, long startOffset, Set<TopicPartition> partitions) {
        Schema keySchema = createKeySchema();
        Struct keyRecord = createKeyRecord(keySchema);
        Schema valueSchema = createExpectedValueSchema();
        Struct valueRecord = createExpectedValueRecord(valueSchema);

        List<SinkRecord> sinkRecords = new ArrayList<>();
        for (TopicPartition tp : partitions) {
            for (long offset = startOffset; offset < startOffset + size; ++offset) {
                sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), keySchema, keyRecord, valueSchema, valueRecord, offset));
            }
        }
        return sinkRecords;
    }

    /**
     * Return a list of new records starting at zero offset.
     *
     * @param size the number of records to return.
     * @return the list of records.
     */
    protected List<SinkRecord> createRecordsKeyIsString(int size, long startOffset, Set<TopicPartition> partitions) {
        Schema keySchema = createKeySchemaKeyIsString();
        String keyRecord = createKeyRecordKeyIsString();
        Schema valueSchema = createValueSchema();
        Struct valueRecord = createValueRecord(valueSchema);

        List<SinkRecord> sinkRecords = new ArrayList<>();
        for (TopicPartition tp : partitions) {
            for (long offset = startOffset; offset < startOffset + size; ++offset) {
                sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), keySchema, keyRecord, valueSchema, valueRecord, offset));
            }
        }
        return sinkRecords;
    }

    /**
     * Return a list of expected records starting at zero offset.
     *
     * @param size the number of records to return.
     * @return the list of records.
     */
    protected List<SinkRecord> createExpectedRecordsKeyIsString(int size, long startOffset, Set<TopicPartition> partitions) {
        Schema keySchema = createKeySchemaKeyIsString();
        String keyRecord = createKeyRecordKeyIsString();
        Schema valueSchema = createExpectedValueSchemaKeyIsString();
        Struct valueRecord = createExpectedValueRecordKeyIsString(valueSchema);

        List<SinkRecord> sinkRecords = new ArrayList<>();
        for (TopicPartition tp : partitions) {
            for (long offset = startOffset; offset < startOffset + size; ++offset) {
                sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), keySchema, keyRecord, valueSchema, valueRecord, offset));
            }
        }
        return sinkRecords;
    }

    /**
     * Return a list of new records starting at zero offset.
     *
     * @param size the number of records to return.
     * @return the list of records.
     */
    protected List<SinkRecord> createRecordsKeyIsInt(int size, long startOffset, Set<TopicPartition> partitions) {
        Schema keySchema = createKeySchemaKeyIsInt();
        Integer keyRecord = createKeyRecordKeyIsInt();
        Schema valueSchema = createValueSchema();
        Struct valueRecord = createValueRecord(valueSchema);

        List<SinkRecord> sinkRecords = new ArrayList<>();
        for (TopicPartition tp : partitions) {
            for (long offset = startOffset; offset < startOffset + size; ++offset) {
                sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), keySchema, keyRecord, valueSchema, valueRecord, offset));
            }
        }
        return sinkRecords;
    }

    /**
     * Return a list of expected records starting at zero offset.
     *
     * @param size the number of records to return.
     * @return the list of records.
     */
    protected List<SinkRecord> createExpectedRecordsKeyIsInt(int size, long startOffset, Set<TopicPartition> partitions) {
        Schema keySchema = createKeySchemaKeyIsInt();
        Integer keyRecord = createKeyRecordKeyIsInt();
        Schema valueSchema = createExpectedValueSchemaKeyIsInt();
        Struct valueRecord = createExpectedValueRecordKeyIsInt(valueSchema);

        List<SinkRecord> sinkRecords = new ArrayList<>();
        for (TopicPartition tp : partitions) {
            for (long offset = startOffset; offset < startOffset + size; ++offset) {
                sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), keySchema, keyRecord, valueSchema, valueRecord, offset));
            }
        }
        return sinkRecords;
    }

    /**
     * Return a list of new records starting at zero offset.
     *
     * @param size the number of records to return.
     * @return the list of records.
     */
    protected List<SinkRecord> createRecordsValueIsNested(int size, long startOffset, Set<TopicPartition> partitions) {
        Schema keySchema = createKeySchema();
        Struct keyRecord = createKeyRecord(keySchema);

        Schema valueSchema = createValueSchemaValueIsNested();
        Schema baseSchema = createValueSchemaValueIsNestedBase();
        Schema subBaseSchema = createValueSchemaValueIsNestedSubBase();

        Struct valueRecord = createValueRecordValueIsNested(valueSchema, baseSchema, subBaseSchema);

        List<SinkRecord> sinkRecords = new ArrayList<>();
        for (TopicPartition tp : partitions) {
            for (long offset = startOffset; offset < startOffset + size; ++offset) {
                sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), keySchema, keyRecord, valueSchema, valueRecord, offset));
            }
        }
        return sinkRecords;
    }

    /**
     * Return a list of expected records starting at zero offset.
     *
     * @param size the number of records to return.
     * @return the list of records.
     */
    protected List<SinkRecord> createExpectedRecordsValueIsNested(int size, long startOffset, Set<TopicPartition> partitions) {
        Schema keySchema = createKeySchema();
        Struct keyRecord = createKeyRecord(keySchema);

        Schema valueSchema = createExpectedValueSchemaValueIsNested();
        Schema baseSchema = createValueSchemaValueIsNestedBase();
        Schema subBaseSchema = createValueSchemaValueIsNestedSubBase();

        Struct valueRecord = createExpectedValueRecordValueIsNested(valueSchema, baseSchema, subBaseSchema);

        List<SinkRecord> sinkRecords = new ArrayList<>();
        for (TopicPartition tp : partitions) {
            for (long offset = startOffset; offset < startOffset + size; ++offset) {
                sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), keySchema, keyRecord, valueSchema, valueRecord, offset));
            }
        }
        return sinkRecords;
    }

    protected String getDirectory() {
        return getDirectory(TOPIC, PARTITION);
    }

    protected String getDirectory(String topic, int partition) {
        String encodedPartition = "partition=" + String.valueOf(partition);
        return partitioner.generatePartitionedPath(topic, encodedPartition);
    }

    protected List<String> getExpectedFiles(long[] validOffsets, TopicPartition tp) {
        List<String> expectedFiles = new ArrayList<>();
        for (int i = 1; i < validOffsets.length; ++i) {
            long startOffset = validOffsets[i - 1];
            expectedFiles.add(FileUtils.fileKeyToCommit(topicsDir, getDirectory(tp.topic(), tp.partition()), tp, startOffset,
                    extension, ZERO_PAD_FMT));
        }
        return expectedFiles;
    }

    protected void verifyFileListing(long[] validOffsets, Set<TopicPartition> partitions) throws IOException {
        List<String> expectedFiles = new ArrayList<>();
        for (TopicPartition tp : partitions) {
            expectedFiles.addAll(getExpectedFiles(validOffsets, tp));
        }
        verifyFileListing(expectedFiles);
    }

    protected void verifyFileListing(List<String> expectedFiles) throws IOException {
        List<S3ObjectSummary> summaries = listObjects(S3_TEST_BUCKET_NAME, null, s3);
        List<String> actualFiles = new ArrayList<>();
        for (S3ObjectSummary summary : summaries) {
            String fileKey = summary.getKey();
            actualFiles.add(fileKey);
        }

        Collections.sort(actualFiles);
        Collections.sort(expectedFiles);
        assertThat(actualFiles, is(expectedFiles));
    }

    protected void verifyContents(List<SinkRecord> expectedRecords, int startIndex, Collection<Object> records) {
        Schema expectedSchema = null;
        for (Object avroRecord : records) {
            if (expectedSchema == null) {
                expectedSchema = expectedRecords.get(startIndex).valueSchema();
            }
            Object expectedValue = SchemaProjector.project(expectedRecords.get(startIndex).valueSchema(),
                    expectedRecords.get(startIndex++).value(),
                    expectedSchema);
            Object value = format.getAvroData().fromConnectData(expectedSchema, expectedValue);
            // AvroData wraps primitive types so their schema can be included. We need to unwrap
            // NonRecordContainers to just their value to properly handle these types
            if (value instanceof NonRecordContainer) {
                value = ((NonRecordContainer) value).getValue();
            }
            assertEquals(value, avroRecord);
        }
    }

    protected void verify(List<SinkRecord> sinkRecords, long[] validOffsets) throws IOException {
        verify(sinkRecords, validOffsets, Collections.singleton(new TopicPartition(TOPIC, PARTITION)), false);
    }

    protected void verify(List<SinkRecord> sinkRecords, long[] validOffsets, Set<TopicPartition> partitions)
            throws IOException {
        verify(sinkRecords, validOffsets, partitions, false);
    }

    /**
     * Verify files and records are uploaded appropriately.
     * @param sinkRecords a flat list of the records that need to appear in potentially several files in S3.
     * @param validOffsets an array containing the offsets that map to uploaded files for a topic-partition.
     *                     Offsets appear in ascending order, the difference between two consecutive offsets
     *                     equals the expected size of the file, and last offset in exclusive.
     * @throws IOException
     */
    protected void verify(List<SinkRecord> sinkRecords, long[] validOffsets, Set<TopicPartition> partitions,
                          boolean skipFileListing)
            throws IOException {
        if (!skipFileListing) {
            verifyFileListing(validOffsets, partitions);
        }

        for (TopicPartition tp : partitions) {
            for (int i = 1, j = 0; i < validOffsets.length; ++i) {
                long startOffset = validOffsets[i - 1];
                long size = validOffsets[i] - startOffset;

                FileUtils.fileKeyToCommit(topicsDir, getDirectory(tp.topic(), tp.partition()), tp, startOffset, extension, ZERO_PAD_FMT);
                Collection<Object> records = readRecords(topicsDir, getDirectory(tp.topic(), tp.partition()), tp, startOffset,
                        extension, ZERO_PAD_FMT, S3_TEST_BUCKET_NAME, s3);
                assertEquals(size, records.size());
                verifyContents(sinkRecords, j, records);
                j += size;
            }
        }
    }

    protected void verifyOffsets(Map<TopicPartition, OffsetAndMetadata> actualOffsets, long[] validOffsets,
                                 Set<TopicPartition> partitions) {
        int i = 0;
        Map<TopicPartition, OffsetAndMetadata> expectedOffsets = new HashMap<>();
        for (TopicPartition tp : partitions) {
            long offset = validOffsets[i++];
            if (offset >= 0) {
                expectedOffsets.put(tp, new OffsetAndMetadata(offset, ""));
            }
        }
        assertTrue(Objects.equals(actualOffsets, expectedOffsets));
    }
}



