package io.confluent.connect;


import io.confluent.common.utils.SystemTime;
import io.confluent.connect.azblob.AzBlobSinkConnectorConfig;
import io.confluent.connect.azblob.AzBlobSinkTask;
import io.confluent.connect.azblob.storage.AzBlobStorage;
import io.confluent.connect.storage.StorageSinkTestBase;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.partitioner.Partitioner;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;

public class AzBlobSinkTaskIT extends StorageSinkTestBase {

    private static final Schema SCHEMA = SchemaBuilder.struct().name("com.example.Person")
            .field("firstName", Schema.STRING_SCHEMA)
            .field("lastName", Schema.STRING_SCHEMA)
            .field("age", Schema.OPTIONAL_INT32_SCHEMA)
            .field("bool", Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .field("short", Schema.OPTIONAL_INT16_SCHEMA)
            .field("byte", Schema.OPTIONAL_INT8_SCHEMA)
            .field("long", Schema.OPTIONAL_INT64_SCHEMA)
            .field("float", Schema.OPTIONAL_FLOAT32_SCHEMA)
            .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .build();

    private HashMap<String, String> connProps;
    private SinkTaskContext sinkTaskContext;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        connProps = new HashMap<String, String>();
        connProps.put(AzBlobSinkConnectorConfig.AZ_STORAGEACCOUNT_CONNECTION_STRING, System.getenv("IT_AZ_STORAGEACCOUNT_CONNECTIONSTR"));
        connProps.put(AzBlobSinkConnectorConfig.AZ_STORAGE_CONTAINER_NAME, System.getenv("IT_AZ_CONTAINER_NAME"));
        connProps.put("format.class", "io.confluent.connect.azblob.format.avro.AvroFormat");
        connProps.put("storage.class", "io.confluent.connect.azblob.storage.AzBlobStorage");
        connProps.put("schema.generator.class", "io.confluent.connect.storage.hive.schema.DefaultSchemaGenerator");
        connProps.put("flush.size", "3");

        connProps.put(StorageCommonConfig.STORE_URL_CONFIG, "someurl");

    }

    @After
    public void tearDown() throws IOException {
    }

    @Test
    public void put() throws Exception {
        AzBlobSinkTask task = new AzBlobSinkTask();
        task.initialize(context);
        System.out.print(connProps);
        task.start(connProps);

        final Struct struct = new Struct(SCHEMA)
                .put("firstName", "Alex")
                .put("lastName", "Smith")
                .put("bool", true)
                .put("short", (short) 1234)
                .put("byte", (byte) -32)
                .put("long", 12425436L)
                .put("float", (float) 2356.3)
                .put("double", -2436546.56457)
                .put("age", 21);

        final String topic = "test-topic";

        List<SinkRecord> records = new ArrayList<>();
        records.add(new SinkRecord(topic, 12, null, null, SCHEMA, struct, 0));
        records.add(new SinkRecord(topic, 12, null, null, SCHEMA, struct, 1));
        records.add(new SinkRecord(topic, 12, null, null, SCHEMA, struct, 2));
        records.add(new SinkRecord(topic, 12, null, null, SCHEMA, struct, 3));
        records.add(new SinkRecord(topic, 12, null, null, SCHEMA, struct, 4));

        task.put(records);
    }

}