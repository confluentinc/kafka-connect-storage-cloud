package io.confluent.connect;

import io.confluent.connect.azblob.AzBlobSinkConnector;
import io.confluent.connect.azblob.AzBlobSinkConnectorConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
@PrepareForTest({AzBlobSinkConnector.class})
@PowerMockIgnore("javax.management.*")
public class AzBlobSinkConnectorIT {

    private AzBlobSinkConnector connector;
    private Map<String, String> connProps;

    @Before
    public void setup() {
        // Define the connection-string with your values. You can copy the string from the azure portal
        final String connectionString = "DefaultEndpointsProtocol=https;AccountName=ecsbdpsandboxsa;AccountKey=ye32nrBsemg99n4xuEXeXce1znnPGHH37Omp+kYRP3j6AW8AUWaDX+opqc5xstsW767H1PzlzD8ZQ6Eisq2gbA==;EndpointSuffix=core.windows.net";
        connector = new AzBlobSinkConnector();
        connProps = new HashMap<>();
        connProps.put(AzBlobSinkConnectorConfig.AZ_STORAGEACCOUNT_CONNECTION_STRING, connectionString);
        connProps.put(AzBlobSinkConnectorConfig.AZ_STORAGE_CONTAINER_NAME, "mycontainer");
        connProps.put("format.class", "io.confluent.connect.azblob.format.avro.AvroFormat");
        connProps.put("storage.class", "io.confluent.connect.azblob.storage.AzBlobStorage");
        connProps.put("schema.generator.class", "io.confluent.connect.storage.hive.schema.DefaultSchemaGenerator");
        connProps.put("flush.size", "3");
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testStartStop() throws Exception {
        connector.start(connProps);
    }

    @Test
    public void testPutRecords() throws Exception {
        connector.start(connProps);
    }


}