package io.confluent.connect;

import io.confluent.connect.azblob.AzBlobSinkConnector;
import io.confluent.connect.azblob.AzBlobSinkConnectorConfig;
import io.confluent.connect.azblob.AzBlobSinkTask;
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
public class AzBlobSinkConnectorTest {

    private AzBlobSinkConnector connector;
    private Map<String, String> connProps;

    @Before
    public void setup() {
        connector = new AzBlobSinkConnector();
        connProps = new HashMap<>();
        connProps.put(AzBlobSinkConnectorConfig.AZ_STORAGE_CONTAINER_NAME, "");
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testTaskClass() {
        assertEquals(AzBlobSinkTask.class, connector.taskClass());
    }

//    @Test(expected = ConnectException.class)
//    public void testMissingUrlConfig() throws Exception {
//        HashMap<String, String> connProps = new HashMap<>();
//        connProps.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
//        connector.start(connProps);
//    }
//
//    @Test(expected = ConnectException.class)
//    public void testMissingModeConfig() throws Exception {
//        HashMap<String, String> connProps = new HashMap<>();
//        connProps.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, db.getUrl());
//        connector.start(Collections.<String, String>emptyMap());
//    }
//
//    @Test(expected = ConnectException.class)
//    public void testStartConnectionFailure() throws Exception {
//        // Invalid URL
//        connector.start(Collections.singletonMap(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, "jdbc:foo"));
//    }
//
//    @Test
//    public void testStartStop() throws Exception {
//        CachedConnectionProvider mockCachedConnectionProvider = PowerMock.createMock(CachedConnectionProvider.class);
//        PowerMock.expectNew(CachedConnectionProvider.class, db.getUrl(), null, null).andReturn(mockCachedConnectionProvider);
//
//        // Should request a connection, then should close it on stop()
//        Connection conn = PowerMock.createMock(Connection.class);
//        EasyMock.expect(mockCachedConnectionProvider.getValidConnection()).andReturn(conn);
//
//        // Since we're just testing start/stop, we don't worry about the value here but need to stub
//        // something since the background thread will be started and try to lookup metadata.
//        EasyMock.expect(conn.getMetaData()).andStubThrow(new SQLException());
//
//        mockCachedConnectionProvider.closeQuietly();
//        PowerMock.expectLastCall();
//
//        PowerMock.replayAll();
//
//        connector.start(connProps);
//        connector.stop();
//
//        PowerMock.verifyAll();
//    }
//
//    @Test
//    public void testPartitioningOneTable() throws Exception {
//        // Tests simplest case where we have exactly 1 table and also ensures we return fewer tasks
//        // if there aren't enough tables for the max # of tasks
//        db.createTable("test", "id", "INT NOT NULL");
//        connector.start(connProps);
//        List<Map<String, String>> configs = connector.taskConfigs(10);
//        assertEquals(1, configs.size());
//        assertTaskConfigsHaveParentConfigs(configs);
//        assertEquals("test", configs.get(0).get(JdbcSourceTaskConfig.TABLES_CONFIG));
//        assertNull(configs.get(0).get(JdbcSourceTaskConfig.QUERY_CONFIG));
//        connector.stop();
//    }
//
//    @Test
//    public void testPartitioningManyTables() throws Exception {
//        // Tests distributing tables across multiple tasks, in this case unevenly
//        db.createTable("test1", "id", "INT NOT NULL");
//        db.createTable("test2", "id", "INT NOT NULL");
//        db.createTable("test3", "id", "INT NOT NULL");
//        db.createTable("test4", "id", "INT NOT NULL");
//        connector.start(connProps);
//        List<Map<String, String>> configs = connector.taskConfigs(3);
//        assertEquals(3, configs.size());
//        assertTaskConfigsHaveParentConfigs(configs);
//
//        assertEquals("test1,test2", configs.get(0).get(JdbcSourceTaskConfig.TABLES_CONFIG));
//        assertNull(configs.get(0).get(JdbcSourceTaskConfig.QUERY_CONFIG));
//        assertEquals("test3", configs.get(1).get(JdbcSourceTaskConfig.TABLES_CONFIG));
//        assertNull(configs.get(1).get(JdbcSourceTaskConfig.QUERY_CONFIG));
//        assertEquals("test4", configs.get(2).get(JdbcSourceTaskConfig.TABLES_CONFIG));
//        assertNull(configs.get(2).get(JdbcSourceTaskConfig.QUERY_CONFIG));
//
//        connector.stop();
//    }
//
//    @Test
//    public void testPartitioningQuery() throws Exception {
//        // Tests "partitioning" when config specifies running a custom query
//        db.createTable("test1", "id", "INT NOT NULL");
//        db.createTable("test2", "id", "INT NOT NULL");
//        final String sample_query = "SELECT foo, bar FROM sample_table";
//        connProps.put(JdbcSourceConnectorConfig.QUERY_CONFIG, sample_query);
//        connector.start(connProps);
//        List<Map<String, String>> configs = connector.taskConfigs(3);
//        assertEquals(1, configs.size());
//        assertTaskConfigsHaveParentConfigs(configs);
//
//        assertEquals("", configs.get(0).get(JdbcSourceTaskConfig.TABLES_CONFIG));
//        assertEquals(sample_query, configs.get(0).get(JdbcSourceTaskConfig.QUERY_CONFIG));
//
//        connector.stop();
//    }
//
//    @Test(expected = ConnectException.class)
//    public void testConflictingQueryTableSettings() {
//        final String sample_query = "SELECT foo, bar FROM sample_table";
//        connProps.put(JdbcSourceConnectorConfig.QUERY_CONFIG, sample_query);
//        connProps.put(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, "foo,bar");
//        connector.start(connProps);
//    }
//
//    @Test
//    public void testSchemaPatternUsedForConfigValidation() throws Exception {
//        connProps.put(JdbcSourceConnectorConfig.SCHEMA_PATTERN_CONFIG, "SOME_SCHEMA");
//
//        PowerMock.mockStatic(JdbcUtils.class);
//        EasyMock.expect(JdbcUtils.getTables(EasyMock.anyObject(Connection.class), EasyMock.eq("SOME_SCHEMA")))
//                .andReturn(new ArrayList<String>())
//                .atLeastOnce();
//
//        PowerMock.replayAll();
//
//        connector.validate(connProps);
//
//        PowerMock.verifyAll();
//    }
//
//    private void assertTaskConfigsHaveParentConfigs(List<Map<String, String>> configs) {
//        for (Map<String, String> config : configs) {
//            assertEquals(this.db.getUrl(),
//                    config.get(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG));
//        }
//    }
}