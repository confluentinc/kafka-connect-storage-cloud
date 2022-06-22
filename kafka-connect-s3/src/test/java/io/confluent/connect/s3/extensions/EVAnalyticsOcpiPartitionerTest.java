package io.confluent.connect.s3.extensions;

import io.confluent.connect.storage.StorageSinkTestBase;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.errors.PartitionException;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatterBuilder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

import org.joda.time.ReadableInstant;
import org.junit.Test;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class EVAnalyticsOcpiPartitionerTest extends StorageSinkTestBase {
    // Partitioner should:
    // - Take in a valid sessions / location payload
    // - Check that it has the offering_uuid somewhere (ideally in the key of the message)
    // - Check that it has id in the payload (needs JSON parser) (for entityId)
    // - Check that it has timestamp in the payload in the right format to be parsed (zulu time)
    // - Produce the correct path from the 'String encodePartition(SinkRecord sinkRecord)' function

    @Test
    public void testProducesPathFromValidOcpiSessionsPayload() throws Exception {
        // Top level config
        Map<String, Object> config = new HashMap<>();
        config.put(StorageCommonConfig.DIRECTORY_DELIM_CONFIG, StorageCommonConfig.DIRECTORY_DELIM_DEFAULT);



        // Configure the partitioner
        EVAnalyticsOcpiPartitioner<String> partitioner = new EVAnalyticsOcpiPartitioner<>();
        partitioner.configure(config);

        String timeZoneString = (String) config.get(PartitionerConfig.TIMEZONE_CONFIG);
        int YYYY = 2022;
        int MM = 6;
        int DD = 9;
        int HH = 7;
        long timestamp = new DateTime(YYYY, MM, DD, HH, 0, 0, 0, DateTimeZone.forID(timeZoneString)).getMillis();
        String streamUuid = "1e962902-65ae-4346-bb8d-d2206d6dc852";
        String entityId = "entity-1234";
        // timestamp format: "2021-08-31T17:24:13Z";
        // Create an OCPI location payload
        String payloadTimestamp = String.format("%d-%02d-%02dT%02d:12:34Z", YYYY, MM, DD, HH);
        String ocpiSessionPayload = String.format(
                "{\"id\":\"%s\",\"countryCode\":\"GB\",\"partyId\":\"CKL\",\"type\":\"EVChargingSession\",\"evseId\":\"GB*CKL*7*1\",\"address\":{\"postalCode\":\"CV1 3AQ\",\"streetAddress\":\"Northumberland Road\",\"addressCountry\":\"GB\",\"addressLocality\":\"Coventry\"},\"totalKW\":1.019,\"location\":{\"type\":\"Point\",\"coordinates\":[-1.524266,52.411123]},\"provider\":\"CKL\",\"sessionId\":59,\"timestamp\":\"%s\",\"connectorId\":7,\"sessionDurationMins\":0.3,\"chargingDurationMins\":0.3,\"sessionStartTime\":\"2020-04-28T11:54:54Z\",\"sessionEndTime\":\"2020-04-28T11:55:09Z\",\"totalCost\":{\"exclVat\":1,\"inclVat\":1.2}}",
                entityId,
                payloadTimestamp
                );
        // Need a 'create OCPI sink record type function, I think'
        Schema schema = this.createSchemaWithTimestampField();
        SinkRecord ocpiSessionRecord = new SinkRecord("test-ocpi-session-topic", 12, Schema.STRING_SCHEMA, streamUuid, schema, ocpiSessionPayload, 0L, timestamp, TimestampType.CREATE_TIME);

        // Run the partitioner
        String encodedPartition = partitioner.encodePartition(ocpiSessionRecord);

        // Assert that the filepath is correct
        String expectedPath = String.format("%s/%s/%d-%02d/%02d/%02d", streamUuid, entityId, YYYY, MM, DD, HH);
        assertThat(encodedPartition, is(expectedPath));
    }

    @Test
    public void testProducesPathFromValidOcpiLocationsPayload() throws Exception {
        // Top level config
        Map<String, Object> config = new HashMap<>();
        config.put(StorageCommonConfig.DIRECTORY_DELIM_CONFIG, StorageCommonConfig.DIRECTORY_DELIM_DEFAULT);

        // Configure the partitioner
        EVAnalyticsOcpiPartitioner<String> partitioner = new EVAnalyticsOcpiPartitioner<>();
        partitioner.configure(config);

        String streamUuid = "1e962902-65ae-4346-bb8d-d2206d6dc852";
        String entityId = "entity-1234";

        String timeZoneString = (String) config.get(PartitionerConfig.TIMEZONE_CONFIG);
        int YYYY = 2022;
        int MM = 6;
        int DD = 9;
        int HH = 7;
        long timestamp = new DateTime(YYYY, MM, DD, HH, 0, 0, 0, DateTimeZone.forID(timeZoneString)).getMillis();
        String payloadTimestamp = String.format("%d-%02d-%02dT%02d:12:34Z", YYYY, MM, DD, HH);
        String ocpiLocationPayload = String.format(
                "{\"id\":\"%s\",\"type\":\"EVChargingStation\",\"status\":{\"type\":\"Property\",\"value\":\"AVAILABLE\"},\"address\":{\"type\":\"Property\",\"value\":{\"type\":\"PostalAddress\",\"postalCode\":\"N15 6BT\",\"streetAddress\":\"Grovelands Road\",\"addressCountry\":\"GBR\",\"addressLocality\":\"Haringey\"}},\"voltage\":{\"type\":\"Property\",\"value\":230},\"amperage\":{\"type\":\"Property\",\"value\":23},\"location\":{\"type\":\"GeoProperty\",\"value\":{\"type\":\"Point\",\"coordinates\":[-0.06414,51.578497]}},\"operator\":{\"type\":\"Property\",\"value\":\"char.gy\"},\"timestamp\":{\"type\":\"Property\",\"value\":\"%s\"},\"powerOutput\":{\"type\":\"Property\",\"value\":52.9},\"chargingType\":{\"type\":\"Property\",\"value\":\"rapid\"},\"dateModified\":{\"type\":\"Property\",\"value\":\"2021-05-07T06:06:30Z\"},\"socketNumber\":{\"type\":\"Property\",\"value\":1},\"commissionDate\":{\"type\":\"Property\",\"value\":\"\"},\"decommissionDate\":{\"type\":\"Property\",\"value\":\"\"},\"@context\":[\"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld\",\"https://raw.githubusercontent.com/smart-data-models/dataModel.Transportation/master/context.jsonld\"]}",
                entityId,
                payloadTimestamp
        );

        Schema schema = this.createSchemaWithTimestampField();
        SinkRecord ocpiSessionRecord = new SinkRecord("test-ocpi-session-topic", 13, Schema.STRING_SCHEMA, streamUuid, schema, ocpiLocationPayload, 0L, timestamp, TimestampType.CREATE_TIME);

        // Run the partitioner
        String encodedPartition = partitioner.encodePartition(ocpiSessionRecord);

        // Assert that the filepath is correct
        String expectedPath = String.format("%s/%s/%d-%02d/%02d/%02d", streamUuid, entityId, YYYY, MM, DD, HH);
        assertThat(encodedPartition, is(expectedPath));
    }

    @Test
    public void testCannotParseJsonPayload() throws Exception {
        // Top level config
        Map<String, Object> config = new HashMap<>();
        config.put(StorageCommonConfig.DIRECTORY_DELIM_CONFIG, StorageCommonConfig.DIRECTORY_DELIM_DEFAULT);

        // Configure the partitioner
        EVAnalyticsOcpiPartitioner<String> partitioner = new EVAnalyticsOcpiPartitioner<>();
        partitioner.configure(config);

        String streamUuid = "1e962902-65ae-4346-bb8d-d2206d6dc852";

        String timeZoneString = (String) config.get(PartitionerConfig.TIMEZONE_CONFIG);
        int YYYY = 2022;
        int MM = 6;
        int DD = 9;
        int HH = 7;
        long timestamp = new DateTime(YYYY, MM, DD, HH, 0, 0, 0, DateTimeZone.forID(timeZoneString)).getMillis();
        String ocpiLocationPayload = "{\"not\":\"validAtAll\"}";
        Schema schema = this.createSchemaWithTimestampField();
        SinkRecord ocpiSessionRecord = new SinkRecord("test-ocpi-session-topic", 13, Schema.STRING_SCHEMA, streamUuid, schema, ocpiLocationPayload, 0L, timestamp, TimestampType.CREATE_TIME);

        Exception e = assertThrows(PartitionException.class, () -> {
            partitioner.encodePartition(ocpiSessionRecord);
        });

        assertEquals("Could not map this payload to a known OCPI class", e.getMessage());
    }

    @Test
    public void testNoValidUuidInKey() throws Exception {
        // Top level config
        Map<String, Object> config = new HashMap<>();
        config.put(StorageCommonConfig.DIRECTORY_DELIM_CONFIG, StorageCommonConfig.DIRECTORY_DELIM_DEFAULT);

        // Configure the partitioner
        EVAnalyticsOcpiPartitioner<String> partitioner = new EVAnalyticsOcpiPartitioner<>();
        partitioner.configure(config);

        String streamUuidNotAUuid = "not-a-uuid-la-la-la";

        String timeZoneString = (String) config.get(PartitionerConfig.TIMEZONE_CONFIG);
        int YYYY = 2022;
        int MM = 6;
        int DD = 9;
        int HH = 7;
        long timestamp = new DateTime(YYYY, MM, DD, HH, 0, 0, 0, DateTimeZone.forID(timeZoneString)).getMillis();
        String payloadTimestamp = String.format("%d-%02d-%02dT%02d:12:34Z", YYYY, MM, DD, HH);
        String ocpiLocationPayload = "{\"not\":\"validEvenIfTheStreamUuidWasValid\"}";
        Schema schema = this.createSchemaWithTimestampField();
        SinkRecord ocpiSessionRecord = new SinkRecord("test-ocpi-session-topic", 13, Schema.STRING_SCHEMA, streamUuidNotAUuid, schema, ocpiLocationPayload, 0L, timestamp, TimestampType.CREATE_TIME);

        Exception e = assertThrows(PartitionException.class, () -> {
            partitioner.encodePartition(ocpiSessionRecord);
        });

        assertEquals("Key is not a valid uuid, it therefore probably not a stream id", e.getMessage());
    }

    @Test
    public void testIncorrectTimestampFormat() throws Exception {
        // Top level config
        Map<String, Object> config = new HashMap<>();
        config.put(StorageCommonConfig.DIRECTORY_DELIM_CONFIG, StorageCommonConfig.DIRECTORY_DELIM_DEFAULT);

        // Configure the partitioner
        EVAnalyticsOcpiPartitioner<String> partitioner = new EVAnalyticsOcpiPartitioner<>();
        partitioner.configure(config);

        String streamUuid = "1e962902-65ae-4346-bb8d-d2206d6dc852";
        String entityId = "entity-1234";

        String timeZoneString = (String) config.get(PartitionerConfig.TIMEZONE_CONFIG);
        int YYYY = 2022;
        int MM = 6;
        int DD = 9;
        int HH = 7;
        long timestamp = new DateTime(YYYY, MM, DD, HH, 0, 0, 0, DateTimeZone.forID(timeZoneString)).getMillis();
        String payloadTimestamp = String.format("hey hey - %d-%02d-%02dla-la-la-la-la%02d:12:34Z", YYYY, MM, DD, HH);
        String ocpiLocationPayload = String.format(
                "{\"id\":\"%s\",\"type\":\"EVChargingStation\",\"status\":{\"type\":\"Property\",\"value\":\"AVAILABLE\"},\"address\":{\"type\":\"Property\",\"value\":{\"type\":\"PostalAddress\",\"postalCode\":\"N15 6BT\",\"streetAddress\":\"Grovelands Road\",\"addressCountry\":\"GBR\",\"addressLocality\":\"Haringey\"}},\"voltage\":{\"type\":\"Property\",\"value\":230},\"amperage\":{\"type\":\"Property\",\"value\":23},\"location\":{\"type\":\"GeoProperty\",\"value\":{\"type\":\"Point\",\"coordinates\":[-0.06414,51.578497]}},\"operator\":{\"type\":\"Property\",\"value\":\"char.gy\"},\"timestamp\":{\"type\":\"Property\",\"value\":\"%s\"},\"powerOutput\":{\"type\":\"Property\",\"value\":52.9},\"chargingType\":{\"type\":\"Property\",\"value\":\"rapid\"},\"dateModified\":{\"type\":\"Property\",\"value\":\"2021-05-07T06:06:30Z\"},\"socketNumber\":{\"type\":\"Property\",\"value\":1},\"commissionDate\":{\"type\":\"Property\",\"value\":\"\"},\"decommissionDate\":{\"type\":\"Property\",\"value\":\"\"},\"@context\":[\"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld\",\"https://raw.githubusercontent.com/smart-data-models/dataModel.Transportation/master/context.jsonld\"]}",
                entityId,
                payloadTimestamp
        );

        Schema schema = this.createSchemaWithTimestampField();
        SinkRecord ocpiSessionRecord = new SinkRecord("test-ocpi-session-topic", 13, Schema.STRING_SCHEMA, streamUuid, schema, ocpiLocationPayload, 0L, timestamp, TimestampType.CREATE_TIME);

        Exception e = assertThrows(PartitionException.class, () -> {
            partitioner.encodePartition(ocpiSessionRecord);
        });

        assertTrue(e.getMessage().contains("Could not parse YYYY-MM/DD/HH values from timestamp"));
    }
}
