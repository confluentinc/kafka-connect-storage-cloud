package io.confluent.connect.s3.extensions;

import io.confluent.connect.storage.StorageSinkTestBase;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.errors.PartitionException;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class UdxStreamPartitionerTest extends StorageSinkTestBase {
    // Partitioner should:
    // - Take in a valid sessions / location payload
    // - Check that it has the offering_uuid somewhere (ideally in the key of the message)
    // - Check that it has id in the payload (needs JSON parser) (for entityId)
    // - Check that it has timestamp in the payload in the right format to be parsed (zulu time)
    // - Produce the correct path from the 'String encodePartition(SinkRecord sinkRecord)' function

    private static final String UDX_PARTITION_FORMAT_FOR_INTS = "streamUuid=%s/entityId=%s/%d-%02d/day=%02d/hour=%02d";

    private SinkRecord generateUdxPayloadRecordNullKey(String streamUuid, String payload, Long timestamp) {
        Headers headers = new ConnectHeaders();
        headers.add("offering_uuid", streamUuid, STRING_SCHEMA);
        Schema schema = this.createSchemaWithTimestampField();
        return new SinkRecord(
                "test-ocpi-session-topic",
                13,
                STRING_SCHEMA,
                null,
                schema,
                payload,
                0L,
                timestamp,
                TimestampType.CREATE_TIME,
                headers
        );
    }

    private SinkRecord generateUdxPayloadRecordNoHeader(String payload, Long timestamp) {
        Schema schema = this.createSchemaWithTimestampField();
        return new SinkRecord(
                "test-ocpi-session-topic",
                13,
                STRING_SCHEMA,
                null,
                schema,
                payload,
                0L,
                timestamp,
                TimestampType.CREATE_TIME
        );
    }

    @Test
    public void testProducesPathFromValidFlatTimestampPayload() {
        // Top level config
        Map<String, Object> config = new HashMap<>();
        config.put(StorageCommonConfig.DIRECTORY_DELIM_CONFIG, StorageCommonConfig.DIRECTORY_DELIM_DEFAULT);



        // Configure the partitioner
        UdxStreamPartitioner<String> partitioner = new UdxStreamPartitioner<>();
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
        SinkRecord ocpiSessionRecord = generateUdxPayloadRecordNullKey(
                streamUuid,
                ocpiSessionPayload,
                timestamp
        );

        // Run the partitioner
        String encodedPartition = partitioner.encodePartition(ocpiSessionRecord);

        // Assert that the filepath is correct
        String expectedPath = String.format(UDX_PARTITION_FORMAT_FOR_INTS, streamUuid, entityId, YYYY, MM, DD, HH);
        assertThat(encodedPartition, is(expectedPath));
    }

    @Test
    public void testProducesPathFromValidNestedTimestampPayload() {
        // Top level config
        Map<String, Object> config = new HashMap<>();
        config.put(StorageCommonConfig.DIRECTORY_DELIM_CONFIG, StorageCommonConfig.DIRECTORY_DELIM_DEFAULT);

        // Configure the partitioner
        UdxStreamPartitioner<String> partitioner = new UdxStreamPartitioner<>();
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

        SinkRecord ocpiSessionRecord = generateUdxPayloadRecordNullKey(
                streamUuid,
                ocpiLocationPayload,
                timestamp
        );

        // Run the partitioner
        String encodedPartition = partitioner.encodePartition(ocpiSessionRecord);

        // Assert that the filepath is correct
        String expectedPath = String.format(UDX_PARTITION_FORMAT_FOR_INTS, streamUuid, entityId, YYYY, MM, DD, HH);
        assertThat(encodedPartition, is(expectedPath));
    }

    @Test
    public void testProducesPathFromValidNestedTimestampIntPayload() {
        // Top level config
        Map<String, Object> config = new HashMap<>();
        config.put(StorageCommonConfig.DIRECTORY_DELIM_CONFIG, StorageCommonConfig.DIRECTORY_DELIM_DEFAULT);

        // Configure the partitioner
        UdxStreamPartitioner<String> partitioner = new UdxStreamPartitioner<>();
        partitioner.configure(config);

        String streamUuid = "1e962902-65ae-4346-bb8d-d2206d6dc852";
        String entityId = "entity-1234";

        String timeZoneString = (String) config.get(PartitionerConfig.TIMEZONE_CONFIG);
        int YYYY = 2022;
        int MM = 6;
        int DD = 9;
        int HH = 7;
        long timestamp = new DateTime(YYYY, MM, DD, HH, 0, 0, 0, DateTimeZone.forID(timeZoneString)).getMillis();
        String stringTimestampISO8601 = String.format("%d-%02d-%02dT%02d:12:34Z", YYYY, MM, DD, HH);
        // TODO: how can we know if timestamps in arbitrary payloads are in millis or s?
        long payloadTimestampAsUnix = new DateTime(stringTimestampISO8601).getMillis();
        DateTime test = new DateTime(payloadTimestampAsUnix);
        String ocpiLocationPayload = String.format(
                "{\"id\":\"%s\",\"type\":\"EVChargingStation\",\"status\":{\"type\":\"Property\",\"value\":\"AVAILABLE\"},\"address\":{\"type\":\"Property\",\"value\":{\"type\":\"PostalAddress\",\"postalCode\":\"N15 6BT\",\"streetAddress\":\"Grovelands Road\",\"addressCountry\":\"GBR\",\"addressLocality\":\"Haringey\"}},\"voltage\":{\"type\":\"Property\",\"value\":230},\"amperage\":{\"type\":\"Property\",\"value\":23},\"location\":{\"type\":\"GeoProperty\",\"value\":{\"type\":\"Point\",\"coordinates\":[-0.06414,51.578497]}},\"operator\":{\"type\":\"Property\",\"value\":\"char.gy\"},\"timestamp\":{\"type\":\"Property\",\"value\":\"%d\"},\"powerOutput\":{\"type\":\"Property\",\"value\":52.9},\"chargingType\":{\"type\":\"Property\",\"value\":\"rapid\"},\"dateModified\":{\"type\":\"Property\",\"value\":\"2021-05-07T06:06:30Z\"},\"socketNumber\":{\"type\":\"Property\",\"value\":1},\"commissionDate\":{\"type\":\"Property\",\"value\":\"\"},\"decommissionDate\":{\"type\":\"Property\",\"value\":\"\"},\"@context\":[\"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld\",\"https://raw.githubusercontent.com/smart-data-models/dataModel.Transportation/master/context.jsonld\"]}",
                entityId,
                payloadTimestampAsUnix
        );

        SinkRecord ocpiSessionRecord = generateUdxPayloadRecordNullKey(
                streamUuid,
                ocpiLocationPayload,
                timestamp
        );

        // Run the partitioner
        String encodedPartition = partitioner.encodePartition(ocpiSessionRecord);

        // Assert that the filepath is correct
        String expectedPath = String.format(UDX_PARTITION_FORMAT_FOR_INTS, streamUuid, entityId, YYYY, MM, DD, HH);
        assertThat(encodedPartition, is(expectedPath));
    }

    @Test
    public void testCannotParseJsonPayload() {
        // Top level config
        Map<String, Object> config = new HashMap<>();
        config.put(StorageCommonConfig.DIRECTORY_DELIM_CONFIG, StorageCommonConfig.DIRECTORY_DELIM_DEFAULT);

        // Configure the partitioner
        UdxStreamPartitioner<String> partitioner = new UdxStreamPartitioner<>();
        partitioner.configure(config);

        String streamUuid = "1e962902-65ae-4346-bb8d-d2206d6dc852";

        String timeZoneString = (String) config.get(PartitionerConfig.TIMEZONE_CONFIG);
        int YYYY = 2022;
        int MM = 6;
        int DD = 9;
        int HH = 7;
        long timestamp = new DateTime(YYYY, MM, DD, HH, 0, 0, 0, DateTimeZone.forID(timeZoneString)).getMillis();
        String ocpiLocationPayload = "{\"not\":\"validAtAll\"}";
        SinkRecord ocpiSessionRecord = generateUdxPayloadRecordNullKey(
                streamUuid,
                ocpiLocationPayload,
                timestamp
        );
        String encodedPartition = partitioner.encodePartition(ocpiSessionRecord);
        assertThat(encodedPartition, is(String.format("invalidIdOrTimestamp/streamUuid=%s", streamUuid)));
    }

    @Test
    public void testNoValidUuidInHeaders() {
        // Top level config
        Map<String, Object> config = new HashMap<>();
        config.put(StorageCommonConfig.DIRECTORY_DELIM_CONFIG, StorageCommonConfig.DIRECTORY_DELIM_DEFAULT);

        // Configure the partitioner
        UdxStreamPartitioner<String> partitioner = new UdxStreamPartitioner<>();
        partitioner.configure(config);

        String streamUuidNotAUuid = "not-a-uuid-la-la-la";

        String timeZoneString = (String) config.get(PartitionerConfig.TIMEZONE_CONFIG);
        int YYYY = 2022;
        int MM = 6;
        int DD = 9;
        int HH = 7;
        long timestamp = new DateTime(YYYY, MM, DD, HH, 0, 0, 0, DateTimeZone.forID(timeZoneString)).getMillis();
        String ocpiLocationPayload = "{\"not\":\"validEvenIfTheStreamUuidWasValid\"}";
        SinkRecord ocpiSessionRecord = generateUdxPayloadRecordNullKey(
                streamUuidNotAUuid,
                ocpiLocationPayload,
                timestamp
        );

        String encodedPartition = partitioner.encodePartition(ocpiSessionRecord);
        assertThat(encodedPartition, is(String.format("invalidIdOrTimestamp/streamUuid=%s", streamUuidNotAUuid)));
    }

    // TODO: test noValidUUid in headers
    // maybe just print a warning here and add it to another partition, /*/invalidStreamUUid/id/YYYY...
    @Test
    public void testNoUuidInHeadersCorruptedPayload() {
        // Top level config
        Map<String, Object> config = new HashMap<>();
        config.put(StorageCommonConfig.DIRECTORY_DELIM_CONFIG, StorageCommonConfig.DIRECTORY_DELIM_DEFAULT);

        // Configure the partitioner
        UdxStreamPartitioner<String> partitioner = new UdxStreamPartitioner<>();
        partitioner.configure(config);

        String timeZoneString = (String) config.get(PartitionerConfig.TIMEZONE_CONFIG);
        int YYYY = 2022;
        int MM = 6;
        int DD = 9;
        int HH = 7;
        long timestamp = new DateTime(YYYY, MM, DD, HH, 0, 0, 0, DateTimeZone.forID(timeZoneString)).getMillis();
        String ocpiLocationPayload = "{\"not\":\"validEvenIfTheStreamUuidWasValid\"}";
        SinkRecord ocpiSessionRecord = generateUdxPayloadRecordNoHeader(
                ocpiLocationPayload,
                timestamp
        );

        String encodedPartition = partitioner.encodePartition(ocpiSessionRecord);
        assertThat(encodedPartition, is(String.format("invalidIdOrTimestamp/streamUuid=noStreamIdFound")));
    }

    @Test
    public void testIncorrectTimestampFormat() {
        // Top level config
        Map<String, Object> config = new HashMap<>();
        config.put(StorageCommonConfig.DIRECTORY_DELIM_CONFIG, StorageCommonConfig.DIRECTORY_DELIM_DEFAULT);

        // Configure the partitioner
        UdxStreamPartitioner<String> partitioner = new UdxStreamPartitioner<>();
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

        SinkRecord ocpiSessionRecord = generateUdxPayloadRecordNullKey(
                streamUuid,
                ocpiLocationPayload,
                timestamp
        );

        String encodedPartition = partitioner.encodePartition(ocpiSessionRecord);
        assertThat(encodedPartition, is(String.format("invalidIdOrTimestamp/streamUuid=%s/entityId=%s/%s", streamUuid, entityId, payloadTimestamp)));
    }
}
