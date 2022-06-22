package io.confluent.connect.s3.extensions;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import org.apache.kafka.connect.sink.SinkRecord;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

// Takes inspiration from
// https://stackoverflow.com/questions/57499274/implementing-a-kafka-connect-custom-partitioner

// Original DefaultPartitioner code
// https://github.com/confluentinc/kafka-connect-storage-common/blob/master/partitioner/src/main/java/io/confluent/connect/storage/partitioner/DefaultPartitioner.java
// Partitioner interface
// https://github.com/confluentinc/kafka-connect-storage-common/blob/master/partitioner/src/main/java/io/confluent/connect/storage/partitioner/Partitioner.java

// Original TimeBasedPartitioner code
// https://github.com/confluentinc/kafka-connect-storage-common/blob/master/partitioner/src/main/java/io/confluent/connect/storage/partitioner/TimeBasedPartitioner.java
// Maybe it's best to extend DefaultPartitioner<T> as above

public class EVAnalyticsOcpiPartitioner<T> extends DefaultPartitioner<T> {
    private static final Logger log = LoggerFactory.getLogger(EVAnalyticsOcpiPartitioner.class);

    @Override
    public void configure(Map<String, Object> config) {
        log.info("Configuring EVAnalyticsOcpiPartitioner...");
        super.configure(config);
    }

    @Override
    public String encodePartition(SinkRecord sinkRecord) {
        // streamUuid/entityId/YYYY-MM/DD/HH
        // Gets streamUuid from the key of the message
        // Decode value of sinkRecord as JSON using jackson
        // If it's not JSON, throw an exception?
        //    somehow exit gracefully
        // get entityId from the Json.value of 'id'
        // from the 'timestamp' Json.value, of form '2021-08-31T17:24:13Z' (zulu time)
        //    get YYYY-MM, create partition
        //      get DD, create partition
        //        get HH, create partition

        // for inspiration, see:
        // https://stackoverflow.com/questions/57499274/implementing-a-kafka-connect-custom-partitioner
        // for jackson, see:
        // https://www.tutorialspoint.com/jackson/jackson_quick_guide.htm
        log.info("encoding partition with EVAnalyticsOcpiPartitioner...");

        String value = sinkRecord.value().toString();
        String streamUuid = sinkRecord.key().toString();

        ObjectMapper mapper = new ObjectMapper();

        // We don't want to have to extend our POJO with every single
        // field that appears in a payload, we only care about the fields
        // that make up the partitions in S3, hence this FAIL_ON_UNKNOWN_PROPERTIES
        // setting
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        // the value of the timestamp can either be:
        // { timestamp: string } (in an OCPI session payload)
        // or
        // { timestamp: { type: 'Property', value: string } } (in an OCPI location payload)
        // We account for both with some sort of generic function...

        OcpiPayload ocpiPayload = null;

        try {
            // try to parse into sessions
            ocpiPayload = mapper.readValue(value, OcpiSessionsPayload.class);
            log.info("Mapped to OcpiSessionsPayload");
        } catch (JacksonException e) { log.warn("Could not parse payload into sessions POJO"); }

        try {
            // try to parse into location
            ocpiPayload = mapper.readValue(value, OcpiLocationsPayload.class);
            log.info("Mapped to OcpiLocationsPayload");
        } catch (JacksonException e) { log.warn("Could not parse payload into location POJO"); }

        if (ocpiPayload == null) {
            log.error("No ocpi mapping found, try again...");
            // TODO: exit gracefully here
        }

        log.info("Mapping record value into object...");
        log.info(ocpiPayload.toString());
        String entityId = ocpiPayload.getId();
        String timestamp = ocpiPayload.getTimestamp();
        // TODO: needs to ensure that id and timestamp are in the right format!
        // timestamp format is e.g. "2021-08-31T17:24:13Z"
        // There's definitely a better way to work with timestamps in Java
        String [] splitTimestamp = timestamp.split("-");
        String YYYY = splitTimestamp[0];
        String MM = splitTimestamp[1];
        String [] dayTime = splitTimestamp[2].split("T");
        String DD = dayTime[0];
        String HH = dayTime[1].substring(0, 2);

        return String.format("%s/%s/%s-%s/%s/%s", streamUuid, entityId, YYYY, MM, DD, HH);

        // How do we handle errors?
        // return "TODO/what/to/do/when/it/errors";
    }

    @Override
    public List<T> partitionFields() {
        // TODO: what are partition fields?
        log.info("Returning partition fields from EVAnalyticsOcpiPartitioner...");
        return super.partitionFields();
    }
}
