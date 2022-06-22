package io.confluent.connect.s3.extensions;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.sink.SinkRecord;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import io.confluent.connect.storage.common.SchemaGenerator;
import io.confluent.connect.storage.common.StorageCommonConfig;

// Takes inspiration from
// https://stackoverflow.com/questions/57499274/implementing-a-kafka-connect-custom-partitioner

// Original DefaultPartitioner code
// https://github.com/confluentinc/kafka-connect-storage-common/blob/master/partitioner/src/main/java/io/confluent/connect/storage/partitioner/DefaultPartitioner.java
// Partitioner interface
// https://github.com/confluentinc/kafka-connect-storage-common/blob/master/partitioner/src/main/java/io/confluent/connect/storage/partitioner/Partitioner.java

// Original TimeBasedPartitioner code
// https://github.com/confluentinc/kafka-connect-storage-common/blob/master/partitioner/src/main/java/io/confluent/connect/storage/partitioner/TimeBasedPartitioner.java
// Maybe it's best to extend DefaultPartitioner<T> as above
// TimeBasedPartitioner interface
//

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
        // Get streamUuid from the key of the message
        // Decode value of sinkRecord as JSON
        // If it's not JSON, throw an exception?
        //    somehow exit gracefully
        // get entityId from the Json.value of 'id'
        // from the 'timestamp' Json.value, of form '2021-08-31T17:24:13Z' (zulu time)
        //    get YYYY-MM, create partition
        //      get DD, create partition
        //        get HH, create partition

        // Use jackson to work with JSON payloads, the lib is already imported

        // for inspiration, see:
        // https://stackoverflow.com/questions/57499274/implementing-a-kafka-connect-custom-partitioner
        // for jackson, see:
        // https://www.tutorialspoint.com/jackson/jackson_quick_guide.htm
        log.info("encoding partition with EVAnalyticsOcpiPartitioner...");

        String value = sinkRecord.value().toString();
        String streamUuid = sinkRecord.key().toString();

        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        try{
            log.info("Mapping record value into object...");
            OcpiPayload ocpiPayload = mapper.readValue(value, OcpiPayload.class);

            System.out.println(ocpiPayload);

            value = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(ocpiPayload);

            String entityId = ocpiPayload.getId();
            String timestamp = ocpiPayload.getTimestamp();
            // timestamp format is e.g. "2021-08-31T17:24:13Z"
            String [] splitTimestamp = timestamp.split("-");
            String YYYY = splitTimestamp[0];
            String MM = splitTimestamp[1];
            String [] dayTime = splitTimestamp[2].split("T");
            String DD = dayTime[0];
            String HH = dayTime[1].substring(0, 2);

            log.info(value);

            return String.format("%s/%s/%s-%s/%s/%s", streamUuid, entityId, YYYY, MM, DD, HH);
        }
        catch (JacksonException e) { e.printStackTrace(); }
        // How do we handle errors?
        return "TODO/what/to/do/when/it/errors";
    }

    @Override
    public List<T> partitionFields() {
        // TODO: what are partition fields?
        log.info("Returning partition fields from EVAnalyticsOcpiPartitioner...");
        return super.partitionFields();
    }
}
