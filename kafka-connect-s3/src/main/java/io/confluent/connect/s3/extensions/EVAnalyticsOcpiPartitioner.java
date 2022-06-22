package io.confluent.connect.s3.extensions;

import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.sink.SinkRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
        log.info("Configuring...");
    }

    @Override
    public String encodePartition(SinkRecord sinkRecord) {
        // see:
        // https://stackoverflow.com/questions/57499274/implementing-a-kafka-connect-custom-partitioner
        log.info("encoding partition...");
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
        return "1234/1234/2022-07/01/00";
    }

    @Override
    public List<T> partitionFields() {
        log.info("Returning partition fields...");
        return new ArrayList<T>();
    }
}
