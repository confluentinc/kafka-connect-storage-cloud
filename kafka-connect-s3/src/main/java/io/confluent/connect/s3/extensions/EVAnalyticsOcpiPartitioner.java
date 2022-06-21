package io.confluent.connect.s3.extensions;

import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;

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
    // Get streamUuid from the key of the message
    // get entityId from the value of 'id'
    // from the 'timestamp' value, of form '2021-08-31T17:24:13Z' (zulu time)
    //    get YYYY-MM, create partition
    //      get DD, create partition
    //        get HH, create partition
}
