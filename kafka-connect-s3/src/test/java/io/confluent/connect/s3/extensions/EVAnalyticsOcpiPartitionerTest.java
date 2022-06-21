package io.confluent.connect.s3.extensions;

public class EVAnalyticsOcpiPartitionerTest {
    // Try to TDD it!
    // Partitioner should:
    // - Take in a valid sessions payload / message
    // - Check that it has the offering_uuid somewhere (ideally in the key of the message)
    // - Check that it has id in the payload (needs JSON parser) (for entityId)
    // - Check that it has timestamp in the payload in the right format to be parsed (zulu time)
    // - Produce the correct path from the 'String encodePartition(SinkRecord sinkRecord)' function
    // - Should take in a valid config object and return a valid EVAnalyticsOcpiPartitioner class
}
