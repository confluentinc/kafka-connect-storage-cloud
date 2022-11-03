package io.confluent.connect.s3;

import io.confluent.common.utils.MockTime;
import io.confluent.connect.s3.format.avro.AvroFormat;
import io.confluent.connect.s3.hooks.KafkaPreCommitHook;
import io.confluent.connect.s3.util.FileUtils;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.confluent.connect.s3.DataWriterAvroTest.EXTENSION;
import static org.apache.kafka.common.utils.Time.SYSTEM;
import static org.mockito.ArgumentMatchers.any;

public class KafkaPreCommitHookTest extends DataWriterTestBase<AvroFormat> {

    public KafkaPreCommitHookTest() {
        super(AvroFormat.class);
    }

    private static final Set<TopicPartition> partitions = new HashSet<>(Arrays.asList(TOPIC_PARTITION, TOPIC_PARTITION2));

    @Test
    public void testPreCommitKafkaHookProducesFiles() throws Exception {
        localProps.put(S3SinkConnectorConfig.FLUSH_SIZE_CONFIG, "3");

        setUp();
        KafkaPreCommitHook kafkaPreCommitSend = Mockito.mock(KafkaPreCommitHook.class);
        Mockito.when(kafkaPreCommitSend.execute(any())).thenAnswer(i -> i.getArguments()[0]);
        InOrder inOrder = Mockito.inOrder(kafkaPreCommitSend);
        task = new S3SinkTask(connectorConfig, context, storage, partitioner, kafkaPreCommitSend, format, SYSTEM_TIME);

        List<SinkRecord> sinkRecords1 = createRecordsInterleaved(3 * partitions.size(), 0, partitions);

        task.put(sinkRecords1);
        task.preCommit(null);

        Map<TopicPartition, Set<String>> expectedFilesSet1 = getExpectedFiles(0, partitions);
        inOrder.verify(kafkaPreCommitSend).execute(expectedFilesSet1);

        List<SinkRecord> sinkRecords2 = createRecordsInterleaved(2 * partitions.size(), 3, partitions);

        task.put(sinkRecords2);
        task.preCommit(null);

        // Actual values are null, we set to negative for the verifier.
        inOrder.verify(kafkaPreCommitSend).execute(new HashMap<>());

        List<SinkRecord> sinkRecords3 = createRecordsInterleaved(partitions.size(), 5, partitions);

        task.put(sinkRecords3);
        task.preCommit(null);

        Map<TopicPartition, Set<String>> expectedFilesSet3 = getExpectedFiles(3, partitions);
        inOrder.verify(kafkaPreCommitSend).execute(expectedFilesSet3);

        List<SinkRecord> sinkRecords4 = createRecordsInterleaved(3 * partitions.size(), 6, partitions);

        // Include all the records beside the last one in the second partition
        task.put(sinkRecords4.subList(0, 3 * partitions.size() - 1));
        task.preCommit(null);

        Map<TopicPartition, Set<String>> expectedFilesSet4 = getExpectedFiles(6, Collections.singleton(TOPIC_PARTITION));
        Mockito.verify(kafkaPreCommitSend).execute(expectedFilesSet4);

        task.close(partitions);
        task.stop();
    }

//    @Test
//    public void testPreCommitOnRotateScheduleTime() throws Exception {
//        // Do not roll on size, only based on time.
//        localProps.put(S3SinkConnectorConfig.FLUSH_SIZE_CONFIG, "1000");
//        localProps.put(
//                S3SinkConnectorConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG,
//                String.valueOf(TimeUnit.HOURS.toMillis(1))
//        );
//        setUp();
//
//        // Define the partitioner
//        TimeBasedPartitioner<?> partitioner = new TimeBasedPartitioner<>();
//        parsedConfig.put(PartitionerConfig.PARTITION_DURATION_MS_CONFIG, TimeUnit.DAYS.toMillis(1));
//        parsedConfig.put(
//                PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
//                TopicPartitionWriterTest.MockedWallclockTimestampExtractor.class.getName()
//        );
//        partitioner.configure(parsedConfig);
//
//        MockTime time = ((TopicPartitionWriterTest.MockedWallclockTimestampExtractor) partitioner
//                .getTimestampExtractor()).time;
//        // Bring the clock to present.
//        time.sleep(SYSTEM.milliseconds());
//
//        List<SinkRecord> sinkRecords = createRecordsWithTimestamp2(
//                2,
//                0,
//                Collections.singleton(new TopicPartition(TOPIC, PARTITION)),
//                SYSTEM.milliseconds()
//        );
//
//        sinkRecords.addAll(createRecordsWithTimestamp2(
//                1,
//                2,
//                Collections.singleton(new TopicPartition(TOPIC, PARTITION)),
//                SYSTEM.milliseconds() - 25 *60 *60 *1000
//        ));
//
//        sinkRecords.addAll(createRecordsWithTimestamp2(
//                1,
//                3,
//                Collections.singleton(new TopicPartition(TOPIC, PARTITION)),
//                SYSTEM.milliseconds()
//        ));
//
//        task = new S3SinkTask(connectorConfig, context, storage, partitioner, format, time);
//
//        // Perform write
//        task.put(sinkRecords);
//
//        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = task.preCommit(null);
//
//        long[] validOffsets1 = {-1, -1};
//
////        verifyOffsets(offsetsToCommit, validOffsets1, context.assignment());
//
//        // 1 hour + 10 minutes
//        time.sleep(TimeUnit.HOURS.toMillis(1) + TimeUnit.MINUTES.toMillis(10));
//
//        long[] validOffsets2 = {3, -1};
//
//        // Since rotation depends on scheduled intervals, flush will happen even when no new records
//        // are returned.
//        task.put(Collections.<SinkRecord>emptyList());
//        offsetsToCommit = task.preCommit(null);
//
////        verifyOffsets(offsetsToCommit, validOffsets2, context.assignment());
//
//        task.close(context.assignment());
//        task.stop();
//    }
//
//    private List<SinkRecord> createRecordsWithTimestamp2(
//            int size,
//            long startOffset,
//            Set<TopicPartition> partitions,
//            long milli
//    ) {
//        String key = "key";
//        Schema schema = createSchema();
//        Struct record = createRecord(schema);
//
//        List<SinkRecord> sinkRecords = new ArrayList<>();
//        for (TopicPartition tp : partitions) {
//            for (long offset = startOffset; offset < startOffset + size; ++offset) {
//                sinkRecords.add(new SinkRecord(
//                        TOPIC,
//                        tp.partition(),
//                        Schema.STRING_SCHEMA,
//                        key,
//                        schema,
//                        record,
//                        offset,
//                        milli,
//                        TimestampType.CREATE_TIME
//                ));
//            }
//        }
//        return sinkRecords;
//    }

    private Map<TopicPartition, Set<String>> getExpectedFiles(long startOffset, Collection<TopicPartition> partitions) {
        Map<TopicPartition, Set<String>> expectedFiles = new HashMap<>();
        for (TopicPartition tp : partitions) {
            Set<String> files = expectedFiles.getOrDefault(tp, new HashSet<>());
            files.add(FileUtils.fileKeyToCommit(
                    topicsDir,
                    getDirectory(tp.topic(), tp.partition()),
                    tp,
                    startOffset,
                    EXTENSION, ZERO_PAD_FMT));
            expectedFiles.put(tp, files);
        }
        return expectedFiles;
    }

    protected List<SinkRecord> createRecordsInterleaved(
            int size,
            long startOffset,
            Set<TopicPartition> partitionSet
    ) {
        String key = "key";
        Schema schema = createSchema();
        Struct record = createRecord(schema);

        Collection<TopicPartition> partitions = sortedPartitions(partitionSet);

        List<SinkRecord> sinkRecords = new ArrayList<>();
        for (long offset = startOffset, total = 0; total < size; ++offset) {
            for (TopicPartition tp : partitions) {
                sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), Schema.STRING_SCHEMA, key, schema, record, offset));
                if (++total >= size) {
                    break;
                }
            }
        }
        return sinkRecords;
    }

    @Override
    protected String getFileExtension() {
        return EXTENSION;
    }

    @Override
    protected void verify(List<SinkRecord> sinkRecords, long[] validOffsets, Set<TopicPartition> partitions) {

    }

    @Override
    protected List<SinkRecord> createGenericRecords(int count, long firstOffset) {
        return null;
    }

    @Override
    protected String getDirectory(String topic, int partition) {
        String encodedPartition = "partition=" + partition;
        return partitioner.generatePartitionedPath(topic, encodedPartition);
    }

}
