package io.confluent.connect.s3;

import io.confluent.connect.s3.format.avro.AvroFormat;
import io.confluent.connect.s3.hooks.BlockingKafkaPostCommitHook;
import io.confluent.connect.s3.util.FileUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.confluent.connect.s3.DataWriterAvroTest.EXTENSION;
import static org.mockito.ArgumentMatchers.any;

public class BlockingKafkaPostCommitHookTest extends DataWriterTestBase<AvroFormat> {

  public BlockingKafkaPostCommitHookTest() {
    super(AvroFormat.class);
  }

  private static final Set<TopicPartition> partitions = new HashSet<>(Arrays.asList(TOPIC_PARTITION, TOPIC_PARTITION2));

  @Test
  public void testPostCommitKafkaHookProducesFiles() throws Exception {
    localProps.put(S3SinkConnectorConfig.FLUSH_SIZE_CONFIG, "3");

    setUp();
    BlockingKafkaPostCommitHook kafkaPostCommitSend = Mockito.mock(BlockingKafkaPostCommitHook.class);
    InOrder inOrder = Mockito.inOrder(kafkaPostCommitSend);
    task = new S3SinkTask(connectorConfig, context, storage, partitioner, kafkaPostCommitSend, format, SYSTEM_TIME);

    List<SinkRecord> sinkRecords1 = createRecordsInterleaved(3 * partitions.size(), 0, partitions);

    task.put(sinkRecords1);
    task.preCommit(null);

    inOrder.verify(kafkaPostCommitSend).put(getExpectedFiles(0, TOPIC_PARTITION), any());
    inOrder.verify(kafkaPostCommitSend).put(getExpectedFiles(0, TOPIC_PARTITION2), any());

    List<SinkRecord> sinkRecords2 = createRecordsInterleaved(2 * partitions.size(), 3, partitions);

    task.put(sinkRecords2);
    task.preCommit(null);

    // nothing to verify as no call is expected

    List<SinkRecord> sinkRecords3 = createRecordsInterleaved(partitions.size(), 5, partitions);

    task.put(sinkRecords3);
    task.preCommit(null);

    inOrder.verify(kafkaPostCommitSend).put(getExpectedFiles(3, TOPIC_PARTITION), any());
    inOrder.verify(kafkaPostCommitSend).put(getExpectedFiles(3, TOPIC_PARTITION2), any());

    List<SinkRecord> sinkRecords4 = createRecordsInterleaved(3 * partitions.size(), 6, partitions);

    // Include all the records beside the last one in the second partition
    task.put(sinkRecords4.subList(0, 3 * partitions.size() - 1));
    task.preCommit(null);

    inOrder.verify(kafkaPostCommitSend).put(getExpectedFiles(6, TOPIC_PARTITION), any());

    task.close(partitions);
    task.stop();
  }

  private List<String> getExpectedFiles(long startOffset, TopicPartition tp) {
    List<String> expectedFiles = new ArrayList<>();
    expectedFiles.add(FileUtils.fileKeyToCommit(
            topicsDir,
            getDirectory(tp.topic(), tp.partition()),
            tp,
            startOffset,
            EXTENSION, ZERO_PAD_FMT));
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
