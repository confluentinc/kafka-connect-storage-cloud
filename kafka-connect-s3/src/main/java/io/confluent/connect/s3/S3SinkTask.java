/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.s3;

import com.amazonaws.AmazonClientException;
import io.confluent.connect.s3.S3SinkConnectorConfig.BehaviorOnNullValues;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import io.confluent.common.utils.SystemTime;
import io.confluent.common.utils.Time;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.s3.util.Version;
import io.confluent.connect.storage.StorageFactory;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;

public class S3SinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(S3SinkTask.class);

  private S3SinkConnectorConfig connectorConfig;
  private String url;
  private long timeoutMs;
  private S3Storage storage;
  private final Map<TopicPartition, TopicPartitionWriter> topicPartitionWriters;
  private Partitioner<?> partitioner;
  private Format<S3SinkConnectorConfig, String> format;
  private RecordWriterProvider<S3SinkConnectorConfig> writerProvider;
  private final Time time;

  /**
   * No-arg constructor. Used by Connect framework.
   */
  public S3SinkTask() {
    // no-arg constructor required by Connect framework.
    topicPartitionWriters = new HashMap<>();
    time = new SystemTime();
  }

  // visible for testing.
  S3SinkTask(S3SinkConnectorConfig connectorConfig, SinkTaskContext context, S3Storage storage,
             Partitioner<?> partitioner, Format<S3SinkConnectorConfig, String> format,
             Time time) throws Exception {
    this.topicPartitionWriters = new HashMap<>();
    this.connectorConfig = connectorConfig;
    this.context = context;
    this.storage = storage;
    this.partitioner = partitioner;
    this.format = format;
    this.time = time;

    url = connectorConfig.getString(StorageCommonConfig.STORE_URL_CONFIG);
    writerProvider = this.format.getRecordWriterProvider();

    open(context.assignment());
    log.info("Started S3 connector task with assigned partitions {}",
        topicPartitionWriters.keySet()
    );
  }

  public void start(Map<String, String> props) {
    try {
      connectorConfig = new S3SinkConnectorConfig(props);
      url = connectorConfig.getString(StorageCommonConfig.STORE_URL_CONFIG);
      timeoutMs = connectorConfig.getLong(S3SinkConnectorConfig.RETRY_BACKOFF_CONFIG);

      @SuppressWarnings("unchecked")
      Class<? extends S3Storage> storageClass =
          (Class<? extends S3Storage>)
              connectorConfig.getClass(StorageCommonConfig.STORAGE_CLASS_CONFIG);
      storage = StorageFactory.createStorage(
          storageClass,
          S3SinkConnectorConfig.class,
          connectorConfig,
          url
      );
      if (!storage.bucketExists()) {
        throw new DataException("No-existent S3 bucket: " + connectorConfig.getBucketName());
      }

      writerProvider = newFormat().getRecordWriterProvider();
      partitioner = newPartitioner(connectorConfig);

      open(context.assignment());
      log.info("Started S3 connector task with assigned partitions: {}",
          topicPartitionWriters.keySet()
      );
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException
        | InvocationTargetException | NoSuchMethodException e) {
      throw new ConnectException("Reflection exception: ", e);
    } catch (AmazonClientException e) {
      throw new ConnectException(e);
    }
  }

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void open(Collection<TopicPartition> partitions) {
    for (TopicPartition tp : partitions) {
      topicPartitionWriters.put(tp, newTopicPartitionWriter(tp));
    }
  }

  @SuppressWarnings("unchecked")
  private Format<S3SinkConnectorConfig, String> newFormat()
      throws ClassNotFoundException, IllegalAccessException, InstantiationException,
             InvocationTargetException, NoSuchMethodException {
    Class<Format<S3SinkConnectorConfig, String>> formatClass =
        (Class<Format<S3SinkConnectorConfig, String>>) connectorConfig.getClass(
            S3SinkConnectorConfig.FORMAT_CLASS_CONFIG
        );
    return formatClass.getConstructor(S3Storage.class).newInstance(storage);
  }

  private Partitioner<?> newPartitioner(S3SinkConnectorConfig config)
      throws ClassNotFoundException, IllegalAccessException, InstantiationException {

    @SuppressWarnings("unchecked")
    Class<? extends Partitioner<?>> partitionerClass =
        (Class<? extends Partitioner<?>>)
            config.getClass(PartitionerConfig.PARTITIONER_CLASS_CONFIG);

    Partitioner<?> partitioner = partitionerClass.newInstance();

    Map<String, Object> plainValues = new HashMap<>(config.plainValues());
    Map<String, ?> originals = config.originals();
    for (String originalKey : originals.keySet()) {
      if (!plainValues.containsKey(originalKey)) {
        // pass any additional configs down to the partitioner so that custom partitioners can
        // have their own configs
        plainValues.put(originalKey, originals.get(originalKey));
      }
    }
    partitioner.configure(plainValues);

    return partitioner;
  }

  @Override
  public void put(Collection<SinkRecord> records) throws ConnectException {
    for (SinkRecord record : records) {
      String topic = record.topic();
      int partition = record.kafkaPartition();
      TopicPartition tp = new TopicPartition(topic, partition);

      if (maybeSkipOnNullValue(record)) {
        continue;
      }
      topicPartitionWriters.get(tp).buffer(record);
    }
    if (log.isDebugEnabled()) {
      log.debug("Read {} records from Kafka", records.size());
    }

    for (TopicPartition tp : topicPartitionWriters.keySet()) {
      TopicPartitionWriter writer = topicPartitionWriters.get(tp);
      try {
        writer.write();
      } catch (RetriableException e) {
        log.error("Exception on topic partition {}: ", tp, e);
        Long currentStartOffset = writer.currentStartOffset();
        if (currentStartOffset != null) {
          context.offset(tp, currentStartOffset);
        }
        context.timeout(timeoutMs);
        writer = newTopicPartitionWriter(tp);
        writer.failureTime(time.milliseconds());
        topicPartitionWriters.put(tp, writer);
      }
    }
  }

  private boolean maybeSkipOnNullValue(SinkRecord record) {
    if (record.value() == null) {
      if (connectorConfig.nullValueBehavior()
          .equalsIgnoreCase(BehaviorOnNullValues.IGNORE.toString())) {
        log.debug(
            "Null valued record from topic '{}', partition {} and offset {} was skipped.",
            record.topic(),
            record.kafkaPartition(),
            record.kafkaOffset()
        );
        return true;
      } else {
        throw new ConnectException("Null valued records are not writeable with current "
            + S3SinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG + " 'settings.");
      }
    }
    return false;
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    // No-op. The connector is managing the offsets.
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
      Map<TopicPartition, OffsetAndMetadata> offsets
  ) {
    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
    for (TopicPartition tp : topicPartitionWriters.keySet()) {
      Long offset = topicPartitionWriters.get(tp).getOffsetToCommitAndReset();
      if (offset != null) {
        log.trace("Forwarding to framework request to commit offset: {} for {}", offset, tp);
        offsetsToCommit.put(tp, new OffsetAndMetadata(offset));
      }
    }
    return offsetsToCommit;
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
    for (TopicPartition tp : topicPartitionWriters.keySet()) {
      try {
        topicPartitionWriters.get(tp).close();
      } catch (ConnectException e) {
        log.error("Error closing writer for {}. Error: {}", tp, e.getMessage());
      }
    }
    topicPartitionWriters.clear();
  }

  @Override
  public void stop() {
    try {
      if (storage != null) {
        storage.close();
      }
    } catch (Exception e) {
      throw new ConnectException(e);
    }
  }

  // Visible for testing
  TopicPartitionWriter getTopicPartitionWriter(TopicPartition tp) {
    return topicPartitionWriters.get(tp);
  }

  private TopicPartitionWriter newTopicPartitionWriter(TopicPartition tp) {
    return new TopicPartitionWriter(
        tp,
        storage,
        writerProvider,
        partitioner,
        connectorConfig,
        context,
        time
    );
  }

  // Visible for testing
  Format<S3SinkConnectorConfig, String> getFormat() {
    return format;
  }
}
