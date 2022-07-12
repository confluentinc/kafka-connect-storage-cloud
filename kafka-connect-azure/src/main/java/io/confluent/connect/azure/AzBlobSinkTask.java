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

package io.confluent.connect.azure;

import com.amazonaws.AmazonClientException;
import io.confluent.connect.azure.format.KeyValueHeaderRecordWriterProvider;
import io.confluent.connect.azure.format.RecordViewSetter;
import io.confluent.connect.azure.format.RecordViews;
import io.confluent.connect.azure.storage.AzBlobStorage;
import io.confluent.connect.azure.util.Version;
import io.confluent.connect.azure.AzBlobSinkConnectorConfig.BehaviorOnNullValues;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import io.confluent.common.utils.SystemTime;
import io.confluent.common.utils.Time;
import io.confluent.connect.storage.StorageFactory;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;

public class AzBlobSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(AzBlobSinkTask.class);

  private AzBlobSinkConnectorConfig connectorConfig;
  private String url;
  private long timeoutMs;
  private AzBlobStorage storage;
  private final Map<TopicPartition, TopicPartitionWriter> topicPartitionWriters;
  private Partitioner<?> partitioner;
  private Format<AzBlobSinkConnectorConfig, String> format;
  private RecordWriterProvider<AzBlobSinkConnectorConfig> writerProvider;
  private final Time time;
  private ErrantRecordReporter reporter;

  /**
   * No-arg constructor. Used by Connect framework.
   */
  public AzBlobSinkTask() {
    // no-arg constructor required by Connect framework.
    topicPartitionWriters = new HashMap<>();
    time = new SystemTime();
  }

  // visible for testing.
  AzBlobSinkTask(AzBlobSinkConnectorConfig connectorConfig, SinkTaskContext context, AzBlobStorage storage,
                 Partitioner<?> partitioner, Format<AzBlobSinkConnectorConfig, String> format,
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
      connectorConfig = new AzBlobSinkConnectorConfig(props);
      url = connectorConfig.getString(StorageCommonConfig.STORE_URL_CONFIG);
      timeoutMs = connectorConfig.getLong(AzBlobSinkConnectorConfig.RETRY_BACKOFF_CONFIG);

      @SuppressWarnings("unchecked")
      Class<? extends AzBlobStorage> storageClass =
          (Class<? extends AzBlobStorage>)
              connectorConfig.getClass(StorageCommonConfig.STORAGE_CLASS_CONFIG);
      storage = StorageFactory.createStorage(
          storageClass,
          AzBlobSinkConnectorConfig.class,
          connectorConfig,
          url
      );
      if (!storage.bucketExists()) {
        throw new ConnectException("Non-existent S3 bucket: " + connectorConfig.getBucketName());
      }

      writerProvider = newRecordWriterProvider(connectorConfig);
      partitioner = newPartitioner(connectorConfig);

      open(context.assignment());
      try {
        reporter = context.errantRecordReporter();
        if (reporter == null) {
          log.info("Errant record reporter not configured.");
        }
      } catch (NoSuchMethodError | NoClassDefFoundError | UnsupportedOperationException e) {
        // Will occur in Connect runtimes earlier than 2.6
        log.warn("Connect versions prior to Apache Kafka 2.6 do not support "
            + "the errant record reporter");
      }

      log.info("Started S3 connector task with assigned partitions: {}",
          topicPartitionWriters.keySet()
      );
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException
        | InvocationTargetException | NoSuchMethodException e) {
      throw new ConnectException("Reflection exception: ", e);
    } catch (AmazonClientException | URISyntaxException e) {
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
  private Format<AzBlobSinkConnectorConfig, String> newFormat(String formatClassConfig)
      throws ClassNotFoundException, IllegalAccessException, InstantiationException,
             InvocationTargetException, NoSuchMethodException {
    Class<Format<AzBlobSinkConnectorConfig, String>> formatClass =
        (Class<Format<AzBlobSinkConnectorConfig, String>>) connectorConfig.getClass(formatClassConfig);
    return formatClass.getConstructor(AzBlobStorage.class).newInstance(storage);
  }

  RecordWriterProvider<AzBlobSinkConnectorConfig> newRecordWriterProvider(
      AzBlobSinkConnectorConfig config)
      throws ClassNotFoundException, InvocationTargetException, InstantiationException,
      NoSuchMethodException, IllegalAccessException {

    RecordWriterProvider<AzBlobSinkConnectorConfig> valueWriterProvider =
        newFormat(AzBlobSinkConnectorConfig.FORMAT_CLASS_CONFIG).getRecordWriterProvider();

    RecordWriterProvider<AzBlobSinkConnectorConfig> keyWriterProvider = null;
    if (config.getBoolean(AzBlobSinkConnectorConfig.STORE_KAFKA_KEYS_CONFIG)) {
      keyWriterProvider = newFormat(AzBlobSinkConnectorConfig.KEYS_FORMAT_CLASS_CONFIG)
          .getRecordWriterProvider();
      ((RecordViewSetter) keyWriterProvider).setRecordView(new RecordViews.KeyRecordView());
    }
    RecordWriterProvider<AzBlobSinkConnectorConfig> headerWriterProvider = null;
    if (config.getBoolean(AzBlobSinkConnectorConfig.STORE_KAFKA_HEADERS_CONFIG)) {
      headerWriterProvider = newFormat(AzBlobSinkConnectorConfig.HEADERS_FORMAT_CLASS_CONFIG)
          .getRecordWriterProvider();
      ((RecordViewSetter) headerWriterProvider).setRecordView(new RecordViews.HeaderRecordView());
    }

    return new KeyValueHeaderRecordWriterProvider(
        valueWriterProvider, keyWriterProvider, headerWriterProvider);
  }

  private Partitioner<?> newPartitioner(AzBlobSinkConnectorConfig config)
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
        if (reporter != null) {
          reporter.report(record, new DataException("Skipping null value record."));
        }
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
            + AzBlobSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG + " 'settings.");
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
        time,
        reporter
    );
  }
}
