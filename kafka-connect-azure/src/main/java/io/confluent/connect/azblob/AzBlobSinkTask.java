/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.azblob;

import io.confluent.common.utils.SystemTime;
import io.confluent.common.utils.Time;
import io.confluent.connect.azblob.storage.AzBlobStorage;
import io.confluent.connect.azblob.util.Version;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class AzBlobSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(AzBlobSinkTask.class);

  private AzBlobSinkConnectorConfig connectorConfig;
  private String url;
  private AzBlobStorage storage;
  private final Set<TopicPartition> assignment;
  private final Map<TopicPartition, TopicPartitionWriter> topicPartitionWriters;
  private Partitioner<FieldSchema> partitioner;
  private Format<AzBlobSinkConnectorConfig, String> format;
  private RecordWriterProvider<AzBlobSinkConnectorConfig> writerProvider;
  private final Time time;

  /**
   * No-arg constructor. Used by Connect framework.
   */
  public AzBlobSinkTask() {
    // no-arg constructor required by Connect framework.
    assignment = new HashSet<>();
    topicPartitionWriters = new HashMap<>();
    time = new SystemTime();
  }

  // visible for testing.
  AzBlobSinkTask(AzBlobSinkConnectorConfig connectorConfig, SinkTaskContext context,
                 AzBlobStorage storage,
                 Partitioner<FieldSchema> partitioner,
                 Format<AzBlobSinkConnectorConfig, String> format,
                 Time time) throws Exception {
    this.assignment = new HashSet<>();
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
    log.info("Started AZ Blob connector task with assigned partitions {}", assignment);
  }

  public void start(Map<String, String> props) {
    checkNotNull("context must not be null", context);
    checkNotNull("context assignment must not be null", context.assignment());

    log.info("Starting SinkTask");
    try {
      connectorConfig = new AzBlobSinkConnectorConfig(props);
      url = connectorConfig.getString(StorageCommonConfig.STORE_URL_CONFIG);

      @SuppressWarnings("unchecked")
      Class<? extends AzBlobStorage> storageClass =
          (Class<? extends AzBlobStorage>)
              connectorConfig.getClass(StorageCommonConfig.STORAGE_CLASS_CONFIG);

      log.info("Starting Storage with storageClass={} connectorConfig={}, url={}", storageClass,
          connectorConfig, url);

      storage = new AzBlobStorage(connectorConfig, "fakeurl");
      if (!storage.bucketExists()) {
        throw new DataException("Non-existent container: " + connectorConfig.getContainerName());
      }

      writerProvider = newFormat().getRecordWriterProvider();
      partitioner = newPartitioner(connectorConfig);

      open(context.assignment());
      log.info("Started AZ Blob connector task with assigned partitions: {}", assignment);
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException
        | InvocationTargetException | NoSuchMethodException e) {
      throw new ConnectException("Reflection exception: ", e);
    } catch (Exception e) {
      throw new ConnectException(e);
    }
  }

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void open(Collection<TopicPartition> partitions) {
    // assignment should be empty, either because this is the initial call or because it follows
    // a call to "close".
    assignment.addAll(partitions);
    for (TopicPartition tp : assignment) {
      TopicPartitionWriter writer = new TopicPartitionWriter(
          tp,
          writerProvider,
          partitioner,
          connectorConfig,
          context
      );
      topicPartitionWriters.put(tp, writer);
    }
  }

  @SuppressWarnings("unchecked")
  private Format<AzBlobSinkConnectorConfig, String> newFormat() throws ClassNotFoundException,
      IllegalAccessException,
      InstantiationException, InvocationTargetException,
      NoSuchMethodException {
    Class<Format<AzBlobSinkConnectorConfig, String>> formatClass =
        (Class<Format<AzBlobSinkConnectorConfig, String>>) connectorConfig
            .getClass(AzBlobSinkConnectorConfig.FORMAT_CLASS_CONFIG);
    return formatClass.getConstructor(AzBlobStorage.class).newInstance(storage);
  }

  private Partitioner<FieldSchema> newPartitioner(AzBlobSinkConnectorConfig config)
      throws ClassNotFoundException, IllegalAccessException, InstantiationException {

    @SuppressWarnings("unchecked")
    Class<? extends Partitioner<FieldSchema>> partitionerClass =
        (Class<? extends Partitioner<FieldSchema>>)
            config.getClass(PartitionerConfig.PARTITIONER_CLASS_CONFIG);

    Partitioner<FieldSchema> partitioner = partitionerClass.newInstance();

    Map<String, Object> plainValues = new HashMap<>(config.plainValues());
    Map<String, ?> originals = config.originals();
    for (String originalKey : originals.keySet()) {
      if (!plainValues.containsKey(originalKey)) {
        // pass any additional configs down to the partitioner so that custom partitioners
        // can have their own configs
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
      log.info("TopicPartition: {}", tp);
      TopicPartitionWriter topicPartitionWriter = topicPartitionWriters.get(tp);
      log.info("topicPartitionWriter: {}", topicPartitionWriter);
      topicPartitionWriter.buffer(record);
    }
    if (log.isDebugEnabled()) {
      log.debug("Read {} records from Kafka", records.size());
    }
    log.info("Read {} records from Kafka", records.size());

    for (TopicPartition tp : assignment) {
      log.info("Writing topic partition {}", tp);
      TopicPartitionWriter topicPartitionWriter = topicPartitionWriters.get(tp);
      topicPartitionWriter.write();
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    // No-op. The connector is managing the offsets.
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
      Map<TopicPartition, OffsetAndMetadata> offsets) {
    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
    for (TopicPartition tp : assignment) {
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
    for (TopicPartition tp : assignment) {
      try {
        topicPartitionWriters.get(tp).close();
      } catch (ConnectException e) {
        log.error("Error closing writer for {}. Error: {}", tp, e.getMessage());
      }
    }
    topicPartitionWriters.clear();
    assignment.clear();
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

  // Visible for testing
  Format<AzBlobSinkConnectorConfig, String> getFormat() {
    return format;
  }
}
