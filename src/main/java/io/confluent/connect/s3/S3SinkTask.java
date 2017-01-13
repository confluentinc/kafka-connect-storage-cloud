/*
 * Copyright 2016 Confluent Inc.
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

package io.confluent.connect.s3;

import com.amazonaws.AmazonClientException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
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

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.s3.storage.S3StorageConfig;
import io.confluent.connect.s3.util.Version;
import io.confluent.connect.storage.StorageFactory;
import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.partitioner.Partitioner;

public class S3SinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(S3SinkTask.class);

  private S3SinkConnectorConfig connectorConfig;
  private String url;
  private S3Storage storage;
  private S3StorageConfig storageConfig;
  private final Set<TopicPartition> assignment;
  private final Map<TopicPartition, TopicPartitionWriter> topicPartitionWriters;
  private Partitioner partitioner;
  private RecordWriterProvider<S3StorageConfig> writerProvider;
  private AvroData avroData;

  /**
   * No-arg consturctor. Used by Connect framework.
   */
  public S3SinkTask() {
    // no-arg constructor required by Connect framework.
    assignment = new HashSet<>();
    topicPartitionWriters = new HashMap<>();
  }

  // visible for testing.
  S3SinkTask(S3SinkConnectorConfig connectorConfig, SinkTaskContext context, S3Storage storage, Partitioner partitioner,
             AvroData avroData) throws Exception {
    this();
    this.connectorConfig = connectorConfig;
    this.context = context;
    this.storage = storage;
    this.partitioner = partitioner;
    this.avroData = avroData;

    storageConfig = new S3StorageConfig(connectorConfig);
    url = connectorConfig.getString(S3SinkConnectorConfig.STORE_URL_CONFIG);
    writerProvider = newFormat().getRecordWriterProvider();

    open(context.assignment());
    log.info("Started S3 connector task with assigned partitions {}", assignment);
  }

  public void start(Map<String, String> props) {
    try {
      connectorConfig = new S3SinkConnectorConfig(props);
      storageConfig = new S3StorageConfig(connectorConfig);
      url = connectorConfig.getString(S3SinkConnectorConfig.STORE_URL_CONFIG);

      @SuppressWarnings("unchecked")
      Class<? extends S3Storage> storageClass = (Class<? extends S3Storage>) Class
                                                                                 .forName(connectorConfig.getString(S3SinkConnectorConfig.STORAGE_CLASS_CONFIG));
      storage = StorageFactory.createStorage(storageClass, S3StorageConfig.class, storageConfig, url);
      storage.bucketExists(storageConfig.bucket());

      avroData = new AvroData(connectorConfig.getInt(S3SinkConnectorConfig.SCHEMA_CACHE_SIZE_CONFIG));
      writerProvider = newFormat().getRecordWriterProvider();
      partitioner = newPartitioner(connectorConfig);

      open(context.assignment());
      log.info("Started S3 connector task with assigned partitions: {}", assignment);
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException | InvocationTargetException
                 | NoSuchMethodException e) {
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
    assignment.clear();
    assignment.addAll(partitions);
    for (TopicPartition tp : assignment) {
      TopicPartitionWriter writer =
          new TopicPartitionWriter(tp, storage, writerProvider, partitioner, connectorConfig, context, avroData);
      topicPartitionWriters.put(tp, writer);
    }
  }

  @SuppressWarnings("unchecked")
  private Format<S3StorageConfig, String> newFormat() throws ClassNotFoundException, IllegalAccessException,
                                                             InstantiationException, InvocationTargetException,
                                                             NoSuchMethodException {
    String name = connectorConfig.getString(S3SinkConnectorConfig.FORMAT_CLASS_CONFIG);
    return ((Class<Format<S3StorageConfig, String>>) Class.forName(name))
               .getConstructor(S3Storage.class, AvroData.class).newInstance(storage, avroData);
  }

  private Partitioner newPartitioner(S3SinkConnectorConfig config)
      throws ClassNotFoundException, IllegalAccessException, InstantiationException {

    @SuppressWarnings("unchecked")
    Class<? extends Partitioner> partitionerClasss = (Class<? extends Partitioner>)
                                                         Class.forName(config.getString(S3SinkConnectorConfig.PARTITIONER_CLASS_CONFIG));

    Map<String, Object> map = copyConfig(config);
    Partitioner partitioner = partitionerClasss.newInstance();
    partitioner.configure(map);
    return partitioner;
  }

  private Map<String, Object> copyConfig(S3SinkConnectorConfig config) {
    Map<String, Object> map = new HashMap<>();
    map.put(S3SinkConnectorConfig.PARTITION_FIELD_NAME_CONFIG, config.getString(S3SinkConnectorConfig.PARTITION_FIELD_NAME_CONFIG));
    map.put(S3SinkConnectorConfig.PARTITION_DURATION_MS_CONFIG, config.getLong(S3SinkConnectorConfig.PARTITION_DURATION_MS_CONFIG));
    map.put(S3SinkConnectorConfig.PATH_FORMAT_CONFIG, config.getString(S3SinkConnectorConfig.PATH_FORMAT_CONFIG));
    map.put(S3SinkConnectorConfig.LOCALE_CONFIG, config.getString(S3SinkConnectorConfig.LOCALE_CONFIG));
    map.put(S3SinkConnectorConfig.TIMEZONE_CONFIG, config.getString(S3SinkConnectorConfig.TIMEZONE_CONFIG));
    return map;
  }

  @Override
  public void put(Collection<SinkRecord> records) throws ConnectException {
    for (SinkRecord record : records) {
      String topic = record.topic();
      int partition = record.kafkaPartition();
      TopicPartition tp = new TopicPartition(topic, partition);
      topicPartitionWriters.get(tp).buffer(record);
    }

    for (TopicPartition tp: assignment) {
      topicPartitionWriters.get(tp).write();
    }
  }

  public void close(Collection<TopicPartition> partitions) {
    for (TopicPartition tp : assignment) {
      try {
        topicPartitionWriters.get(tp).close();
      } catch (ConnectException e) {
        log.error("Error closing writer for {}. Error: {}", tp, e.getMessage());
      } finally {
        topicPartitionWriters.remove(tp);
      }
    }
  }

  @Override
  public void stop() {
    try {
      storage.close();
    } catch (Exception e) {
      throw new ConnectException(e);
    }
  }

  Partitioner getPartitioner() {
    return partitioner;
  }

}
