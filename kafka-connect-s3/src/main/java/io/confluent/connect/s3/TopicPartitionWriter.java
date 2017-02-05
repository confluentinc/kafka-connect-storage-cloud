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

package io.confluent.connect.s3;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.IllegalWorkerStateException;
import org.apache.kafka.connect.errors.SchemaProjectorException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.hive.HiveConfig;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import io.confluent.connect.storage.schema.StorageSchemaCompatibility;
import io.confluent.connect.storage.util.DateTimeUtils;

public class TopicPartitionWriter {
  private static final Logger log = LoggerFactory.getLogger(TopicPartitionWriter.class);

  private final Map<String, String> commitFiles;
  private final Map<String, RecordWriter> writers;
  private final Map<String, Schema> currentSchemas;
  private final TopicPartition tp;
  private final Partitioner<FieldSchema> partitioner;
  private String topicsDir;
  private State state;
  private final Queue<SinkRecord> buffer;
  private final SinkTaskContext context;
  private int recordCounter;
  private final int flushSize;
  private final long rotateIntervalMs;
  private final long rotateScheduleIntervalMs;
  private long nextScheduledRotate;
  private final RecordWriterProvider<S3SinkConnectorConfig> writerProvider;
  private long offset;
  private final Map<String, Long> startOffsets;
  private final Map<String, Long> offsets;
  private long timeoutMs;
  private long failureTime;
  private final StorageSchemaCompatibility compatibility;
  private final String extension;
  private final String zeroPadOffsetFormat;
  private final String dirDelim;
  private final String fileDelim;
  private final DateTimeZone timezone;
  private final Time time;

  public TopicPartitionWriter(TopicPartition tp,
                              S3Storage storage,
                              RecordWriterProvider<S3SinkConnectorConfig> writerProvider,
                              Partitioner<FieldSchema> partitioner,
                              S3SinkConnectorConfig connectorConfig,
                              SinkTaskContext context) {
    this(tp, writerProvider, partitioner, connectorConfig, context, Time.SYSTEM);
  }

  // Visible for testing
  TopicPartitionWriter(TopicPartition tp,
                       RecordWriterProvider<S3SinkConnectorConfig> writerProvider,
                       Partitioner<FieldSchema> partitioner,
                       S3SinkConnectorConfig connectorConfig,
                       SinkTaskContext context,
                       Time time) {
    this.time = time;
    this.tp = tp;
    this.context = context;
    this.writerProvider = writerProvider;
    this.partitioner = partitioner;

    flushSize = connectorConfig.getInt(S3SinkConnectorConfig.FLUSH_SIZE_CONFIG);
    topicsDir = connectorConfig.getString(StorageCommonConfig.TOPICS_DIR_CONFIG);
    rotateIntervalMs = connectorConfig.getLong(S3SinkConnectorConfig.ROTATE_INTERVAL_MS_CONFIG);
    rotateScheduleIntervalMs = connectorConfig.getLong(S3SinkConnectorConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG);
    timeoutMs = connectorConfig.getLong(S3SinkConnectorConfig.RETRY_BACKOFF_CONFIG);
    compatibility = StorageSchemaCompatibility.getCompatibility(
        connectorConfig.getString(HiveConfig.SCHEMA_COMPATIBILITY_CONFIG));

    buffer = new LinkedList<>();
    commitFiles = new HashMap<>();
    writers = new HashMap<>();
    currentSchemas = new HashMap<>();
    startOffsets = new HashMap<>();
    offsets = new HashMap<>();
    state = State.WRITE_STARTED;
    failureTime = -1L;
    offset = -1L;
    dirDelim = connectorConfig.getString(StorageCommonConfig.DIRECTORY_DELIM_CONFIG);
    fileDelim = connectorConfig.getString(StorageCommonConfig.FILE_DELIM_CONFIG);
    extension = writerProvider.getExtension();
    zeroPadOffsetFormat = "%0" + connectorConfig.getInt(S3SinkConnectorConfig.FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG)
                              + "d";

    timezone = rotateScheduleIntervalMs > 0 ?
                   DateTimeZone.forID(connectorConfig.getString(PartitionerConfig.TIMEZONE_CONFIG)) :
                   null;

    // Initialize rotation timers
    updateRotationTimers();
  }

  private enum State {
    WRITE_STARTED,
    WRITE_PARTITION_PAUSED,
    SHOULD_ROTATE,
    FILE_COMMITTED;

    private static final State[] VALS = values();

    public State next() {
      return VALS[(ordinal() + 1) % VALS.length];
    }
  }

  private void updateRotationTimers() {
    long lastRotate = time.milliseconds();
    if (log.isDebugEnabled() && rotateIntervalMs > 0) {
      log.debug("Update last rotation timer. Next rotation for {} will be in {}ms", tp, rotateIntervalMs);
    }
    if (rotateScheduleIntervalMs > 0) {
      nextScheduledRotate = DateTimeUtils.getNextTimeAdjustedByDay(lastRotate, rotateScheduleIntervalMs, timezone);
      if (log.isDebugEnabled()) {
        log.debug("Update scheduled rotation timer. Next rotation for {} will be at {}", tp, new DateTime(nextScheduledRotate).withZone(timezone).toString());
      }
    }
  }

  @SuppressWarnings("fallthrough")
  public void write() {
    long now = time.milliseconds();
    if (failureTime > 0 && now - failureTime < timeoutMs) {
      return;
    }

    updateRotationTimers();

    while (!buffer.isEmpty()) {
      try {
        switch (state) {
          case WRITE_STARTED:
            pause();
            nextState();
          case WRITE_PARTITION_PAUSED:
            SinkRecord record = buffer.peek();
            Schema valueSchema = record.valueSchema();
            String encodedPartition = partitioner.encodePartition(record);
            Schema currentValueSchema = currentSchemas.get(encodedPartition);
            if (currentValueSchema == null) {
              currentValueSchema = record.valueSchema();
              currentSchemas.put(encodedPartition, valueSchema);
            }

            if (compatibility.shouldChangeSchema(record, null, currentValueSchema)) {
              currentSchemas.put(encodedPartition, valueSchema);
              if (recordCounter > 0) {
                nextState();
              } else {
                break;
              }
            } else {
              SinkRecord projectedRecord = compatibility.project(record, null, currentValueSchema);
              writeRecord(projectedRecord);
              buffer.poll();
              if (shouldRotate(projectedRecord.timestamp())) {
                log.info("Starting commit and rotation for topic partition {} with start offset {} and end offset {}", tp, startOffsets, offsets);
                nextState();
                // Fall through and try to rotate immediately
              } else {
                break;
              }
            }
          case SHOULD_ROTATE:
            updateRotationTimers();
            commitFiles();
            nextState();
          case FILE_COMMITTED:
            setState(State.WRITE_PARTITION_PAUSED);
            break;
          default:
            log.error("{} is not a valid state to write record for topic partition {}.", state, tp);
        }
      } catch (SchemaProjectorException | IllegalWorkerStateException e) {
        throw new RuntimeException(e);
      } catch (ConnectException e) {
        log.error("Exception on topic partition {}: ", tp, e);
        failureTime = time.milliseconds();
        setRetryTimeout(timeoutMs);
        break;
      }
    }
    if (buffer.isEmpty()) {
      // Need to define this corner case in S3
      // committing files after waiting for rotateIntervalMs time but less than flush.size records available
      // if (recordCounter > 0 && shouldRotate(now))

      resume();
      setState(State.WRITE_STARTED);
    }
  }

  public void close() throws ConnectException {
    log.debug("Closing TopicPartitionWriter {}", tp);
    // abort any outstanding multi-part uploads? Wait for them?
    // writers.abort?
    writers.clear();
    startOffsets.clear();
    offsets.clear();
  }

  public void buffer(SinkRecord sinkRecord) {
    buffer.add(sinkRecord);
  }

  public long offset() {
    return offset;
  }

  public Map<String, RecordWriter> getWriters() {
    return writers;
  }

  private String getDirectoryPrefix(String encodedPartition) {
    return partitioner.generatePartitionedPath(tp.topic(), encodedPartition);
  }

  private void nextState() {
    state = state.next();
  }

  private void setState(State state) {
    this.state = state;
  }

  private boolean shouldRotate(Long timestamp) {
    // TODO: is this appropriate for late data? (creation of arbitrarily small files possible).
    // If not, it should be disabled.
    boolean scheduledRotation = rotateScheduleIntervalMs > 0
                                    && timestamp != null
                                    && timestamp >= nextScheduledRotate;
    boolean messageSizeRotation = recordCounter >= flushSize;

    log.trace("Should rotate (counter {} >= flush size {} and schedule interval {} next schedule {} timestamp {})? {}",
              recordCounter, flushSize, rotateScheduleIntervalMs, nextScheduledRotate, timestamp,
              scheduledRotation || messageSizeRotation);

    return scheduledRotation || messageSizeRotation;
  }

  private void pause() {
    log.trace("Pausing writer for topic-partition {}", tp);
    context.pause(tp);
  }

  private void resume() {
    log.trace("Resuming writer for topic-partition {}", tp);
    context.resume(tp);
  }

  private RecordWriter getWriter(SinkRecord record, String encodedPartition) throws ConnectException {
    if (writers.containsKey(encodedPartition)) {
      return writers.get(encodedPartition);
    }
    String commitFilename = getCommitFilename(encodedPartition);
    RecordWriter writer = writerProvider.getRecordWriter(null, commitFilename);
    writers.put(encodedPartition, writer);
    return writer;
  }

  private String getCommitFilename(String encodedPartition) {
    String commitFile;
    if (commitFiles.containsKey(encodedPartition)) {
      commitFile = commitFiles.get(encodedPartition);
    } else {
      long startOffset = startOffsets.get(encodedPartition);
      String prefix = getDirectoryPrefix(encodedPartition);
      commitFile = fileKeyToCommit(prefix, startOffset);
      commitFiles.put(encodedPartition, commitFile);
    }
    return commitFile;
  }

  private String fileKey(String topicsPrefix, String keyPrefix, String name) {
    return topicsPrefix + dirDelim + keyPrefix + dirDelim + name;
  }

  private String fileKeyToCommit(String dirPrefix, long startOffset) {
    String name = tp.topic()
                      + fileDelim
                      + tp.partition()
                      + fileDelim
                      + String.format(zeroPadOffsetFormat, startOffset)
                      + extension;
    return fileKey(topicsDir, dirPrefix, name);
  }

  private void writeRecord(SinkRecord record) {
    // TODO: double-check this is valid in all cases of start-up/recovery
    long recordOffset = record.kafkaOffset();
    if (offset == -1) {
      offset = recordOffset;
      log.trace("Writer's offset: -1. Resetting to: {}", offset);
    }

    String encodedPartition = partitioner.encodePartition(record);
    if (!startOffsets.containsKey(encodedPartition)) {
      log.trace("Setting writer's start offset for '{}' to {}", encodedPartition, recordOffset);
      startOffsets.put(encodedPartition, recordOffset);
    }

    RecordWriter writer = getWriter(record, encodedPartition);
    writer.write(record);

    offsets.put(encodedPartition, recordOffset);
    recordCounter++;
    log.trace("Setting writer's offset for '{}' to {} - Total records {}", encodedPartition, recordOffset,
              recordCounter);
  }

  private void commitFiles() {
    for (String encodedPartition : commitFiles.keySet()) {
      commitFile(encodedPartition);
    }
    currentSchemas.clear();
  }

  private void commitFile(String encodedPartition) {
    if (!startOffsets.containsKey(encodedPartition)) {
      log.warn("Tried to commit file with missing starting offset partition: {}. Ignoring.");
      return;
    }

    if (writers.containsKey(encodedPartition)) {
      RecordWriter writer = writers.get(encodedPartition);
      writer.close();
      writers.remove(encodedPartition);
      log.debug("Removed writer for '{}'", encodedPartition);
    }

    long commitOffset = offsets.get(encodedPartition);
    context.offset(tp, commitOffset);
    log.debug("Reset offset for {} to {}", tp, commitOffset);

    startOffsets.remove(encodedPartition);
    String filename = commitFiles.remove(encodedPartition);
    offset = -1L;
    recordCounter = 0;
    log.info("Committed {} for {}", filename, tp);
  }

  private void setRetryTimeout(long timeoutMs) {
    context.timeout(timeoutMs);
  }
}
