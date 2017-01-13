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

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.s3.storage.S3StorageConfig;
import io.confluent.connect.s3.util.FileUtils;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.storage.schema.StorageSchemaCompatibility;
import io.confluent.connect.storage.util.DateTimeUtils;

public class TopicPartitionWriter {
  private static final Logger log = LoggerFactory.getLogger(TopicPartitionWriter.class);

  private final Map<String, String> commitFiles;
  private final Map<String, RecordWriter<SinkRecord>> writers;
  private final Map<String, Schema> currentSchemas;
  private final TopicPartition tp;
  private final Partitioner partitioner;
  private final String url;
  private String topicsDir;
  private State state;
  private final Queue<SinkRecord> buffer;
  private final S3Storage storage;
  private final SinkTaskContext context;
  private int recordCounter;
  private final int flushSize;
  private final long rotateIntervalMs;
  private final long rotateScheduleIntervalMs;
  private long nextScheduledRotate;
  private final RecordWriterProvider<S3StorageConfig> writerProvider;
  private final S3StorageConfig conf;
  private final AvroData avroData;
  private long offset;
  private boolean sawInvalidOffset;
  private final Map<String, Long> startOffsets;
  private final Map<String, Long> offsets;
  private long timeoutMs;
  private long failureTime;
  private final StorageSchemaCompatibility compatibility;
  private final String extension;
  private final String zeroPadOffsetFormat;
  private final DateTimeZone timezone;
  private final Time time;

  public TopicPartitionWriter(TopicPartition tp,
                              S3Storage storage,
                              RecordWriterProvider<S3StorageConfig> writerProvider,
                              Partitioner partitioner,
                              S3SinkConnectorConfig connectorConfig,
                              SinkTaskContext context,
                              AvroData avroData) {
    this(tp, storage, writerProvider, partitioner, connectorConfig, context, avroData, Time.SYSTEM);
  }

  // Visible for testing
  TopicPartitionWriter(TopicPartition tp,
                       S3Storage storage,
                       RecordWriterProvider<S3StorageConfig> writerProvider,
                       Partitioner partitioner,
                       S3SinkConnectorConfig connectorConfig,
                       SinkTaskContext context,
                       AvroData avroData,
                       Time time) {
    this.time = time;
    this.tp = tp;
    this.context = context;
    this.avroData = avroData;
    this.storage = storage;
    this.writerProvider = writerProvider;
    this.partitioner = partitioner;
    this.url = storage.url();
    this.conf = storage.conf();

    flushSize = connectorConfig.getInt(S3SinkConnectorConfig.FLUSH_SIZE_CONFIG);
    topicsDir = connectorConfig.getString(S3SinkConnectorConfig.TOPICS_DIR_CONFIG);
    rotateIntervalMs = connectorConfig.getLong(S3SinkConnectorConfig.ROTATE_INTERVAL_MS_CONFIG);
    rotateScheduleIntervalMs = connectorConfig.getLong(S3SinkConnectorConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG);
    timeoutMs = connectorConfig.getLong(S3SinkConnectorConfig.RETRY_BACKOFF_CONFIG);
    compatibility = StorageSchemaCompatibility.getCompatibility(
        connectorConfig.getString(S3SinkConnectorConfig.SCHEMA_COMPATIBILITY_CONFIG));

    buffer = new LinkedList<>();
    commitFiles = new HashMap<>();
    writers = new HashMap<>();
    currentSchemas = new HashMap<>();
    startOffsets = new HashMap<>();
    offsets = new HashMap<>();
    state = State.WRITE_STARTED;
    failureTime = -1L;
    offset = -1L;
    sawInvalidOffset = false;
    extension = writerProvider.getExtension();
    zeroPadOffsetFormat = "%0" + connectorConfig.getInt(S3SinkConnectorConfig.FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG)
                              + "d";

    timezone = rotateScheduleIntervalMs > 0 ?
                   DateTimeZone.forID(connectorConfig.getString(S3SinkConnectorConfig.TIMEZONE_CONFIG)) :
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
            Schema currentSchema = currentSchemas.get(encodedPartition);
            if (currentSchema == null) {
              currentSchema = record.valueSchema();
              currentSchemas.put(encodedPartition, valueSchema);
            }

            if (compatibility.shouldChangeSchema(valueSchema, currentSchema)) {
              currentSchemas.put(encodedPartition, valueSchema);
              if (recordCounter > 0) {
                nextState();
              } else {
                break;
              }
            } else {
              SinkRecord projectedRecord = compatibility.project(record, currentSchema);
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

  public Map<String, RecordWriter<SinkRecord>> getWriters() {
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

    return scheduledRotation || messageSizeRotation;
  }

  private void pause() {
    context.pause(tp);
  }

  private void resume() {
    context.resume(tp);
  }

  private RecordWriter<SinkRecord> getWriter(SinkRecord record, String encodedPartition) throws ConnectException {
    if (writers.containsKey(encodedPartition)) {
      return writers.get(encodedPartition);
    }
    String commitFilename = getCommitFilename(encodedPartition);
    RecordWriter<SinkRecord> writer = writerProvider.getRecordWriter(conf, commitFilename);
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
      commitFile = FileUtils.fileKeyToCommit(topicsDir, prefix, tp, startOffset, extension, zeroPadOffsetFormat);
      commitFiles.put(encodedPartition, commitFile);
    }
    return commitFile;
  }

  private void writeRecord(SinkRecord record) {
    // TODO: double-check this is valid in all cases of start-up/recovery
    long expectedOffset = offset + recordCounter;
    if (offset == -1) {
      offset = record.kafkaOffset();
    } else if (record.kafkaOffset() != expectedOffset) {
      // Currently it's possible to see stale data with the wrong offset after a rebalance when you
      // rewind, which we do since we manage our own offsets. See KAFKA-2894.
      if (!sawInvalidOffset) {
        log.info(
            "Ignoring stale out-of-order record in {}-{}. Has offset {} instead of expected offset {}",
            record.topic(), record.kafkaPartition(), record.kafkaOffset(), expectedOffset);
      }
      sawInvalidOffset = true;
      return;
    }

    if (sawInvalidOffset) {
      log.info(
          "Recovered from stale out-of-order records in {}-{} with offset {}",
          record.topic(), record.kafkaPartition(), expectedOffset);
      sawInvalidOffset = false;
    }

    String encodedPartition = partitioner.encodePartition(record);
    if (!startOffsets.containsKey(encodedPartition)) {
      startOffsets.put(encodedPartition, record.kafkaOffset());
    }

    RecordWriter<SinkRecord> writer = getWriter(record, encodedPartition);
    writer.write(record);

    offsets.put(encodedPartition, record.kafkaOffset());
    recordCounter++;
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

    long startOffset = startOffsets.get(encodedPartition);
    String prefix = getDirectoryPrefix(encodedPartition);
    String file = FileUtils.fileKeyToCommit(topicsDir, prefix, tp, startOffset, extension, zeroPadOffsetFormat);
    //storage.commit(null, file);

    if (writers.containsKey(encodedPartition)) {
      RecordWriter<SinkRecord> writer = writers.get(encodedPartition);
      writer.close();
      writers.remove(encodedPartition);
    }

    long commitOffset = offsets.get(encodedPartition);
    // TODO: Do I need a check here? > than startOffset? > 0?
    log.debug("Resetting offset for {} to {}", tp, commitOffset);
    context.offset(tp, commitOffset);

    startOffsets.remove(encodedPartition);
    commitFiles.remove(encodedPartition);
    offset = -1L;
    recordCounter = 0;
    log.info("Committed {} for {}", file, tp);
  }

  private void setRetryTimeout(long timeoutMs) {
    context.timeout(timeoutMs);
  }
}
