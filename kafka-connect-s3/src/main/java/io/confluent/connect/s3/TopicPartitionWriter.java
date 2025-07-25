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

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import io.confluent.connect.s3.errors.FileExistsException;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.s3.util.FileRotationTracker;
import io.confluent.connect.s3.util.RetryUtil;
import io.confluent.connect.s3.util.TombstoneTimestampExtractor;
import io.confluent.connect.storage.errors.PartitionException;
import io.confluent.connect.storage.schema.SchemaCompatibilityResult;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.IllegalWorkerStateException;
import org.apache.kafka.connect.errors.SchemaProjectorException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.avro.SchemaParseException;
import org.apache.parquet.schema.InvalidSchemaException;

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;

import io.confluent.common.utils.SystemTime;
import io.confluent.common.utils.Time;
import io.confluent.connect.storage.StorageSinkConnectorConfig;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.common.util.StringUtils;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;
import io.confluent.connect.storage.partitioner.TimestampExtractor;
import io.confluent.connect.storage.schema.StorageSchemaCompatibility;
import io.confluent.connect.storage.util.DateTimeUtils;

import static io.confluent.connect.s3.S3SinkConnectorConfig.MAX_FILE_SCAN_LIMIT_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.S3_PART_RETRIES_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.S3_RETRY_BACKOFF_CONFIG;

public class TopicPartitionWriter {

  private static final Logger log = LoggerFactory.getLogger(TopicPartitionWriter.class);

  private final Map<String, String> commitFiles;
  private final Map<String, RecordWriter> writers;
  private final Map<String, Schema> currentSchemas;
  private final TopicPartition tp;
  private final S3Storage storage;
  private final Partitioner<?> partitioner;
  private TimestampExtractor timestampExtractor;
  private String topicsDir;
  private State state;
  private final Queue<SinkRecord> buffer;
  private final SinkTaskContext context;
  private final boolean isTaggingEnabled;
  private final List<String> extraTagKeyValuePair;
  private HashMap<String, String> hashMapTag;
  private final boolean ignoreTaggingErrors;
  private int recordCount;
  private final int flushSize;
  private final int partitionerMaxOpenFiles;
  private final long rotateIntervalMs;
  private final long rotateScheduleIntervalMs;
  private long nextScheduledRotation;
  private long currentOffset;
  private Long currentTimestamp;
  private String currentEncodedPartition;
  private Long baseRecordTimestamp;
  private Long offsetToCommit;
  private final RecordWriterProvider<S3SinkConnectorConfig> writerProvider;

  // VisibleForTesting
  Map<Long, String> offsetToFilenameMap;

  private final Map<String, Long> startOffsets;
  private final Map<String, Long> endOffsets;
  private final Map<String, Long> recordCounts;
  private long timeoutMs;
  private long failureTime;
  private final StorageSchemaCompatibility compatibility;
  private final String extension;
  private final String zeroPadOffsetFormat;
  private final String dirDelim;
  private final String fileDelim;
  private final Time time;
  private DateTimeZone timeZone;
  private final S3SinkConnectorConfig connectorConfig;
  private static final Time SYSTEM_TIME = new SystemTime();
  private ErrantRecordReporter reporter;

  private final long maxWriteDurationMs;
  private long writeDeadline;

  boolean isPaused = false;

  private final FileRotationTracker fileRotationTracker;

  public TopicPartitionWriter(TopicPartition tp,
                              S3Storage storage,
                              RecordWriterProvider<S3SinkConnectorConfig> writerProvider,
                              Partitioner<?> partitioner,
                              S3SinkConnectorConfig connectorConfig,
                              SinkTaskContext context,
                              ErrantRecordReporter reporter) {
    this(tp, storage, writerProvider, partitioner, connectorConfig, context, SYSTEM_TIME, reporter);
  }

  // Visible for testing
  TopicPartitionWriter(TopicPartition tp,
                       S3Storage storage,
                       RecordWriterProvider<S3SinkConnectorConfig> writerProvider,
                       Partitioner<?> partitioner,
                       S3SinkConnectorConfig connectorConfig,
                       SinkTaskContext context,
                       Time time,
                       ErrantRecordReporter reporter
  ) {
    this.connectorConfig = connectorConfig;
    this.time = time;
    this.tp = tp;
    this.storage = storage;
    this.context = context;
    this.writerProvider = writerProvider;
    this.partitioner = partitioner;
    this.reporter = reporter;
    this.timestampExtractor = null;

    if (partitioner instanceof TimeBasedPartitioner) {
      this.timestampExtractor = ((TimeBasedPartitioner) partitioner).getTimestampExtractor();
      if (connectorConfig.isTombstoneWriteEnabled()) {
        this.timestampExtractor = new TombstoneTimestampExtractor(timestampExtractor);
      }
    }

    isTaggingEnabled = connectorConfig.getBoolean(S3SinkConnectorConfig.S3_OBJECT_TAGGING_CONFIG);
    extraTagKeyValuePair =
        connectorConfig.getList(S3SinkConnectorConfig.S3_OBJECT_TAGGING_EXTRA_KV);
    getS3Tag();
    ignoreTaggingErrors = connectorConfig.getString(
            S3SinkConnectorConfig.S3_OBJECT_BEHAVIOR_ON_TAGGING_ERROR_CONFIG)
            .equalsIgnoreCase(S3SinkConnectorConfig.IgnoreOrFailBehavior.IGNORE.toString());
    flushSize = connectorConfig.getInt(S3SinkConnectorConfig.FLUSH_SIZE_CONFIG);
    partitionerMaxOpenFiles = connectorConfig.getInt(
        S3SinkConnectorConfig.PARTITIONER_MAX_OPEN_FILES_CONFIG);
    topicsDir = connectorConfig.getString(StorageCommonConfig.TOPICS_DIR_CONFIG);
    rotateIntervalMs = connectorConfig.getLong(S3SinkConnectorConfig.ROTATE_INTERVAL_MS_CONFIG);
    if (rotateIntervalMs > 0 && timestampExtractor == null) {
      log.warn(
          "Property '{}' is set to '{}ms' but partitioner is not an instance of '{}'. This property"
              + " is ignored.",
          S3SinkConnectorConfig.ROTATE_INTERVAL_MS_CONFIG,
          rotateIntervalMs,
          TimeBasedPartitioner.class.getName()
      );
    }
    rotateScheduleIntervalMs =
        connectorConfig.getLong(S3SinkConnectorConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG);
    if (rotateScheduleIntervalMs > 0) {
      timeZone = DateTimeZone.forID(connectorConfig.getString(PartitionerConfig.TIMEZONE_CONFIG));
    }
    timeoutMs = connectorConfig.getLong(S3SinkConnectorConfig.RETRY_BACKOFF_CONFIG);
    compatibility = StorageSchemaCompatibility.getCompatibility(
        connectorConfig.getString(StorageSinkConnectorConfig.SCHEMA_COMPATIBILITY_CONFIG));

    buffer = new LinkedList<>();
    commitFiles = new LinkedHashMap<>();
    writers = new HashMap<>();
    currentSchemas = new HashMap<>();
    startOffsets = new HashMap<>();
    endOffsets = new HashMap<>();
    recordCounts = new HashMap<>();
    state = State.WRITE_STARTED;
    failureTime = -1L;
    currentOffset = -1L;
    dirDelim = connectorConfig.getString(StorageCommonConfig.DIRECTORY_DELIM_CONFIG);
    fileDelim = connectorConfig.getString(StorageCommonConfig.FILE_DELIM_CONFIG);
    extension = writerProvider.getExtension();
    zeroPadOffsetFormat = "%0"
        + connectorConfig.getInt(S3SinkConnectorConfig.FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG)
        + "d";
    fileRotationTracker = new FileRotationTracker();

    maxWriteDurationMs = connectorConfig.getLong(S3SinkConnectorConfig.MAX_WRITE_DURATION);
    writeDeadline = Long.MAX_VALUE;

    offsetToFilenameMap = new HashMap<>();

    // Initialize scheduled rotation timer if applicable
    setNextScheduledRotation();
  }

  private void getS3Tag() {
    hashMapTag = new HashMap<>();
    if (extraTagKeyValuePair.size() != 0) {
      for (int i = 0; i < extraTagKeyValuePair.size(); i++) {
        String[] singleKv = extraTagKeyValuePair.get(i).split(":");
        hashMapTag.put(singleKv[0], singleKv[1]);
      }
    }
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

  public void write() {
    long now = time.milliseconds();
    if (failureTime > 0 && now - failureTime < timeoutMs) {
      return;
    } else {
      failureTime = -1;
    }

    resetExpiredScheduledRotationIfNoPendingRecords(now);


    while (!buffer.isEmpty() && !isWriteDeadlineExceeded()) {
      try {
        executeState(now);
      } catch (IllegalWorkerStateException e) {
        throw new ConnectException(e);
      } catch (SchemaProjectorException e) {
        if (reporter != null) {
          reporter.report(buffer.poll(), e);
          log.warn("Errant record written to DLQ due to: {}", e.getMessage());
        } else {
          throw e;
        }
      }
    }
    if (!isWriteDeadlineExceeded()) {
      commitOnTimeIfNoData(now);
    }
    pauseOrResumeOnBuffer();

  }

  private void pauseOrResumeOnBuffer() {
    // if the deadline exceeds before all the records in buffer are processed, pause the writer
    // if the buffered records are greater than flush size, this is to ensure that we don't keep
    // getting messages from the consumer while we are still processing the buffer which can lead
    // to memory issues
    if (buffer.size() >= Math.max(flushSize, 1)) {
      pause();
    } else if (isPaused) {
      resume();
    }
  }

  public void setWriteDeadline(long currentTimeMs) {
    writeDeadline = currentTimeMs + maxWriteDurationMs;
    //prevent overflow
    if (writeDeadline < 0) {
      writeDeadline = Long.MAX_VALUE;
    }
  }

  protected boolean isWriteDeadlineExceeded() {
    boolean isWriteDeadlineExceeded = time.milliseconds() > writeDeadline;
    if (isWriteDeadlineExceeded) {
      log.info("Deadline exceeded");
    }
    return isWriteDeadlineExceeded;
  }

  @SuppressWarnings("fallthrough")
  private void executeState(long now) {
    switch (state) {
      case WRITE_STARTED:
        pause();
        nextState();
        // fallthrough
      case WRITE_PARTITION_PAUSED:
        SinkRecord record = buffer.peek();

        if (timestampExtractor != null) {
          currentTimestamp = timestampExtractor.extract(record, now);
          if (baseRecordTimestamp == null) {
            baseRecordTimestamp = currentTimestamp;
          }
        }
        Schema valueSchema = record.valueSchema();
        String encodedPartition;
        try {
          encodedPartition = partitioner.encodePartition(record, now);
        } catch (PartitionException e) {
          if (reporter != null) {
            reporter.report(record, e);
            buffer.poll();
            break;
          } else {
            throw e;
          }
        }

        if (offsetToFilenameMap.size() < connectorConfig.getInt(MAX_FILE_SCAN_LIMIT_CONFIG)) {
          offsetToFilenameMap.put(record.kafkaOffset(), getCommitFilename(record));
        }

        Schema currentValueSchema = currentSchemas.get(encodedPartition);
        // Rotation will happen for:
        // 1. non-tombstone followed by tombstone
        // 2. tombstone (valueSchema is null) followed by non-tombstone
        boolean shouldRotateForNullSchema = currentSchemas.containsKey(encodedPartition)
                && (currentValueSchema == null ^ valueSchema == null);

        // TB: Tombstone, NTB: Non-Tombstone, -> : followed by
        // Cases handled: TB -> TB, TB -> NTB, NTB -> TB
        // NTB -> NTB is handled by schema compatibility checks
        if (currentValueSchema == null || valueSchema == null) {
          currentSchemas.put(encodedPartition, valueSchema);
          currentValueSchema = valueSchema;
        }

        if (!checkRotationOrAppend(
            record,
            currentValueSchema,
            valueSchema,
            encodedPartition,
            now, shouldRotateForNullSchema
        )) {
          break;
        }
        // fallthrough
      case SHOULD_ROTATE:
        if (isWriteDeadlineExceeded()) {
          // note: this is a best-effort attempt to rotate the file before the deadline
          // this check can pass and the deadline gets exceeded before the rotation is complete
          break;
        }
        commitFiles();
        nextState();
        // fallthrough
      case FILE_COMMITTED:
        setState(State.WRITE_PARTITION_PAUSED);
        break;
      default:
        log.error("{} is not a valid state to write record for topic partition {}.", state, tp);
    }
  }

  /**
   * Check if we should rotate the file (schema change, time-based).
   * @returns true if rotation is being performed, false otherwise
   */
  private boolean checkRotationOrAppend(
      SinkRecord record,
      Schema currentValueSchema,
      Schema valueSchema,
      String encodedPartition,
      long now,
      boolean shouldRotateForNullSchema
  ) {

    if (shouldRotateForNullSchema) {
      fileRotationTracker.incrementRotationByNullSchemaCount(encodedPartition);
      nextState();
      return true;
    }

    // rotateOnTime is safe to go before writeRecord, because it is acceptable
    // even for a faulty record to trigger time-based rotation if it applies
    if (rotateOnTime(encodedPartition, currentTimestamp, now)) {
      setNextScheduledRotation();
      nextState();
      return true;
    }

    SchemaCompatibilityResult shouldChangeSchema =
        compatibility.shouldChangeSchema(record, null, currentValueSchema);
    if (shouldChangeSchema.isInCompatible() && recordCount > 0) {
      fileRotationTracker.incrementRotationBySchemaChangeCount(encodedPartition,
          shouldChangeSchema.getSchemaIncompatibilityType());
      // This branch is never true for the first record read by this TopicPartitionWriter
      log.trace(
          "Incompatible change of schema detected for record '{}' with encoded partition "
          + "'{}' and current offset: '{}'",
          record,
          encodedPartition,
          currentOffset
      );
      currentSchemas.put(encodedPartition, valueSchema);
      nextState();
      return true;
    }

    if (rotateOnPartitionerMaxOpenFiles(encodedPartition)) {
      fileRotationTracker.incrementRotationByPartitionerMaxOpenFilesCount(encodedPartition);
      log.info(
          "Starting commit and rotation for topic partition {} with start offset {}",
          tp,
          startOffsets
      );
      nextState();
      return true;
    }

    SinkRecord projectedRecord = compatibility.project(record, null, currentValueSchema);
    boolean validRecord = writeRecord(projectedRecord, encodedPartition, record);
    buffer.poll();
    if (!validRecord) {
      // skip the faulty record and don't rotate
      return false;
    }

    if (rotateOnSize()) {
      fileRotationTracker.incrementRotationByFlushSizeCount(encodedPartition);
      log.info(
          "Starting commit and rotation for topic partition {} with start offset {}",
          tp,
          startOffsets
      );
      nextState();
      return true;
    }

    return false;
  }

  private boolean rotateOnPartitionerMaxOpenFiles(String encodedPartition) {
    if (partitionerMaxOpenFiles == -1) {
      return false;
    }

    boolean rotate = !commitFiles.containsKey(encodedPartition)
        && commitFiles.size() == partitionerMaxOpenFiles;
    log.trace("Should apply rotation on max open files for topic-partition '{}': "
            + "(partitionerMaxOpenFiles: '{}', commitFiles.size(): '{}')? {}",
        tp, partitionerMaxOpenFiles, commitFiles.size(), rotate);
    return rotate;
  }

  private void commitOnTimeIfNoData(long now) {
    if (buffer.isEmpty()) {
      // committing files after waiting for rotateIntervalMs time but less than flush.size
      // records available
      if (recordCount > 0 && rotateOnTime(currentEncodedPartition, currentTimestamp, now)) {
        log.info(
            "Committing files after waiting for rotateIntervalMs time but less than flush.size "
            + "records available."
        );
        setNextScheduledRotation();

        commitFiles();
      }

      resume();
      setState(State.WRITE_STARTED);
    }
  }

  private void resetExpiredScheduledRotationIfNoPendingRecords(long now) {
    if (recordCount == 0 && shouldApplyScheduledRotation(now)) {
      setNextScheduledRotation();
    }
  }

  public void close() throws ConnectException {
    log.debug("Closing TopicPartitionWriter {}", tp);
    for (RecordWriter writer : writers.values()) {
      writer.close();
    }
    writers.clear();
    startOffsets.clear();
  }

  public void buffer(SinkRecord sinkRecord) {
    buffer.add(sinkRecord);
  }

  public Long getOffsetToCommitAndReset() {
    Long latest = offsetToCommit;
    offsetToCommit = null;
    return latest;
  }

  public Long currentStartOffset() {
    return minStartOffset();
  }

  public void failureTime(long when) {
    this.failureTime = when;
  }

  public FileRotationTracker getFileRotationTracker() {
    return fileRotationTracker;
  }

  public StorageSchemaCompatibility getSchemaCompatibility() {
    return compatibility;
  }

  private Long minStartOffset() {
    Optional<Long> minStartOffset = startOffsets.values().stream()
        .min(Comparator.comparing(Long::valueOf));
    return minStartOffset.isPresent() ? minStartOffset.get() : null;
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

  private boolean rotateOnPartitionChange(String encodedPartition) {
    return connectorConfig.shouldRotateOnPartitionChange()
        && !encodedPartition.equals(currentEncodedPartition);
  }

  private boolean rotateOnTime(String encodedPartition, Long recordTimestamp, long now) {
    if (recordCount <= 0) {
      return false;
    }
    // rotateIntervalMs > 0 implies timestampExtractor != null
    boolean periodicRotation = rotateIntervalMs > 0
        && timestampExtractor != null
        && (
        recordTimestamp - baseRecordTimestamp >= rotateIntervalMs
            || rotateOnPartitionChange(encodedPartition)
    );

    log.trace(
        "Checking rotation on time for topic-partition '{}' "
            + "with recordCount '{}' and encodedPartition '{}'",
        tp,
        recordCount,
        encodedPartition
    );

    log.trace(
        "Should apply periodic time-based rotation for topic-partition '{}':"
            + " (rotateIntervalMs: '{}', baseRecordTimestamp: "
            + "'{}', timestamp: '{}', encodedPartition: '{}', currentEncodedPartition: '{}')? {}",
        tp,
        rotateIntervalMs,
        baseRecordTimestamp,
        recordTimestamp,
        encodedPartition,
        currentEncodedPartition,
        periodicRotation
    );
    if (periodicRotation) {
      fileRotationTracker.incrementRotationByRotationIntervalCount(encodedPartition);
    } else if (shouldApplyScheduledRotation(now)) {
      fileRotationTracker.incrementRotationByScheduledRotationIntervalCount(encodedPartition);
    }
    return periodicRotation || shouldApplyScheduledRotation(now);
  }

  private boolean shouldApplyScheduledRotation(long now) {
    boolean scheduledRotation = rotateScheduleIntervalMs > 0 && now >= nextScheduledRotation;
    log.trace(
        "Should apply scheduled rotation for topic-partition '{}':"
            + " (rotateScheduleIntervalMs: '{}', nextScheduledRotation:"
            + " '{}', now: '{}')? {}",
        tp,
        rotateScheduleIntervalMs,
        nextScheduledRotation,
        now,
        scheduledRotation
    );
    return scheduledRotation;
  }

  private void setNextScheduledRotation() {
    if (rotateScheduleIntervalMs > 0) {
      long now = time.milliseconds();
      nextScheduledRotation = DateTimeUtils.getNextTimeAdjustedByDay(
          now,
          rotateScheduleIntervalMs,
          timeZone
      );
      if (log.isDebugEnabled()) {
        log.debug(
            "Update scheduled rotation timer for topic-partition '{}': "
                + "(rotateScheduleIntervalMs: '{}', nextScheduledRotation: '{}', now: '{}'). "
                + "Next rotation will be at {}",
            tp,
            rotateScheduleIntervalMs,
            nextScheduledRotation,
            now,
            new DateTime(nextScheduledRotation).withZone(timeZone).toString()
        );
      }
    }
  }

  private boolean rotateOnSize() {
    boolean messageSizeRotation = recordCount >= flushSize;
    log.trace("Should apply size-based rotation for topic-partition '{}':"
            + " (count {} >= flush size {})? {}",
        tp,
        recordCount,
        flushSize,
        messageSizeRotation
    );
    return messageSizeRotation;
  }

  private void pause() {
    log.trace("Pausing writer for topic-partition '{}'", tp);
    context.pause(tp);
    isPaused = true;
  }

  private void resume() {
    log.trace("Resuming writer for topic-partition '{}'", tp);
    context.resume(tp);
    isPaused = false;
  }

  private RecordWriter newWriter(SinkRecord record, String encodedPartition)
      throws ConnectException {
    String commitFilename = getCommitFilename(encodedPartition);
    log.debug(
        "Creating new writer encodedPartition='{}' filename='{}'",
        encodedPartition,
        commitFilename
    );
    RecordWriter writer = writerProvider.getRecordWriter(connectorConfig, commitFilename);
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

  String getCommitFilename(SinkRecord sinkRecord) {
    String prefix = getDirectoryPrefix(partitioner.encodePartition(sinkRecord));
    return fileKeyToCommit(prefix, sinkRecord.kafkaOffset());
  }

  private String fileKey(String topicsPrefix, String keyPrefix, String name) {
    String suffix = keyPrefix + dirDelim + name;
    return StringUtils.isNotBlank(topicsPrefix)
           ? topicsPrefix + dirDelim + suffix
           : suffix;
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

  private boolean writeRecord(SinkRecord projectedRecord, String encodedPartition,
                              SinkRecord originalRecord) {
    RecordWriter writer = writers.get(encodedPartition);
    long currentOffsetIfSuccessful = projectedRecord.kafkaOffset();
    boolean shouldRemoveWriter = false;
    boolean shouldRemoveStartOffset = false;
    boolean shouldRemoveCommitFilename = false;
    try {
      if (!startOffsets.containsKey(encodedPartition)) {
        log.trace(
            "Setting writer's start offset for '{}' to {}",
            encodedPartition,
            currentOffsetIfSuccessful
        );
        startOffsets.put(encodedPartition, currentOffsetIfSuccessful);
        shouldRemoveStartOffset = true;
      }
      if (writer == null) {
        if (!commitFiles.containsKey(encodedPartition)) {
          shouldRemoveCommitFilename = true;
        }
        writer = newWriter(projectedRecord, encodedPartition);
        shouldRemoveWriter = true;
      }
      writer.write(projectedRecord);
    } catch (DataException | SchemaParseException | InvalidSchemaException e) {
      if (reporter != null) {
        if (shouldRemoveStartOffset) {
          startOffsets.remove(encodedPartition);
        }
        if (shouldRemoveWriter) {
          writers.remove(encodedPartition);
        }
        if (shouldRemoveCommitFilename) {
          commitFiles.remove(encodedPartition);
        }
        reporter.report(originalRecord, e);
        log.warn("Errant record written to DLQ due to: {}", e.getMessage());
        return false;
      } else {
        throw new ConnectException(e);
      }
    }

    currentEncodedPartition = encodedPartition;
    currentOffset = projectedRecord.kafkaOffset();
    if (shouldRemoveStartOffset) {
      log.trace(
          "Setting writer's start offset for '{}' to {}",
          currentEncodedPartition,
          currentOffset
      );

      // Once we have a "start offset" for a particular "encoded partition"
      // value, we know that we have at least one record. This allows us
      // to initialize all our maps at the same time, and saves future
      // checks on the existence of keys
      recordCounts.put(currentEncodedPartition, 0L);
      endOffsets.put(currentEncodedPartition, 0L);
    }
    ++recordCount;

    recordCounts.put(currentEncodedPartition, recordCounts.get(currentEncodedPartition) + 1);
    endOffsets.put(currentEncodedPartition, currentOffset);
    return true;
  }

  private void commitFiles() {
    for (Map.Entry<String, String> entry : commitFiles.entrySet()) {
      String encodedPartition = entry.getKey();
      commitFile(encodedPartition);
      if (isTaggingEnabled) {
        RetryUtil.exponentialBackoffRetry(() -> tagFile(
                    encodedPartition,
                    entry.getValue(),
                    hashMapTag
                ),
                ConnectException.class,
                connectorConfig.getInt(S3_PART_RETRIES_CONFIG),
                connectorConfig.getLong(S3_RETRY_BACKOFF_CONFIG)
        );
      }
      startOffsets.remove(encodedPartition);
      endOffsets.remove(encodedPartition);
      recordCounts.remove(encodedPartition);
      log.debug("Committed {} for {}", entry.getValue(), tp);
    }

    offsetToCommit = currentOffset + 1;
    commitFiles.clear();
    currentSchemas.clear();
    offsetToFilenameMap.clear();

    recordCount = 0;
    baseRecordTimestamp = null;
    log.info("Files committed to S3. Target commit offset for {} is {}", tp, offsetToCommit);
  }

  private void commitFile(String encodedPartition) {
    if (!startOffsets.containsKey(encodedPartition)) {
      log.warn("Tried to commit file with missing starting offset partition: {}. Ignoring.");
      return;
    }

    if (writers.containsKey(encodedPartition)) {
      RecordWriter writer = writers.get(encodedPartition);
      // Commits the file and closes the underlying output stream.
      tryCommitFile(writer, encodedPartition);
      writers.remove(encodedPartition);
      log.debug("Removed writer for '{}'", encodedPartition);
    }
  }

  private void tryCommitFile(RecordWriter writer, String encodedPartition) {
    try {
      writer.commit();
    } catch (FileExistsException e) {
      long nextStartOffset = findNextAvailableFile(encodedPartition);
      log.info("Next available offset for encoded partition {} is {}",
          encodedPartition, nextStartOffset);
      startOffsets.put(encodedPartition, nextStartOffset);
      throw e;
    }
  }

  public long findNextAvailableFile(String encodedPartition) {
    long startOffset = startOffsets.get(encodedPartition) + 1;
    long targetEndOffset = startOffset + connectorConfig.getInt(MAX_FILE_SCAN_LIMIT_CONFIG);
    log.info("Scanning for available files for start_offset:{} and file {}",
        startOffset, commitFiles.get(encodedPartition));
    do {
      String commitFile = offsetToFilenameMap.get(startOffset);
      try {
        if (!offsetToFilenameMap.containsKey(startOffset)) {
          log.info("Start offset {} not present in offsets map. "
              + "Considering {} as next offset to process from", startOffset, startOffset);
          return startOffset;
        }
        if (!storage.exists(offsetToFilenameMap.get(startOffset))) {
          log.info("File {} does not exist in S3. Next target offset to reset to is {}",
              offsetToFilenameMap.get(startOffset), startOffset);
          return startOffset;
        }
        log.debug("File {} already exists, checking for next available file", commitFile);
      } catch (AmazonS3Exception e) {
        if (e.getStatusCode() == 403) {
          log.warn("Connector failed with 403 error. Incrementing offset by 1", e);
          return startOffset;
        }
        throw e;
      }
      startOffset++;
    } while (startOffset < targetEndOffset);
    log.info("Max scanning limit reached. Resetting offset to {}", targetEndOffset);
    return targetEndOffset;
  }

  private void tagFile(
      String encodedPartition,
      String s3ObjectPath,
      Map<String,String> extraHashMapTag) {
    Long startOffset = startOffsets.get(encodedPartition);
    Long endOffset = endOffsets.get(encodedPartition);
    Long recordCount = recordCounts.get(encodedPartition);
    if (startOffset == null || endOffset == null || recordCount == null) {
      log.warn(
          "Missing tags when attempting to tag file {}. "
              + "Starting offset tag: {}, "
              + "ending offset tag: {}, "
              + "record count tag: {}. Ignoring.",
          encodedPartition,
          startOffset == null ? "missing" : startOffset,
          endOffset == null ? "missing" : endOffset,
          recordCount == null ? "missing" : recordCount
      );
      return;
    }

    log.debug("Object to tag is: {}", s3ObjectPath);
    Map<String, String> tags = new HashMap<>();
    tags.put("startOffset", Long.toString(startOffset));
    tags.put("endOffset", Long.toString(endOffset));
    tags.put("recordCount", Long.toString(recordCount));
    if (extraHashMapTag != null) {
      tags.putAll(extraHashMapTag);
    }
    try {
      storage.addTags(s3ObjectPath, tags);
      log.info("Tagged S3 object {} with starting offset {}, ending offset {}, record count {}",
          s3ObjectPath, startOffset, endOffset, recordCount);
    } catch (SdkClientException e) {
      if (ignoreTaggingErrors) {
        log.warn("Unable to tag S3 object {}. Ignoring.", s3ObjectPath, e);
      } else {
        throw new ConnectException(String.format("Unable to tag S3 object %s", s3ObjectPath), e);
      }
    } catch (Exception e) {
      if (ignoreTaggingErrors) {
        log.warn("Unrecoverable exception while attempting to tag S3 object {}. Ignoring.",
                s3ObjectPath, e);
      } else {
        throw new ConnectException(String.format("Unable to tag S3 object %s", s3ObjectPath), e);
      }
    }
  }
}
