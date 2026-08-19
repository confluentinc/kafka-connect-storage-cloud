/*
 * Copyright 2025 Confluent Inc.
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

import io.confluent.connect.s3.backup.S3StorageWriter;
import io.confluent.connect.storage.backup.BackupEnvelope;
import io.confluent.connect.storage.backup.BackupModeValidator;
import io.confluent.connect.storage.backup.ConverterTypeDetector;
import io.confluent.connect.storage.backup.ObjectStoreSchemaBackupStore;
import io.confluent.connect.storage.backup.SchemaBackupStore;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.format.backup.EnvelopeTransformer;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

/**
 * Backup-specific sink task that pre-wraps records in an envelope
 * before delegating to the parent {@link S3SinkTask}.
 *
 * <p>Records are transformed into envelope-shaped SinkRecords in
 * {@code put()}, before they reach {@code TopicPartitionWriter}.
 * This means all schema rotation decisions (key changes, value changes,
 * tombstone transitions) are based on the envelope schema, which
 * naturally handles every case without special-case logic.
 *
 * <p>Schema files (.entry.json + .avsc/.proto/.json) are backed up
 * as a side effect during wrapping. Writes are <b>idempotent</b>:
 * the S3 path is keyed by schema ID and the content (raw schema bytes,
 * entry.json fields) is a pure function of that key, so two tasks
 * writing the same schema always produce identical bytes and S3
 * last-write-wins is a no-op.
 *
 * <p>To avoid redundant work, {@link ObjectStoreSchemaBackupStore}
 * applies <b>3-level deduplication</b>:
 * <ol>
 *   <li>In-memory {@code ConcurrentHashMap}-backed set of already-backed-up
 *       schema keys (blocks re-attempts within the same task JVM).</li>
 *   <li>S3 {@code HEAD} on the entry.json path (blocks cross-task
 *       double-writes when another task has already completed).</li>
 *   <li>Byte-identical write if both prior levels miss (writes still
 *       succeed; last-write-wins produces the same bytes).</li>
 * </ol>
 */
public class BackupS3SinkTask extends S3SinkTask {

  private static final Logger log = LoggerFactory.getLogger(BackupS3SinkTask.class);

  private EnvelopeTransformer envelopeTransformer;

  @Override
  public void start(Map<String, String> props) {
    super.start(props);

    String topicsDir = connectorConfig.getString(
        StorageCommonConfig.TOPICS_DIR_CONFIG);
    String dirDelim = connectorConfig.getString(
        StorageCommonConfig.DIRECTORY_DELIM_CONFIG);

    SchemaBackupStore backupStore = new ObjectStoreSchemaBackupStore(
        new S3StorageWriter(storage), topicsDir, dirDelim);

    Map<String, String> originals = connectorConfig.originalsStrings();
    String keyType = ConverterTypeDetector.detectSchemaType(
        originals.get(BackupEnvelope.KEY_CONVERTER_CONFIG),
        originals, BackupEnvelope.KEY_CONVERTER_CONFIG);
    String valueType = ConverterTypeDetector.detectSchemaType(
        originals.get(BackupEnvelope.VALUE_CONVERTER_CONFIG),
        originals, BackupEnvelope.VALUE_CONVERTER_CONFIG);

    envelopeTransformer = new EnvelopeTransformer(
        backupStore, keyType, valueType);

    BackupModeValidator.logSinkStartupSummary(
        originals, connectorConfig.formatClass().getSimpleName(),
        keyType, valueType);
    log.info("Started backup-mode S3 sink task (keyType={}, valueType={}, format={})",
        keyType, valueType, connectorConfig.formatClass().getSimpleName());
  }

  @Override
  public void put(Collection<SinkRecord> records) throws ConnectException {
    super.put(envelopeTransformer.wrapAll(records));
  }

  @Override
  public void stop() {
    envelopeTransformer = null;
    super.stop();
  }

}
