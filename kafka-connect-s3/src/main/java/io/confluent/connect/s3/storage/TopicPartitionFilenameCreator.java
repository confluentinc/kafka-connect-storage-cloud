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

package io.confluent.connect.s3.storage;

import io.confluent.connect.storage.StorageSinkConnectorConfig;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.common.util.StringUtils;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Map;

/**
 * Default FilenameCreator if none specified.
 * It creates a filename based on the topic, partition, and offset.
 * implements FilenameCreator interface.
 * @author Pierre Stridsberg
 */
public class TopicPartitionFilenameCreator implements FilenameCreator {
  private String fileDelim;
  private String dirDelim;
  private String zeroPadOffsetFormat;
  private String topicsDir;

  @Override
  public void configure(Map<String, Object> config) {
    dirDelim = (String) config.get(StorageCommonConfig.DIRECTORY_DELIM_CONFIG);
    fileDelim = (String) config.get(StorageCommonConfig.FILE_DELIM_CONFIG);
    zeroPadOffsetFormat = "%0"
        + config.get(StorageSinkConnectorConfig.FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG)
        + "d";
    topicsDir = (String) config.get(StorageCommonConfig.TOPICS_DIR_CONFIG);
  }

  @Override
  public String createName(SinkRecord sinkRecord,
                           String dirPrefix,
                           long startOffset,
                           String extension) {
    String name = sinkRecord.topic()
        + fileDelim
        + sinkRecord.kafkaPartition()
        + fileDelim
        + String.format(zeroPadOffsetFormat, startOffset)
        + extension;
    return fileKey(topicsDir, dirPrefix, name);
  }

  private String fileKey(String topicsPrefix, String keyPrefix, String name) {
    String suffix = keyPrefix + dirDelim + name;
    return StringUtils.isNotBlank(topicsPrefix)
        ? topicsPrefix + dirDelim + suffix
        : suffix;
  }
}
