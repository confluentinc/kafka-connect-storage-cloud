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

package io.confluent.connect.s3.util;

import org.apache.kafka.common.TopicPartition;

import io.confluent.connect.s3.S3SinkConnectorConstants;

import static io.confluent.connect.s3.S3SinkConnectorConstants.DIRECTORY_SEPARATOR;

public class FileUtils {

  public static String topicPrefix(String bucketName, String topicsPrefix, String topic) {
    return bucketName + DIRECTORY_SEPARATOR + topicsPrefix + DIRECTORY_SEPARATOR + topic;
  }

  public static String directoryPrefix(String bucketName, String topicsPrefix, String dirPrefix) {
    return bucketName + DIRECTORY_SEPARATOR + topicsPrefix + DIRECTORY_SEPARATOR + dirPrefix;
  }

  public static String directoryPrefix(String bucketName, String topicsPrefix, TopicPartition tp) {
    String topic = tp.topic();
    int partition = tp.partition();
    return bucketName + DIRECTORY_SEPARATOR + topicsPrefix + DIRECTORY_SEPARATOR + topic + DIRECTORY_SEPARATOR
               + partition;
  }

  public static String fileKey(String bucketName, String topicsPrefix, TopicPartition tp, String name) {
    String topic = tp.topic();
    int partition = tp.partition();
    return bucketName + DIRECTORY_SEPARATOR + topicsPrefix + DIRECTORY_SEPARATOR + topic + DIRECTORY_SEPARATOR
               + partition + DIRECTORY_SEPARATOR + name;
  }

  public static String fileKey(String bucketName, String topicsPrefix, String dirPrefix, String name) {
    return bucketName + DIRECTORY_SEPARATOR + topicsPrefix + DIRECTORY_SEPARATOR + dirPrefix + DIRECTORY_SEPARATOR
               + name;
  }

  public static String fileKey(String topicsPrefix, String keyPrefix, String name) {
    return DIRECTORY_SEPARATOR + topicsPrefix + DIRECTORY_SEPARATOR + keyPrefix + DIRECTORY_SEPARATOR + name;
  }

  public static String fileKeyToCommit(String topicsPrefix, String dirPrefix, TopicPartition tp, long startOffset,
                                       String extension, String zeroPadFormat) {
    String name = tp.topic()
                      + S3SinkConnectorConstants.COMMITTED_FILENAME_SEPARATOR
                      + tp.partition()
                      + S3SinkConnectorConstants.COMMITTED_FILENAME_SEPARATOR
                      + String.format(zeroPadFormat, startOffset)
                      + extension;
    return fileKey(topicsPrefix, dirPrefix, name);
  }

}
