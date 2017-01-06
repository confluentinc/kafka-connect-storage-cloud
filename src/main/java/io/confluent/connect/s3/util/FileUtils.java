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

package io.confluent.connect.s3.util;

import org.apache.kafka.common.TopicPartition;

import io.confluent.connect.s3.S3SinkConnectorConstants;

public class FileUtils {

  public static String fileName(String url, String topicsPrefix, String keyPrefix, String name) {
    return url + "/" + topicsPrefix + "/" + keyPrefix + "/" + name;
  }

  public static String committedFileName(String url, String topicsPrefix, String keyPrefix, TopicPartition topicPart,
                                         long startOffset, String extension, String zeroPadFormat) {
    String name = topicPart.topic()
                      + S3SinkConnectorConstants.COMMITTED_FILENAME_SEPARATOR
                      + topicPart.partition()
                      + S3SinkConnectorConstants.COMMITTED_FILENAME_SEPARATOR
                      + String.format(zeroPadFormat, startOffset)
                      + extension;
    return fileName(url, topicsPrefix, keyPrefix, name);
  }

}
