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

import io.confluent.connect.storage.common.util.StringUtils;

public class FileUtils {
  public static final String TEST_FILE_DELIM = "#";
  public static final String TEST_DIRECTORY_DELIM = "_";

  public static String fileKey(String topicsPrefix, String keyPrefix, String name) {
    String suffix = keyPrefix + TEST_DIRECTORY_DELIM + name;
    return StringUtils.isNotBlank(topicsPrefix)
           ? topicsPrefix + TEST_DIRECTORY_DELIM + suffix
           : suffix;
  }

  public static String fileKeyToCommit(String topicsPrefix, String dirPrefix, TopicPartition tp, long startOffset,
                                       String extension, String zeroPadFormat) {
    String name = tp.topic()
                      + TEST_FILE_DELIM
                      + tp.partition()
                      + TEST_FILE_DELIM
                      + String.format(zeroPadFormat, startOffset)
                      + extension;
    return fileKey(topicsPrefix, dirPrefix, name);
  }

}
