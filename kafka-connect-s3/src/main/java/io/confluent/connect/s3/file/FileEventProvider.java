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

package io.confluent.connect.s3.file;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

public abstract class FileEventProvider implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(FileEventProvider.class);
  protected final String configJson;
  protected final boolean skipError;

  public FileEventProvider(String configJson, boolean skipError) {
    this.configJson = configJson;
    this.skipError = skipError;
  }

  public String formatDateRFC3339(DateTime timestamp){
    DateTimeFormatter fmt = ISODateTimeFormat.dateTime();
    return fmt.print(timestamp);
  }

  public void call(
          String topicName,
          String s3Partition,
          String filePath,
          int partition,
          DateTime baseRecordTimestamp,
          DateTime currentTimestamp,
          int recordCount,
          DateTime eventDatetime) {
    try {
      log.info("Running file event : {}, {}", topicName, filePath);
      callImpl(topicName, s3Partition, filePath, partition, baseRecordTimestamp, currentTimestamp, recordCount, eventDatetime);
    } catch (Exception e) {
      if (skipError) {
        log.error(e.getMessage(), e);
      } else {
        throw new RuntimeException(e);
      }
    }
  }
  public abstract void callImpl(
      String topicName,
      String s3Partition,
      String filePath,
      int partition,
      DateTime baseRecordTimestamp,
      DateTime currentTimestamp,
      int recordCount,
      DateTime eventDatetime);
}
