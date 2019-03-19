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

package io.confluent.connect.s3.util;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class TimeUtils {

  public static String encodeTimestamp(long partitionDurationMs, String pathFormat, String timeZoneString, long timestamp) {
    DateTimeZone timeZone = DateTimeZone.forID(timeZoneString);
    DateTimeFormatter formatter = DateTimeFormat.forPattern(pathFormat).withZone(timeZone);
    DateTime partition = new DateTime(getPartition(partitionDurationMs, timestamp, timeZone));
    return partition.toString(formatter);
  }

  private static long getPartition(long timeGranularityMs, long timestamp, DateTimeZone timeZone) {
    long adjustedTimeStamp = timeZone.convertUTCToLocal(timestamp);
    long partitionedTime = (adjustedTimeStamp / timeGranularityMs) * timeGranularityMs;
    return timeZone.convertLocalToUTC(partitionedTime, false);
  }

}
