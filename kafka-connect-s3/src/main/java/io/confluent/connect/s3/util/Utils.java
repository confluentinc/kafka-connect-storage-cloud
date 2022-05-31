/*
 * Copyright 2020 Confluent Inc.
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

import io.confluent.connect.s3.format.RecordView;
import org.apache.kafka.connect.sink.SinkRecord;

public class Utils {

  /**
   * Get the filename for the respective record view. Appends the Value, Key or Header file
   * extensions before the existing file extension. Typically unchanged for the Value view.
   *
   * @param recordView the record view (key, header or value)
   * @param filename the current name of the file, equivalent
   * @param initialExtension the file extension without the appended view extension, eg. .avro
   * @return the filename with the view extension appended, eg. file1.keys.avro
   */
  public static String getAdjustedFilename(RecordView recordView, String filename,
      String initialExtension) {
    if (filename.endsWith(initialExtension)) {
      int index = filename.lastIndexOf(initialExtension);
      return filename.substring(0, index) + recordView.getExtension() + initialExtension;
    } else {
      // filename is already stripped
      return filename + recordView.getExtension() + initialExtension;
    }
  }

  /**
   * Returns a safe string representation of the sink record that can be logged in the logs.
   *
   * @param sinkRecord the record
   * @return the safe string representation that can be logged
   */
  public static String sinkRecordToLoggableString(SinkRecord sinkRecord) {
    return "SinkRecord{kafkaOffset=" + sinkRecord.kafkaOffset() + ", topic='" + sinkRecord.topic()
        + "', kafkaPartition=" + sinkRecord.kafkaPartition() + "} ";
  }
}
