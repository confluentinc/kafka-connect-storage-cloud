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

import io.confluent.connect.storage.partitioner.TimeBasedPartitioner.RecordTimestampExtractor;
import io.confluent.connect.storage.partitioner.TimestampExtractor;
import java.util.Map;
import org.apache.kafka.connect.connector.ConnectRecord;

public class TombstoneTimestampExtractor implements TimestampExtractor {

  private final TimestampExtractor delegateTimestampExtractor;
  private static final TimestampExtractor recordTimestampExtractor
      = new RecordTimestampExtractor();

  public TombstoneTimestampExtractor(TimestampExtractor delegateTimestampExtractor) {
    this.delegateTimestampExtractor = delegateTimestampExtractor;
  }

  @Override
  public void configure(Map<String, Object> map) {
    delegateTimestampExtractor.configure(map);
    recordTimestampExtractor.configure(map);
  }

  @Override
  public Long extract(ConnectRecord<?> connectRecord) {
    if (connectRecord.value() == null) {
      return recordTimestampExtractor.extract(connectRecord);
    }
    return delegateTimestampExtractor.extract(connectRecord);
  }
}
