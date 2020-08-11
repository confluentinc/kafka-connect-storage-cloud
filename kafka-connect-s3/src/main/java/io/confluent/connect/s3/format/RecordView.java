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

package io.confluent.connect.s3.format;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * An interface to get the schema for all three (Key, Vale, Header) record portions
 * in a consistent way.
 */
public interface RecordView {

  /**
   * The schema of the record view. eg. record.keySchema when the RecordView is KeyRecordView.
   *
   * @param record the SinkRecord to get the view schema on.
   * @param enveloped whether the schema should be enveloped in a struct
   *                  (applicable for keys/headers in Parquet formats)
   * @return the schema of the current record view
   */
  Schema getViewSchema(SinkRecord record, boolean enveloped);

  /**
   * The value of the current record view. ed. record.key when the RecordView is KeyRecordView.
   *
   * @param record the SinkRecord to get the value from
   * @param enveloped whether the view should be enveloped in a struct
   *                  (applicable for keys/headers in Parquet formats)
   * @return the value based on the current RecordView
   */
  Object getView(SinkRecord record, boolean enveloped);

  /**
   * Get the extension for the current RecordView, eg. .keys for KeyRecordView.
   *
   * @return the view's file extension
   */
  String getExtension();
}
