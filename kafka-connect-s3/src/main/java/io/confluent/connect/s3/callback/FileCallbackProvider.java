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

package io.confluent.connect.s3.callback;

public abstract class FileCallbackProvider {
  protected final String configJson;
  protected final boolean skipError;

  public FileCallbackProvider(String configJson, boolean skipError) {
    this.configJson = configJson;
    this.skipError = skipError;
  }

  public abstract void call(
      String topicName,
      String s3Partition,
      String filePath,
      int partition,
      Long baseRecordTimestamp,
      Long currentTimestamp,
      int recordCount);
}
