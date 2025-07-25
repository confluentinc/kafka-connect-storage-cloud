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

package io.confluent.connect.s3.storage;

import io.confluent.connect.s3.S3SinkConnectorConfig;

import java.io.IOException;

import software.amazon.awssdk.services.s3.S3Client;

public class S3ParquetOutputStream extends S3OutputStream {

  private volatile boolean commit;

  public S3ParquetOutputStream(String key, S3SinkConnectorConfig conf, S3Client s3) {
    super(key, conf, s3);
    commit = false;
  }

  @Override
  public void close() throws IOException {
    if (commit) {
      super.commit();
      commit = false;
    } else {
      super.close();
    }
  }

  public void setCommit() {
    commit = true;
  }
}
