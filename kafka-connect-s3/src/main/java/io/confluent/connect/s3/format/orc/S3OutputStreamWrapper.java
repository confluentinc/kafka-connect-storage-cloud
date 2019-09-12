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

package io.confluent.connect.s3.format.orc;

import com.amazonaws.services.s3.AmazonS3;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.storage.S3OutputStream;

import java.io.IOException;

public class S3OutputStreamWrapper extends S3OutputStream {

  private boolean shouldBeCommitted;

  public S3OutputStreamWrapper(String key, S3SinkConnectorConfig conf, AmazonS3 s3) {
    super(key, conf, s3);
  }


  public void commitBeforeClose() {
    shouldBeCommitted = true;
  }

  @Override
  public void close() throws IOException {
    if (shouldBeCommitted) {
      shouldBeCommitted = false;
      commit();
    } else {
      super.close();
    }
  }
}
