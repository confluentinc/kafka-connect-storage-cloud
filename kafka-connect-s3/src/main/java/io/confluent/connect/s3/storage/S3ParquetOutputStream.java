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

import java.io.IOException;

public class S3ParquetOutputStream extends S3OutputStream {

  private boolean committed;

  public S3ParquetOutputStream(S3OutputStream s3OutputStream) {
    super(s3OutputStream.getKey(), s3OutputStream.getConnectorConfig(), s3OutputStream.getS3());
    committed = false;
  }

  @Override
  public void close() throws IOException {
    if (!committed) {
      committed = true;
      super.commit();
    } else {
      super.close();
    }
  }
}
