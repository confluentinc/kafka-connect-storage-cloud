/*
 * Copyright 2025 Confluent Inc.
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

package io.confluent.connect.s3.backup;

import io.confluent.connect.s3.storage.S3OutputStream;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.s3.util.S3ErrorUtils;
import io.confluent.connect.storage.backup.StorageWriter;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * S3 implementation of {@link StorageWriter}.
 * Handles S3's commit-before-close semantics.
 * All writes are idempotent — no CAS needed.
 */
public class S3StorageWriter implements StorageWriter {

  private static final Logger log =
      LoggerFactory.getLogger(S3StorageWriter.class);

  private final S3Storage storage;

  public S3StorageWriter(S3Storage storage) {
    this.storage = storage;
  }

  @Override
  public void write(String path, String content) {
    if (content == null) {
      throw new ConnectException("Cannot write null content to path: " + path);
    }
    try {
      S3OutputStream s3out =
          (S3OutputStream) storage.create(path, storage.conf(), true);
      try {
        s3out.write(content.getBytes(StandardCharsets.UTF_8));
        s3out.commit();
      } finally {
        s3out.close();
      }
    } catch (IOException e) {
      S3ErrorUtils.throwConnectException(e);
    }
  }

  @Override
  public boolean exists(String path) {
    return storage.exists(path);
  }
}
