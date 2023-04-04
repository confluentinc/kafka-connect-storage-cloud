/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.connect.s3.format;

import io.confluent.connect.s3.storage.IORecordWriter;
import io.confluent.connect.storage.format.RecordWriter;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.confluent.connect.s3.util.S3ErrorUtils.throwConnectException;

/**
 * Wrapper class which may convert an IOException to either a ConnectException
 * or a RetriableException depending upon whether the exception is "retriable"
 * as determined within `throwConnectException()`.
 */
public class S3RetriableRecordWriter implements RecordWriter {

  private static final Logger log = LoggerFactory.getLogger(S3RetriableRecordWriter.class);
  private final IORecordWriter writer;

  public S3RetriableRecordWriter(IORecordWriter writer) {
    if (writer == null) {
      log.debug("S3 Retriable record writer was passed a null writer (IORecordWriter)");
      throw new NullPointerException(
        "S3 Retriable record writer was passed a null writer (IORecordWriter)"
      );
    }
    this.writer = writer;
  }

  @Override
  public void write(SinkRecord sinkRecord) {
    try {
      writer.write(sinkRecord);
    } catch (IOException e) {
      throwConnectException(e);
    }
  }

  @Override
  public void commit() {
    try {
      writer.commit();
    } catch (IOException e) {
      throwConnectException(e);
    }
  }

  @Override
  public void close() {
    try {
      writer.close();
    } catch (IOException e) {
      throwConnectException(e);
    }
  }
}
