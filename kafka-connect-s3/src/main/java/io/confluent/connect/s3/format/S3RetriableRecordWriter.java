package io.confluent.connect.s3.format;

import io.confluent.connect.s3.storage.IORecordWriter;
import io.confluent.connect.storage.format.RecordWriter;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;

import static io.confluent.connect.s3.util.S3ErrorUtils.throwConnectException;

/**
 * Wrapper class which may convert an IOException ito either a ConnectException
 * or a RetriableException depending upon whether the exception is "retriable"
 * as determined within `throwConnectException()`.
 */
public class S3RetriableRecordWriter implements RecordWriter {
  private IORecordWriter writer;

  public S3RetriableRecordWriter(IORecordWriter writer) {
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
