package io.confluent.connect.s3.storage;

import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;

/**
 * Interface which duplicates RecordWriter, yet with
 * IOException throwing signatures.
 */
public interface IORecordWriter {
  void write(SinkRecord sinkRecord) throws IOException;

  void close() throws IOException;

  void commit() throws IOException;
}
