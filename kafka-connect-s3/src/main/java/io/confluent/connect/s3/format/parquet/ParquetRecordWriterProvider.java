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


package io.confluent.connect.s3.format.parquet;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.storage.S3ParquetOutputStream;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ParquetRecordWriterProvider implements RecordWriterProvider<S3SinkConnectorConfig> {
  private static final Logger log = LoggerFactory.getLogger(ParquetRecordWriterProvider.class);
  private static final String EXTENSION = ".parquet";
  private static final String COMPRESSION_TYPE = ".snappy";
  private final S3Storage storage;
  private final AvroData avroData;
  private S3ParquetOutputFile s3ParquetOutputFile;

  ParquetRecordWriterProvider(S3Storage storage, AvroData avroData) {
    this.storage = storage;
    this.avroData = avroData;
  }

  @Override
  public String getExtension() {
    return COMPRESSION_TYPE + EXTENSION;
  }

  @Override
  public RecordWriter getRecordWriter(final S3SinkConnectorConfig conf, final String filename) {
    return new RecordWriter() {
      Schema schema = null;
      Boolean committed = false;
      ParquetWriter<GenericRecord> writer;

      @Override
      public void write(SinkRecord record) {
        if (schema == null) {
          schema = record.valueSchema();
          try {
            log.info("Opening record writer for: {}", filename);
            s3ParquetOutputFile = new S3ParquetOutputFile(storage, filename);
            final int pageSize = 64 * 1024;
            org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(schema);

            writer = AvroParquetWriter
                    .<GenericRecord>builder(s3ParquetOutputFile)
                    .withSchema(avroSchema)
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                    .withDictionaryEncoding(true)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withPageSize(pageSize)
                    .build();
          } catch (IOException e) {
            throw new ConnectException(e);
          }
        }
        log.trace("Sink record: {}", record.toString());
        Object value = avroData.fromConnectData(schema, record.value());
        try {
          writer.write((GenericRecord) value);
        } catch (IOException e) {
          throw new ConnectException(e);
        }
      }

      @Override
      public void close() {
        if (committed) {
          return;
        }
        commit();
      }

      @Override
      public void commit() {
        try {
          committed = true;
          if (writer != null) {
            writer.close();
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    };
  }

  private static class S3ParquetOutputFile implements OutputFile {
    private S3Storage storage;
    private String filename;
    private S3ParquetOutputStream s3out;

    S3ParquetOutputFile(S3Storage storage, String filename) {
      this.storage = storage;
      this.filename = filename;
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) {
      s3out = new S3ParquetOutputStream(storage.create(filename, true));
      return s3out;
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) {
      return create(blockSizeHint);
    }

    @Override
    public boolean supportsBlockSize() {
      return false;
    }

    @Override
    public long defaultBlockSize() {
      return 0;
    }
  }
}
