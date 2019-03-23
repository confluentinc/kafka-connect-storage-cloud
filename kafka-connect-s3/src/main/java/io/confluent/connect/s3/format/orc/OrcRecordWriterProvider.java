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

import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static io.confluent.connect.s3.S3SinkConnectorConfig.ORC_CODEC_CONFIG;

public class OrcRecordWriterProvider implements RecordWriterProvider<S3SinkConnectorConfig> {

  private static final Logger log = LoggerFactory.getLogger(OrcRecordWriterProvider.class);
  private static final String EXTENSION = ".orc";
  private final S3Storage storage;

  OrcRecordWriterProvider(S3Storage storage) {
    this.storage = storage;
  }

  @Override
  public String getExtension() {
    return EXTENSION;
  }

  @Override
  public RecordWriter getRecordWriter(final S3SinkConnectorConfig conf, final String filename) {
    // This is not meant to be a thread-safe writer!
    return new RecordWriter() {
      Path pathToFile = new Path(filename);
      Writer writer;
      S3OutputStreamWrapper s3OutputStream;
      OrcHepler orcHelper;
      VectorizedRowBatch batch;
      boolean closed;


      @Override
      public void write(SinkRecord record) {
        if (orcHelper == null) {
          Schema schema = record.valueSchema();
          try {
            orcHelper = new OrcHepler(schema);
            log.info("Opening record writer for: {}", filename);
            OrcFile.WriterOptions writerOptions = getWriterOptions(orcHelper.orcSchema, storage);
            s3OutputStream = new S3OutputStreamWrapper(filename, storage.conf(), storage.getS3());
            writerOptions.fileSystem(new S3OrcFileSystem(s3OutputStream));
            writer = OrcFile.createWriter(pathToFile, writerOptions);
            batch = orcHelper.createBatch();
          } catch (IOException e) {
            throw new ConnectException(e);
          }
        }
        log.trace("Sink record: {}", record);
        try {
          orcHelper.setValue(batch, record.valueSchema(), (Struct) record.value(), batch.size++);
          if (batch.size == batch.getMaxSize()) {
            writer.addRowBatch(batch);
            batch.reset();
          }

        } catch (IOException e) {
          throw new ConnectException(e);
        }
      }

      @Override
      public void commit() {
        try {
          if (batch.size != 0) {
            writer.addRowBatch(batch);
          }
          s3OutputStream.comiitBeforeClose();
          writer.close();
          closed = true;
        } catch (IOException e) {
          throw new ConnectException(e);
        }
      }

      @Override
      public void close() {
        try {
          if (!closed) {
            writer.close();
          }
        } catch (IOException e) {
          throw new ConnectException(e);
        }
      }
    };
  }

  static OrcFile.WriterOptions getWriterOptions(TypeDescription schema, S3Storage s3Storage) {
    Configuration configuration = new Configuration();
    OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(configuration)
        .setSchema(schema);
    S3SinkConnectorConfig conf = s3Storage.conf();
    String compressionType = conf.getString(ORC_CODEC_CONFIG);
    if (compressionType != null) {
      writerOptions.compress(CompressionKind.valueOf(compressionType));
    }

    return writerOptions;

  }

}
