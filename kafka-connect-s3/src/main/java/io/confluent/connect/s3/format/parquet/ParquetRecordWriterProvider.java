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

import static io.confluent.connect.s3.util.Utils.getAdjustedFilename;
import static io.confluent.connect.s3.util.Utils.sinkRecordToLoggableString;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.storage.IORecordWriter;
import io.confluent.connect.s3.format.RecordViewSetter;
import io.confluent.connect.s3.format.S3RetriableRecordWriter;
import io.confluent.connect.s3.storage.S3ParquetOutputStream;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class ParquetRecordWriterProvider extends RecordViewSetter
    implements RecordWriterProvider<S3SinkConnectorConfig> {
  private static final Logger log = LoggerFactory.getLogger(ParquetRecordWriterProvider.class);
  private static final String EXTENSION = ".parquet";
  private static final int PAGE_SIZE = 64 * 1024;
  private final S3Storage storage;
  private final AvroData avroData;

  ParquetRecordWriterProvider(S3Storage storage, AvroData avroData) {
    this.storage = storage;
    this.avroData = avroData;
  }

  @Override
  public String getExtension() {
    return storage.conf().parquetCompressionCodecName().getExtension() + EXTENSION;
  }

  @Override
  public RecordWriter getRecordWriter(final S3SinkConnectorConfig conf, final String filename) {
    return new S3RetriableRecordWriter(
        new IORecordWriter() {
          final String adjustedFilename = getAdjustedFilename(recordView, filename, getExtension());
          Schema schema = null;
          ParquetWriter<GenericRecord> writer;
          S3ParquetOutputFile s3ParquetOutputFile;

          @Override
          public void write(SinkRecord record) throws IOException {
            if (schema == null || writer == null) {
              schema = recordView.getViewSchema(record, true);
              log.info("Opening record writer for: {}", adjustedFilename);
              org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(schema);
              s3ParquetOutputFile = new S3ParquetOutputFile(storage, adjustedFilename);
              AvroParquetWriter.Builder<GenericRecord> builder =
                  AvroParquetWriter.<GenericRecord>builder(s3ParquetOutputFile)
                      .withSchema(avroSchema)
                      .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                      .withDictionaryEncoding(true)
                      .withCompressionCodec(storage.conf().parquetCompressionCodecName())
                      .withPageSize(PAGE_SIZE);
              if (schemaHasArrayOfOptionalItems(schema, /*seenSchemas=*/null)) {
                // If the schema contains an array of optional items, then
                // it is possible that the array may have null items during the
                // writing process.  In this case, we set a flag so as not to
                // incur a NullPointerException
                log.debug(
                    "Setting \"" + AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE
                        + "\" to false because the schema contains an array "
                        + "with optional items"
                );
                builder.config(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, "false");
              }
              writer = builder.build();
            }
            log.trace("Sink record with view {}: {}", recordView,
                sinkRecordToLoggableString(record));
            Object value = avroData.fromConnectData(schema, recordView.getView(record, true));
            writer.write((GenericRecord) value);
          }

          @Override
          public void close() throws IOException {
            if (writer != null) {
              writer.close();
            }
          }

          @Override
          public void commit() throws IOException {
            s3ParquetOutputFile.s3out.setCommit();
            if (writer != null) {
              writer.close();
            }
          }
        }
    );
  }

  /**
   * Check if any schema (or nested schema) is an array of optional items
   * @param schema The shema to check
   * @return 'true' if the schema contains an array with optional items.
   */
  /* VisibleForTesting */
  public static boolean schemaHasArrayOfOptionalItems(Schema schema, Set<Schema> seenSchemas) {
    // First, check for infinitely recursing schemas
    if (seenSchemas == null) {
      seenSchemas = new HashSet<>();
    } else if (seenSchemas.contains(schema)) {
      return false;
    }
    seenSchemas.add(schema);
    switch (schema.type()) {
      case STRUCT:
        for (Field field : schema.fields()) {
          if (schemaHasArrayOfOptionalItems(field.schema(), seenSchemas)) {
            return true;
          }
        }
        return false;
      case MAP:
        return schemaHasArrayOfOptionalItems(schema.valueSchema(), seenSchemas);
      case ARRAY:
        return schema.valueSchema().isOptional()
            || schemaHasArrayOfOptionalItems(schema.valueSchema(), seenSchemas);
      default:
        return false;
    }
  }

  private static class S3ParquetOutputFile implements OutputFile {
    private static final int DEFAULT_BLOCK_SIZE = 0;
    private S3Storage storage;
    private String filename;
    private S3ParquetOutputStream s3out;

    S3ParquetOutputFile(S3Storage storage, String filename) {
      this.storage = storage;
      this.filename = filename;
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) {
      s3out = (S3ParquetOutputStream) storage.create(filename, true, ParquetFormat.class);
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
      return DEFAULT_BLOCK_SIZE;
    }
  }
}
