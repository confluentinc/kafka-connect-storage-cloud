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

package io.confluent.connect.azure.format.json;

import static io.confluent.connect.azure.util.S3ErrorUtils.throwConnectException;
import static io.confluent.connect.azure.util.Utils.getAdjustedFilename;
import static io.confluent.connect.azure.util.Utils.sinkRecordToLoggableString;

import com.azure.storage.blob.specialized.BlobOutputStream;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.connect.azure.storage.AzBlobStorage;
import io.confluent.connect.azure.storage.IORecordWriter;
import io.confluent.connect.azure.format.RecordViews.HeaderRecordView;
import io.confluent.connect.azure.format.S3RetriableRecordWriter;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPOutputStream;

import io.confluent.connect.azure.AzBlobSinkConnectorConfig;
import io.confluent.connect.azure.format.RecordViewSetter;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;

public class JsonRecordWriterProvider extends RecordViewSetter
    implements RecordWriterProvider<AzBlobSinkConnectorConfig> {

  private static final Logger log = LoggerFactory.getLogger(JsonRecordWriterProvider.class);
  private static final String EXTENSION = ".json";
  private static final String LINE_SEPARATOR = System.lineSeparator();
  private static final byte[] LINE_SEPARATOR_BYTES
      = LINE_SEPARATOR.getBytes(StandardCharsets.UTF_8);
  private final AzBlobStorage storage;
  private final ObjectMapper mapper;
  private final JsonConverter converter;

  JsonRecordWriterProvider(AzBlobStorage storage, JsonConverter converter) {
    this.storage = storage;
    this.mapper = new ObjectMapper();
    this.converter = converter;
  }

  @Override
  public String getExtension() {
    return EXTENSION + storage.conf().getCompressionType().extension;
  }

  @Override
  public RecordWriter getRecordWriter(final AzBlobSinkConnectorConfig conf, final String filename) {
    try {
      return new S3RetriableRecordWriter(
          new IORecordWriter() {
            final String adjustedFilename = getAdjustedFilename(recordView, filename,
                getExtension());
            final BlobOutputStream blobOut = storage.create(adjustedFilename, true);
            final OutputStream blobOutWrapper = new GZIPOutputStream(blobOut, 8 * 1024) {
                public OutputStream setLevel(int level) {
                    def.setLevel(level);
                    return this;
                }
            }.setLevel(9);

            final JsonGenerator writer = mapper.getFactory()
                .createGenerator(blobOutWrapper)
                .setRootValueSeparator(null);

            @Override
            public void write(SinkRecord record) throws IOException {
              log.trace("Sink record with view {}: {}", recordView,
                  sinkRecordToLoggableString(record));
              // headers need to be enveloped for json format
              boolean envelop = recordView instanceof HeaderRecordView;
              Object value = recordView.getView(record, envelop);
              if (value instanceof Struct) {
                byte[] rawJson = converter.fromConnectData(
                    record.topic(),
                    recordView.getViewSchema(record, envelop),
                    value
                );
                blobOutWrapper.write(rawJson);
                blobOutWrapper.write(LINE_SEPARATOR_BYTES);
              } else {
                writer.writeObject(value);
                writer.writeRaw(LINE_SEPARATOR);
              }
            }

            @Override
            public void commit() throws IOException {
              // Flush is required here, because closing the writer will close the underlying S3
              // output stream before committing any data to S3.
              writer.flush();
              writer.close();
            }

            @Override
            public void close() throws IOException {
              writer.close();
            }
          }
      );
    } catch (IOException e) {
      throwConnectException(e);
      // compiler can't see that the above method is always throwing an exception,
      // so had to add this useless return statement
      return null;
    }
  }
}
