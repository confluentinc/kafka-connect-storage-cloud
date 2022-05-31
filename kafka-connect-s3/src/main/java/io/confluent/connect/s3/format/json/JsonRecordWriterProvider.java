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

package io.confluent.connect.s3.format.json;

import static io.confluent.connect.s3.util.S3ErrorUtils.throwConnectException;
import static io.confluent.connect.s3.util.Utils.getAdjustedFilename;
import static io.confluent.connect.s3.util.Utils.sinkRecordToLoggableString;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.connect.s3.storage.IORecordWriter;
import io.confluent.connect.s3.format.RecordViews.HeaderRecordView;
import io.confluent.connect.s3.format.S3RetriableRecordWriter;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.format.RecordViewSetter;
import io.confluent.connect.s3.storage.S3OutputStream;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;

public class JsonRecordWriterProvider extends RecordViewSetter
    implements RecordWriterProvider<S3SinkConnectorConfig> {

  private static final Logger log = LoggerFactory.getLogger(JsonRecordWriterProvider.class);
  private static final String EXTENSION = ".json";
  private static final String LINE_SEPARATOR = System.lineSeparator();
  private static final byte[] LINE_SEPARATOR_BYTES
      = LINE_SEPARATOR.getBytes(StandardCharsets.UTF_8);
  private final S3Storage storage;
  private final ObjectMapper mapper;
  private final JsonConverter converter;

  JsonRecordWriterProvider(S3Storage storage, JsonConverter converter) {
    this.storage = storage;
    this.mapper = new ObjectMapper();
    this.converter = converter;
  }

  @Override
  public String getExtension() {
    return EXTENSION + storage.conf().getCompressionType().extension;
  }

  @Override
  public RecordWriter getRecordWriter(final S3SinkConnectorConfig conf, final String filename) {
    try {
      return new S3RetriableRecordWriter(
          new IORecordWriter() {
            final String adjustedFilename = getAdjustedFilename(recordView, filename,
                getExtension());
            final S3OutputStream s3out = storage.create(adjustedFilename, true, JsonFormat.class);
            final OutputStream s3outWrapper = s3out.wrapForCompression();
            final JsonGenerator writer = mapper.getFactory()
                .createGenerator(s3outWrapper)
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
                s3outWrapper.write(rawJson);
                s3outWrapper.write(LINE_SEPARATOR_BYTES);
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
              s3out.commit();
              s3outWrapper.close();
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
