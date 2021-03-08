package io.confluent.connect.s3.format.csv;

import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.format.RecordViewSetter;
import io.confluent.connect.s3.format.json.JsonRecordWriterProvider;
import io.confluent.connect.s3.storage.S3OutputStream;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.confluent.connect.s3.util.Utils.getAdjustedFilename;

public class CSVRecordWriterProvider extends RecordViewSetter
        implements RecordWriterProvider<S3SinkConnectorConfig> {

    private static final Logger log = LoggerFactory.getLogger(JsonRecordWriterProvider.class);
    private static final String EXTENSION = ".csv";
    private static final String LINE_SEPARATOR = System.lineSeparator();
    private static final byte[] LINE_SEPARATOR_BYTES
            = LINE_SEPARATOR.getBytes(StandardCharsets.UTF_8);
    private final S3Storage storage;
    private final CSVConverter converter;


    public CSVRecordWriterProvider(S3Storage storage, CSVConverter converter) {
        this.storage = storage;
        this.converter = converter;
    }

    @Override
    public RecordWriter getRecordWriter(S3SinkConnectorConfig s3SinkConnectorConfig, String filename) {
        return new RecordWriter() {
            final String adjustedFilename = getAdjustedFilename(recordView, filename, getExtension());
            final S3OutputStream s3out = storage.create(adjustedFilename, true, CSVFormat.class);
            final OutputStream s3outWrapper = s3out.wrapForCompression();
            final AtomicBoolean headerWritten = new AtomicBoolean(false);

            @Override
            public void write(SinkRecord record) {
                log.trace("Sink record with view {}: {}", recordView, record);
                try {
                    Object value = recordView.getView(record, false);
                    // only Struct is supported. Anything else ignored
                    if (value instanceof Struct) {
                        byte[] csvLine = converter.fromConnectData(
                                record.topic(),
                                recordView.getViewSchema(record, false),
                                value
                        );
                        if (!headerWritten.getAndSet(true)) {
                            // TODO: make this configurable
                            s3outWrapper.write(converter.getHeader());
                            s3outWrapper.write(LINE_SEPARATOR_BYTES);
                        }
                        s3outWrapper.write(csvLine);
                        s3outWrapper.write(LINE_SEPARATOR_BYTES);
                    }
                } catch (IOException e) {
                    throw new ConnectException(e);
                }
            }

            @Override
            public void commit() {
                try {
                    // Flush is required here, because closing the writer will close the underlying S3
                    // output stream before committing any data to S3.
                    s3out.commit();
                    s3outWrapper.close();
                } catch (IOException e) {
                    throw new RetriableException(e);
                }
            }

            @Override
            public void close() {
                try {
                    s3outWrapper.close();
                } catch (IOException e) {
                    throw new ConnectException(e);
                }
            }
        };
    }

    @Override
    public String getExtension() {
        return EXTENSION + storage.conf().getCompressionType().extension;
    }
}
