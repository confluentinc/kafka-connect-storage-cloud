package io.confluent.connect.s3.format.csv;

import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.format.SchemaFileReader;

import java.util.HashMap;
import java.util.Map;

public class CSVFormat implements Format<S3SinkConnectorConfig, String> {

    private final S3Storage storage;
    private final CSVConverter converter;

    public CSVFormat(S3Storage storage) {
        this.storage = storage;
        this.converter = new CSVConverter();
        Map<String, Object> converterConfig = new HashMap<>();
        this.converter.configure(converterConfig, false);
    }

    @Override
    public RecordWriterProvider<S3SinkConnectorConfig> getRecordWriterProvider() {
        return new CSVRecordWriterProvider(storage, converter);
    }

    @Override
    public SchemaFileReader<S3SinkConnectorConfig, String> getSchemaFileReader() {
        throw new UnsupportedOperationException("Reading schemas from S3 is not currently supported");
    }

    @Override
    @Deprecated
    public Object getHiveFactory() {
        throw new UnsupportedOperationException(
                "Hive integration is not currently supported in S3 Connector"
        );
    }
}
