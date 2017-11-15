package io.confluent.connect.s3.format.parquet;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.format.avro.AvroRecordWriterProvider;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.format.SchemaFileReader;
import io.confluent.connect.storage.hive.HiveFactory;

public class ParquetFormat implements Format<S3SinkConnectorConfig, String> {
  private final S3Storage storage;
  private final AvroData avroData;

  public ParquetFormat(S3Storage storage) {
    this.storage = storage;
    this.avroData = new AvroData(storage.conf().getInt(S3SinkConnectorConfig.SCHEMA_CACHE_SIZE_CONFIG));
  }

  @Override
  public RecordWriterProvider<S3SinkConnectorConfig> getRecordWriterProvider() {
    return new ParquetRecordWriterProvider(storage, avroData);
  }

  @Override
  public SchemaFileReader<S3SinkConnectorConfig, String> getSchemaFileReader() {
    throw new UnsupportedOperationException("Reading schemas from S3 is not currently supported");
  }

  @Override
  public HiveFactory getHiveFactory() {
    throw new UnsupportedOperationException("Hive integration is not currently supported in S3 Connector");
  }

  public AvroData getAvroData() {
    return avroData;
  }
}
