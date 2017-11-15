package io.confluent.connect.s3.format.parquet;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.format.avro.AvroRecordWriterProvider;
import io.confluent.connect.s3.storage.S3OutputStream;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ParquetRecordWriterProvider implements RecordWriterProvider<S3SinkConnectorConfig> {

  private static final Logger log = LoggerFactory.getLogger(AvroRecordWriterProvider.class);
  private static final String EXTENSION = ".parquet";
  private final S3Storage storage;
  private final AvroData avroData;

  ParquetRecordWriterProvider(S3Storage storage, AvroData avroData) {
    this.storage = storage;
    this.avroData = avroData;
  }

  @Override
  public String getExtension() {
    return EXTENSION;
  }

  @Override
  public RecordWriter getRecordWriter(final S3SinkConnectorConfig conf, final String filename) {
//    String awsKey = System.getenv("AWS_ACCESS_KEY_ID");
//    String awsSecret = System.getenv("AWS_SECRET_ACCESS_KEY");
    AWSCredentials awsCredentials = DefaultAWSCredentialsProviderChain.getInstance().getCredentials();
    Configuration hadoopConf = new Configuration();
    hadoopConf.set("fs.s3a.access.key", awsCredentials.getAWSAccessKeyId());
    hadoopConf.set("fs.s3a.secret.key", awsCredentials.getAWSSecretKey());

    // This is not meant to be a thread-safe writer!
    return new RecordWriter() {
      int blockSize = 256 * 1024 * 1024;
      int pageSize = 64 * 1024;
      CompressionCodecName compressionCodecName = CompressionCodecName.SNAPPY;
      Schema schema = null;
//      S3OutputStream s3out;
      org.apache.avro.Schema avroSchema;
      String s3Path = "s3a://sailthru-ds-data/" + filename;
      Path path = new Path(s3Path);
      ParquetWriter<GenericRecord> parquetWriter = null;

      @Override
      public void write(SinkRecord record) {
        if (schema == null) {
          schema = record.valueSchema();
          log.info("Opening record writer for: {}", filename);
//          s3out = storage.create(filename, true);
          avroSchema = avroData.fromConnectSchema(schema);
          try {
            if(parquetWriter == null){
              parquetWriter = new AvroParquetWriter<>(path, avroSchema, compressionCodecName, blockSize, pageSize, true);
            }
          } catch (IOException e) {
            throw new ConnectException(e);
          }
        }
        log.trace("Sink record: {}", record);
        Object value = avroData.fromConnectData(schema, record.value());
        try {
          parquetWriter.write((GenericRecord) value);
        } catch (IOException e) {
          throw new ConnectException(e);
        }
      }

      @Override
      public void commit() {
        // Flush is required here, because closing the writer will close the underlying S3 output stream before
        // committing any data to S3.
//          s3out.commit();
        log.error("COMMITTING I GUESS?");
      }

      @Override
      public void close() {
        try {
          parquetWriter.close();
        } catch (IOException e) {
          throw new ConnectException(e);
        }
      }
    };
  }

}
