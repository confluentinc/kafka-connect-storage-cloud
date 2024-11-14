package io.confluent.connect.s3.format.parquet;

import io.confluent.connect.avro.AvroData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;

public class ParquetUtils {

  public static Collection<Object> getRecords(InputStream inputStream, String fileKey) throws IOException {
    File tempFile = File.createTempFile(fileKey, "");
    java.nio.file.Path filePath = tempFile.toPath();
    tempFile.deleteOnExit();
    FileOutputStream out = new FileOutputStream(tempFile);
    IOUtils.copy(inputStream, out);

    SeekableByteChannel sbc = Files.newByteChannel(filePath, StandardOpenOption.READ);
    S3ParquetInputFile parquetInputFile = new S3ParquetInputFile(sbc);
    ArrayList<Object> records = new ArrayList<>();
    try(ParquetReader<GenericRecord> reader = AvroParquetReader
            .<GenericRecord>builder(parquetInputFile)
            .withCompatibility(false)
            .build()) {
      GenericRecord record;
      record = reader.read();
      while (record != null) {
        records.add(record);
        record = reader.read();
      }
    }
    return records;
  }


  public static byte[] putRecords(Collection<SinkRecord> records, AvroData avroData) throws IOException {
    ParquetWriter<GenericRecord> writer = null;
    Path tempFilePath = new Path("temp");
    Schema schema = null;
    for (SinkRecord record : records) {
      if (schema == null) {
        schema = record.valueSchema();
        final int pageSize = 64 * 1024;
        org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(schema);
        writer = AvroParquetWriter
          .<GenericRecord>builder(tempFilePath)
          .withSchema(avroSchema)
          .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
          .withDictionaryEncoding(true)
          .withPageSize(pageSize)
          .build();
      }
      Object value = avroData.fromConnectData(schema, record.value());
      try {
        if (writer != null) {
          writer.write((GenericRecord) value);
        }
      } catch (IOException e) {
        throw new ConnectException(e);
      }
    }
    try {
      if (writer != null) {
        writer.close();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    FSDataInputStream fsDataInputStream = tempFilePath.getFileSystem(new Configuration()).open(tempFilePath);
    InputStream inputStream = fsDataInputStream.getWrappedStream();
    fsDataInputStream.getWrappedStream();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buff = new byte[8000];
    int bytesRead;
    while((bytesRead = inputStream.read(buff)) != -1) {
      out.write(buff, 0, bytesRead);
    }
    fsDataInputStream.close();
    tempFilePath.getFileSystem(new Configuration()).delete(tempFilePath, false);
    return out.toByteArray();
  }

  private static class S3ParquetInputFile implements InputFile {

    private SeekableByteChannel seekableByteChannel;

    S3ParquetInputFile(SeekableByteChannel seekableByteChannel) {
      this.seekableByteChannel = seekableByteChannel;
    }

    @Override
    public long getLength() throws IOException {
      return seekableByteChannel.size();
    }

    @Override
    public SeekableInputStream newStream() {
      return new DelegatingSeekableInputStream(Channels.newInputStream(seekableByteChannel)) {
        @Override
        public long getPos() throws IOException {
          return seekableByteChannel.position();
        }

        @Override
        public void seek(long newPos) throws IOException {
          seekableByteChannel.position(newPos);
        }
      };
    }
  }
}
