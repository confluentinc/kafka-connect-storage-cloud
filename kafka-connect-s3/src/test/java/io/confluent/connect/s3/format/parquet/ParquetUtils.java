package io.confluent.connect.s3.format.parquet;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
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
