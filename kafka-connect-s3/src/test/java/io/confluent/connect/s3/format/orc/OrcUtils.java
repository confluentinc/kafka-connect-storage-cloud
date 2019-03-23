package io.confluent.connect.s3.format.orc;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;

public class OrcUtils {

  public static Collection<Object> getRecords(String filePath) {
    Reader reader = null;
    Collection<Object> result = new ArrayList<>();
    try {
      reader = OrcFile.createReader(new org.apache.hadoop.fs.Path(filePath), OrcFile.readerOptions(new Configuration()));
      try (RecordReader rows = reader.rows()) {
        TypeDescription schema = reader.getSchema();
        VectorizedRowBatch batch = schema.createRowBatch();
        int numCols = batch.numCols;
        List<TypeDescription> children = schema.getChildren();
        while (rows.nextBatch(batch)) {
          Object[] storedData = new Object[numCols];
          for (int row = 0; row < batch.size; row++) {
            for (int j = 0; j < numCols; j++) {
              ColumnVector col = batch.cols[j];
              TypeDescription fieldDescription = children.get(j);
              TypeDescription.Category fieldType = fieldDescription.getCategory();
              switch (fieldType) {
                case INT:
                  storedData[j] = (int) ((LongColumnVector) col).vector[row];
                  break;
                case LONG:
                  storedData[j] = ((LongColumnVector) col).vector[row];
                  break;
                case DOUBLE:
                  storedData[j] = ((DoubleColumnVector) col).vector[row];
                  break;
                case FLOAT:
                  storedData[j] = ((Double) ((DoubleColumnVector) col).vector[row]).floatValue();
                  break;
                case BOOLEAN:
                  storedData[j] = ((LongColumnVector) col).vector[row] == 1;
                  break;
                case STRING:
                  storedData[j] = ((BytesColumnVector) col).toString(row);
                  break;
                case TIMESTAMP:
                  storedData[j] = ((TimestampColumnVector) col).getTime(row);
                  break;
                default:
                  throw new IllegalArgumentException("Not supported type: " + fieldType);
              }
            }
            result.add(storedData);
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Issue on reading orc file", e);
    }
    return result;
  }

  public static void assertRecordMatches(SinkRecord sinkRecord, Object actual) {
    Struct value = (Struct) sinkRecord.value();
    Object[] expectedValues = {value.get("boolean"), value.get("int"), value.get("long"), value.get("float"), value.get("double")};
    assertArrayEquals(expectedValues, (Object[]) actual);
  }

  public static byte[] putRecords(Collection<SinkRecord> records) throws IOException {
    if (records.isEmpty()) {
      return new byte[0];
    } else {
      SinkRecord next = records.iterator().next();
      OrcHepler orcHepler = new OrcHepler(next.valueSchema());
      java.nio.file.Path tmpFile = Paths.get(System.getProperty("java.io.tmpdir"), "orc", "" + System.currentTimeMillis() + ".orc");

      try {
        VectorizedRowBatch rowBatch = orcHepler.orcSchema.createRowBatch();
        Writer writer = createWriter(orcHepler.orcSchema, tmpFile.toString());

        for (SinkRecord record : records) {
          orcHepler.setValue(rowBatch, record.valueSchema(), (Struct) record.value(), rowBatch.size++);
        }
        writer.addRowBatch(rowBatch);
        writer.close();
        return Files.readAllBytes(tmpFile);
      } finally {
        FileUtils.deleteQuietly(tmpFile.toFile());
      }
    }
  }

  static Writer createWriter(TypeDescription schema, String filePath) throws IOException {
    OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(new Configuration()).setSchema(schema);
    if (System.getProperty("os.name").startsWith("Windows")) {
      WindowsTestFileSystem fs = new WindowsTestFileSystem(new Configuration());
      writerOptions.fileSystem(fs);
    }
    return OrcFile.createWriter(new Path(filePath), writerOptions);
  }


  private static class WindowsTestFileSystem extends LocalFileSystem {

    private RawLocalFileSystem raw = new RawLocalFileSystem(){
      @Override
      public void setPermission(Path p, FsPermission permission) throws IOException {
        //do nothing
      }
    };

    WindowsTestFileSystem(Configuration conf) {
      super();
      fs.setConf(conf);
      setConf(conf);
    }

    @Override
    public void setPermission(Path p, FsPermission permission) throws IOException {
      //do nothing
    }

    @Override
    public FileSystem getRawFileSystem() {
      return raw;
    }
  }


}
