package io.confluent.connect.s3.format.orc;

import io.confluent.connect.s3.format.orc.schema.OrcSchemaHelper;
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
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
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
        List<TypeDescription> children = schema.getChildren();
        while (rows.nextBatch(batch)) {
          int numCols = batch.numCols;
          for (int row = 0; row < batch.size; row++) {
            Object[] storedData = new Object[numCols];
            for (int j = 0; j < numCols; j++) {
              ColumnVector col = batch.cols[j];
              TypeDescription fieldDescription = children.get(j);
              storedData[j] = parseField(col, fieldDescription, row);
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


  private static Object parseField(ColumnVector col, TypeDescription fieldDescription, int row) {
    TypeDescription.Category fieldType = fieldDescription.getCategory();
    if (col.isNull[row]) {
      return null;
    } else {
      switch (fieldType) {
        case INT:
          return (int) ((LongColumnVector) col).vector[row];
        case LONG:
          return ((LongColumnVector) col).vector[row];
        case DOUBLE:
          return ((DoubleColumnVector) col).vector[row];
        case FLOAT:
          return ((Double) ((DoubleColumnVector) col).vector[row]).floatValue();
        case BOOLEAN:
          return ((LongColumnVector) col).vector[row] == 1;
        case STRING:
          return ((BytesColumnVector) col).toString(row);
        case TIMESTAMP:
          return ((TimestampColumnVector) col).getTime(row);
        case STRUCT:
          List<TypeDescription> children = fieldDescription.getChildren();
          ColumnVector[] data = ((StructColumnVector) col).fields;
          Object[] parsedData = new Object[data.length];
          for (int i = 0; i < children.size(); i++) {
            parsedData[i] = parseField(data[i], children.get(i), row);
          }
          return parsedData;
        default:
          throw new IllegalArgumentException("Not supported type: " + fieldType);
      }
    }
  }


  public static void assertRecordMatches(SinkRecord sinkRecord, Object actual) {
    Struct value = (Struct) sinkRecord.value();
    Schema schema = sinkRecord.valueSchema();
    List<Field> fields = schema.fields();
    Object[] expectedData = parseConnectData(fields, value);
    assertArrayEquals(expectedData, (Object[]) actual);
  }

  private static Object[] parseConnectData(List<Field> fields, Struct value) {
    Object[] data = new Object[fields.size()];
    if (value == null) {
      return data;
    }
    for (int i = 0; i < fields.size(); i++) {
      Field field = fields.get(i);
      if (field.schema().type() == Schema.Type.STRUCT) {
        data[i] = parseConnectData(field.schema().fields(), value.getStruct(field.name()));
      } else {
        data[i] = value.get(field);
      }
    }
    return data;
  }


  public static byte[] putRecords(Collection<SinkRecord> records) throws IOException {
    if (records.isEmpty()) {
      return new byte[0];
    } else {
      SinkRecord next = records.iterator().next();
      OrcSchemaHelper orcSchemaHelper = new OrcSchemaHelper(next.valueSchema());
      java.nio.file.Path tmpFile = Paths.get(System.getProperty("java.io.tmpdir"), "orc", "" + System.currentTimeMillis() + ".orc");

      try {
        VectorizedRowBatch rowBatch = orcSchemaHelper.createBatch();
        Writer writer = createWriter(orcSchemaHelper.getOrcSchema(), tmpFile.toString());

        for (SinkRecord record : records) {
          orcSchemaHelper.setData(rowBatch, record.value());
        }
        writer.addRowBatch(rowBatch);
        writer.close();
        return Files.readAllBytes(tmpFile);
      } finally {
        FileUtils.deleteQuietly(tmpFile.toFile());
      }
    }
  }

  private static Writer createWriter(TypeDescription schema, String filePath) throws IOException {
    OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(new Configuration()).setSchema(schema);
    if (System.getProperty("os.name").startsWith("Windows")) {
      WindowsTestFileSystem fs = new WindowsTestFileSystem(new Configuration());
      writerOptions.fileSystem(fs);
    }
    return OrcFile.createWriter(new Path(filePath), writerOptions);
  }


  private static class WindowsTestFileSystem extends LocalFileSystem {

    private RawLocalFileSystem raw = new RawLocalFileSystem() {
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
