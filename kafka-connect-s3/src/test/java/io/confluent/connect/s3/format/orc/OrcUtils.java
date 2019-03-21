package io.confluent.connect.s3.format.orc;

import org.apache.hadoop.conf.Configuration;
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

}
