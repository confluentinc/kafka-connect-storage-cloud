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

package io.confluent.connect.s3.format.orc;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class OrcHepler {
  private static final Logger log = LoggerFactory.getLogger(OrcHepler.class);

  private final List<OrcField> orcFields;
  public final TypeDescription orcSchema;


  public OrcHepler(Schema schema) {
    orcFields = fromConnectSchema(schema);
    orcSchema = createSchema();
  }


  public void setValue(VectorizedRowBatch batch, Schema schema, Struct struct, int rowIndex) {
    List<Field> fields = schema.fields();
    for (Field field : fields) {
      Object value = struct.get(field);
      OrcField typeConverter = orcFields.get(field.index());
      if (typeConverter != null) {
        typeConverter.setData(batch, value, rowIndex);
      } else {
        throw new DataException("Record's schema are not same for all messages");
      }
    }
  }


  VectorizedRowBatch createBatch() {
    TypeDescription orcSchema = createSchema();
    return orcSchema.createRowBatch();
  }

  private TypeDescription createSchema() {
    if (orcFields.isEmpty()) {
      throw new IllegalArgumentException("No fields were specified");
    }
    StringBuilder schema = new StringBuilder("struct<");
    Iterator<OrcField> fieldIterator = orcFields.iterator();
    while (fieldIterator.hasNext()) {
      OrcField field = fieldIterator.next();
      schema.append(field.fieldDescription());
      if (fieldIterator.hasNext()) {
        schema.append(",");
      }
    }
    schema.append(">");
    return TypeDescription.fromString(schema.toString());
  }

  private static List<OrcField> fromConnectSchema(Schema connectSchema) {
    List<Field> connectFields = connectSchema.fields();
    List<OrcField> result = new ArrayList<>(connectFields.size());
    for (Field connectField : connectFields) {

      Schema.Type fieldType = connectField.schema().type();
      switch (fieldType) {
        case BOOLEAN:
          result.add(new BooleanOrcField(connectField.name(), connectField.index()));
          break;
        case INT8:
        case INT16:
        case INT32: {
          result.add(new IntegerOrcField(connectField.name(), connectField.index()));
          break;
        }
        case INT64: {
          //todo ORC handle date????
          result.add(new LongOrcField(connectField.name(), connectField.index()));
          break;
        }
        case BYTES:
        case STRING: {
          result.add(new ByteOrcField(connectField.name(), connectField.index()));
          break;
        }
        case FLOAT64:
          result.add(new DoubleOrcField(connectField.name(), connectField.index()));
          break;
        case FLOAT32: {
          result.add(new FloatOrcField(connectField.name(), connectField.index()));
          break;
        }
        //todo ORC map, list
        default:
          throw new DataException("Unsupported type: " + fieldType);
      }
    }
    Collections.sort(result, Comparator.comparing(OrcField::columnIndex));
    return result;
  }

  interface OrcField {
    void setData(VectorizedRowBatch rowBatch, Object data, int rowIndex);

    String fieldDescription();

    int columnIndex();
  }

  private static class LongOrcField implements OrcField {
    String name;
    int columnIndex;

    public LongOrcField(String name, int columnIndex) {
      this.name = name;
      this.columnIndex = columnIndex;
    }

    @Override
    public void setData(VectorizedRowBatch rowBatch, Object data, int rowIndex) {
      LongColumnVector col = (LongColumnVector) rowBatch.cols[columnIndex];
      col.vector[rowIndex] = (Long) data;
    }

    @Override
    public String fieldDescription() {
      return name + ":bigint";
    }

    @Override
    public int columnIndex() {
      return columnIndex;
    }
  }

  private static class IntegerOrcField implements OrcField {
    String name;
    int columnIndex;

    public IntegerOrcField(String name, int columnIndex) {
      this.name = name;
      this.columnIndex = columnIndex;
    }

    @Override
    public void setData(VectorizedRowBatch rowBatch, Object data, int rowIndex) {
      LongColumnVector col = (LongColumnVector) rowBatch.cols[columnIndex];
      col.vector[rowIndex] = ((Integer) data).longValue();
    }


    @Override
    public String fieldDescription() {
      return name + ":int";
    }

    @Override
    public int columnIndex() {
      return columnIndex;
    }
  }

  /**
   * handles timestamp values
   */
  private class TimestampOrcField implements OrcField {
    String name;
    int columnIndex;

    public TimestampOrcField(String name, int columnIndex) {
      this.name = name;
      this.columnIndex = columnIndex;
    }

    @Override
    public void setData(VectorizedRowBatch rowBatch, Object data, int rowIndex) {
      TimestampColumnVector col = (TimestampColumnVector) rowBatch.cols[columnIndex];
      col.set(rowIndex, new Timestamp((Long) data));
    }

    @Override
    public String fieldDescription() {
      return name + ":timestamp";
    }

    @Override
    public int columnIndex() {
      return columnIndex;
    }
  }

  /**
   * handles all of the floating point types (double, and float)
   */
  private static class DoubleOrcField implements OrcField {
    String name;
    int columnIndex;

    public DoubleOrcField(String name, int columnIndex) {
      this.name = name;
      this.columnIndex = columnIndex;
    }

    @Override
    public void setData(VectorizedRowBatch rowBatch, Object data, int rowIndex) {
      DoubleColumnVector col = (DoubleColumnVector) rowBatch.cols[columnIndex];
      col.vector[rowIndex] = (Double) data;
    }

    @Override
    public String fieldDescription() {
      return name + ":double";
    }

    @Override
    public int columnIndex() {
      return columnIndex;
    }
  }

  private static class FloatOrcField implements OrcField {
    String name;
    int columnIndex;

    public FloatOrcField(String name, int columnIndex) {
      this.name = name;
      this.columnIndex = columnIndex;
    }

    @Override
    public void setData(VectorizedRowBatch rowBatch, Object data, int rowIndex) {
      DoubleColumnVector col = (DoubleColumnVector) rowBatch.cols[columnIndex];
      col.vector[rowIndex] = Double.valueOf(data.toString());
    }

    @Override
    public String fieldDescription() {
      return name + ":float";
    }

    @Override
    public int columnIndex() {
      return columnIndex;
    }
  }

  /**
   * handles all of the binary types (binary, char, string, and varchar)
   */
  private static class ByteOrcField implements OrcField {
    String name;
    int columnIndex;

    public ByteOrcField(String name, int columnIndex) {
      this.name = name;
      this.columnIndex = columnIndex;
    }

    @Override
    public void setData(VectorizedRowBatch rowBatch, Object data, int rowIndex) {
      BytesColumnVector col = (BytesColumnVector) rowBatch.cols[columnIndex];
      col.setVal(rowIndex, ((String) data).getBytes());
    }

    @Override
    public String fieldDescription() {
      return name + ":string";
    }

    @Override
    public int columnIndex() {
      return columnIndex;
    }
  }

  private static class BooleanOrcField implements OrcField {
    String name;
    int columnIndex;

    public BooleanOrcField(String name, int columnIndex) {
      this.name = name;
      this.columnIndex = columnIndex;
    }

    @Override
    public void setData(VectorizedRowBatch rowBatch, Object data, int rowIndex) {
      LongColumnVector col = (LongColumnVector) rowBatch.cols[columnIndex];
      col.vector[rowIndex] = ((Boolean) data) ? 1L : 0L;
    }

    @Override
    public String fieldDescription() {
      return name + ":boolean";
    }

    @Override
    public int columnIndex() {
      return columnIndex;
    }
  }

}
