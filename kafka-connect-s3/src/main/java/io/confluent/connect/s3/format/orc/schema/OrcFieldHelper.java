package io.confluent.connect.s3.format.orc.schema;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

public class OrcFieldHelper {

  interface OrcFieldConverter {

    void setNotNullData(ColumnVector column, Object data, int rowIndex);

    default void setData(ColumnVector column, Object data, int rowIndex) {
      if (data == null) {
        setNull(column, rowIndex);
      } else {
        setNotNullData(column, data, rowIndex);
      }
    }

    default void setNull(ColumnVector column, int rowIndex) {
      column.isNull[rowIndex] = true;
      column.noNulls = false;
    }
  }

  private static final Map<ColumnVector.Type, OrcFieldConverter> converters = new HashMap<>();

  static {
    converters.put(ColumnVector.Type.LONG, (column, data, rowIndex) ->
        ((LongColumnVector) column).vector[rowIndex] = (Long) data);

    converters.put(ColumnVector.Type.BYTES, (column, data, rowIndex) ->
        ((BytesColumnVector) column).setVal(rowIndex, (byte[]) data));

    converters.put(ColumnVector.Type.DOUBLE, (column, data, rowIndex) ->
        ((DoubleColumnVector) column).vector[rowIndex] = (Double) data);

    converters.put(ColumnVector.Type.TIMESTAMP, (column, data, rowIndex) ->
        ((TimestampColumnVector) column).set(rowIndex, new Timestamp((Long) data)));

    converters.put(ColumnVector.Type.LIST, (column, data, rowIndex) -> {
      ListColumnVector listColumn = (ListColumnVector) column;

      listColumn.offsets[rowIndex] = listColumn.childCount;
      listColumn.lengths[rowIndex] = ((Collection) data).size();
      listColumn.childCount += ((Collection) data).size();

      ColumnVector dataVector = listColumn.child;
      OrcFieldConverter orcFieldConverter = converters.get(dataVector.type);

      int currentPosition = (int) listColumn.offsets[rowIndex];
      for (Object object : ((Collection) data)) {
        orcFieldConverter.setData(dataVector, object, currentPosition++);
      }
    });

    converters.put(ColumnVector.Type.MAP, (column, data, rowIndex) -> {
      MapColumnVector mapColumn = (MapColumnVector) column;

      mapColumn.offsets[rowIndex] = mapColumn.childCount;
      mapColumn.lengths[rowIndex] = ((Map) data).size();
      mapColumn.childCount += ((Map) data).size();

      ColumnVector keyColumn = mapColumn.keys;
      ColumnVector valueColumn = mapColumn.values;
      OrcFieldConverter keyDataConvert = converters.get(keyColumn.type);
      OrcFieldConverter valueDataConvert = converters.get(valueColumn.type);

      int currentPosition = (int) mapColumn.offsets[rowIndex];
      for (Object entry : ((Map) data).entrySet()) {
        int pos = currentPosition++;
        keyDataConvert.setData(keyColumn, ((Map.Entry) entry).getKey(), pos);
        valueDataConvert.setData(valueColumn, ((Map.Entry) entry).getValue(), pos);
      }
    });

    converters.put(ColumnVector.Type.STRUCT, (column, data, rowIndex) -> {
      StructColumnVector structColumn = (StructColumnVector) column;
      convertStruct(structColumn.fields, (Struct) data, rowIndex);
    });

  }

  public static void convertStruct(ColumnVector[] orcColumns, Struct data, int rowIndex) {
    List<Field> schemaFields = data.schema().fields();
    for (int i = 0; i < orcColumns.length; i++) {
      ColumnVector column = orcColumns[i];
      Field field = schemaFields.get(i);
      OrcFieldConverter orcFieldConverter = converters.get(column.type);
      if (orcFieldConverter == null) {
        throw new UnsupportedOperationException("Orc schema not supported: " + column.type);
      }

      Object orcData = SchemaParser.fromConnectData(field.schema(), data.get(field));
      orcFieldConverter.setData(column, orcData, rowIndex);
    }
  }

}

