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
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.orc.TypeDescription;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OrcConverterUtils {

  /**
   * Create orc schema description from connect schema
   * @param connectSchema connect schema
   * @return orc schema
   */
  public static TypeDescription fromConnectSchema(Schema connectSchema) {
    List<Field> fields = connectSchema.fields();
    TypeDescription orcSchema = fromConnectSchema(fields);
    return orcSchema;
  }

  private static TypeDescription fromConnectSchema(List<Field> fields) {
    TypeDescription struct = TypeDescription.createStruct();
    for (Field field : fields) {
      struct.addField(field.name(), fromConnectFieldSchema(field.schema()));
    }
    return struct;

  }

  private static TypeDescription fromConnectFieldSchema(Schema fieldSchema) {
    Schema.Type fieldType = fieldSchema.type();
    switch (fieldType) {
      case BOOLEAN:
        return TypeDescription.createBoolean();
      case BYTES:
        return TypeDescription.createBinary();
      case INT8:
        return TypeDescription.createByte();
      case INT16:
        return TypeDescription.createShort();
      case INT32:
        return TypeDescription.createInt();
      case INT64:
        if (Timestamp.LOGICAL_NAME.equals(fieldSchema.name())) {
          return TypeDescription.createTimestamp();
        } else {
          return TypeDescription.createLong();
        }
      case STRING:
        return TypeDescription.createString();
      case FLOAT64:
        return TypeDescription.createDouble();
      case FLOAT32:
        return TypeDescription.createFloat();
      case MAP:
        return TypeDescription.createMap(
            fromConnectFieldSchema(fieldSchema.keySchema()),
            fromConnectFieldSchema(fieldSchema.valueSchema())
        );
      case ARRAY:
        return TypeDescription.createList(fromConnectFieldSchema(fieldSchema.valueSchema()));
      case STRUCT:
        List<Field> fields = fieldSchema.fields();
        return fromConnectSchema(fields);
      default:
        throw new DataException("Unsupported type: " + fieldType);
    }
  }

  /**
   * Parse connector data and set its data to orc columns
   * @param orcColumns columns which represent row batch
   * @param data connect data to parse
   * @param rowIndex index of row in batch
   */
  public static void parseConnectData(ColumnVector[] orcColumns, Struct data, int rowIndex) {
    List<Field> schemaFields = data.schema().fields();
    for (int i = 0; i < orcColumns.length; i++) {
      ColumnVector column = orcColumns[i];
      Field field = schemaFields.get(i);
      parseConnectData(column, field.schema(), data.get(field), rowIndex);
    }
  }

  private static void parseConnectData(ColumnVector column, Schema connectFieldSchema,
                                       Object fieldData, int rowIndex) {
    if (fieldData == null) {
      setNullData(column, rowIndex);
    } else {
      ColumnVector.Type type = column.type;
      switch (type) {
        case LONG:
          ((LongColumnVector) column).vector[rowIndex] =
              (Long) fromConnectFieldData(connectFieldSchema, fieldData);
          break;
        case BYTES:
          ((BytesColumnVector) column)
              .setVal(rowIndex, (byte[]) fromConnectFieldData(connectFieldSchema, fieldData));
          break;
        case DOUBLE:
          ((DoubleColumnVector) column).vector[rowIndex]
              = (Double) fromConnectFieldData(connectFieldSchema, fieldData);
          break;
        case TIMESTAMP:
          ((TimestampColumnVector) column).set(
              rowIndex,
              new java.sql.Timestamp((Long) fromConnectFieldData(connectFieldSchema, fieldData))
          );
          break;
        case LIST:
          ListColumnVector listColumn = (ListColumnVector) column;
          listColumn.offsets[rowIndex] = listColumn.childCount;
          listColumn.lengths[rowIndex] = ((Collection) fieldData).size();
          listColumn.childCount += ((Collection) fieldData).size();

          ColumnVector childListColumn = listColumn.child;
          Schema childFieldSchema = connectFieldSchema.valueSchema();
          int currentPosition = (int) listColumn.offsets[rowIndex];
          for (Object object : ((Collection) fieldData)) {
            parseConnectData(childListColumn, childFieldSchema, object, currentPosition++);
          }
          break;
        case MAP:
          MapColumnVector mapColumn = (MapColumnVector) column;

          mapColumn.offsets[rowIndex] = mapColumn.childCount;
          mapColumn.lengths[rowIndex] = ((Map) fieldData).size();
          mapColumn.childCount += ((Map) fieldData).size();

          ColumnVector keyColumn = mapColumn.keys;
          ColumnVector valueColumn = mapColumn.values;

          int startPosition = (int) mapColumn.offsets[rowIndex];
          for (Object entry : ((Map) fieldData).entrySet()) {
            int pos = startPosition++;
            Object key = ((Map.Entry) entry).getKey();
            parseConnectData(keyColumn, connectFieldSchema.keySchema(), key, pos);
            Object value = ((Map.Entry) entry).getValue();
            parseConnectData(valueColumn, connectFieldSchema.valueSchema(), value, pos);
          }
          break;
        case STRUCT:
          StructColumnVector structColumn = (StructColumnVector) column;
          parseConnectData(structColumn.fields, (Struct) fieldData, rowIndex);
          break;
        default:
          throw new DataException("Unsupported orc schema type:" + type);
      }
    }
  }

  /**
   * Mark that current column on row has null as value, mark whole column as nullable
   */
  private static void setNullData(ColumnVector column, int rowIndex) {
    column.isNull[rowIndex] = true;
    column.noNulls = false;
  }


  private static Object fromConnectFieldData(Schema connectSchema, Object fieldValue) {
    Schema.Type type = connectSchema.type();
    switch (type) {
      case FLOAT64:
      case STRUCT:
        return fieldValue;
      case BYTES:
        return ((ByteBuffer) fieldValue).array();
      case BOOLEAN:
        return ((Boolean) fieldValue) ? 1L : 0L;
      case FLOAT32:
        return Double.valueOf(fieldValue.toString());
      case INT8:
        return ((Byte) fieldValue).longValue();
      case INT16:
        return ((Short) fieldValue).longValue();
      case INT32:
        return ((Integer) fieldValue).longValue();
      case INT64:
        String name = connectSchema.name();
        if (org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME.equals(name)) {
          return ((Date) fieldValue).getTime();
        } else {
          return fieldValue;
        }
      case STRING:
        return ((String) fieldValue).getBytes();
      case ARRAY:
        Schema listType = connectSchema.valueSchema();
        List<Object> transformedValues = new ArrayList<>(((List) fieldValue).size());
        for (Object value : ((List) fieldValue)) {
          transformedValues.add(fromConnectFieldData(listType, value));
        }
        return transformedValues;
      case MAP:
        Schema keySchema = connectSchema.keySchema();
        Schema valueSchema = connectSchema.valueSchema();
        Map<Object, Object> transformedMap = new HashMap<>();
        for (Object entry : ((Map) fieldValue).entrySet()) {
          Object key = fromConnectFieldData(keySchema, ((Map.Entry) entry).getKey());
          Object value = fromConnectFieldData(valueSchema, ((Map.Entry) entry).getValue());
          transformedMap.put(key, value);
        }
        return transformedMap;
      default:
        throw new DataException("Unsupported connect schema type:" + connectSchema);
    }
  }
}
