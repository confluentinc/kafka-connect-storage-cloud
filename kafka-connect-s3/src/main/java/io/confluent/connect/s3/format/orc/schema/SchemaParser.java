package io.confluent.connect.s3.format.orc.schema;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.orc.TypeDescription;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SchemaParser {

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
        return TypeDescription.createMap(fromConnectFieldSchema(fieldSchema.keySchema()), fromConnectFieldSchema(fieldSchema.valueSchema()));
      case ARRAY:
        return TypeDescription.createList(fromConnectFieldSchema(fieldSchema.valueSchema()));
      case STRUCT:
        List<Field> fields = fieldSchema.fields();
        return fromConnectSchema(fields);
      default:
        throw new DataException("Unsupported type: " + fieldType);
    }
  }

 /* public static void fromConnectData(ColumnVector orcColumn, Schema connectSchema, Object connectData, int rowIndex){
    if(connectData == null){
      orcColumn.isNull[rowIndex] = true;
      orcColumn.noNulls = false;
    }else{
      ColumnVector.Type type = orcColumn.type;
      switch (type){
        case LONG:
          ((LongColumnVector) orcColumn).vector[rowIndex] = (Long) fromConnectData(connectSchema, ))
      }
    }
  }*/


  public static Object fromConnectData(Schema connectSchema, Object fieldValue) {
    if (fieldValue == null) {
      return null;
    }
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
          transformedValues.add(fromConnectData(listType, value));
        }
        return transformedValues;
      case MAP:
        Schema keySchema = connectSchema.keySchema();
        Schema valueSchema = connectSchema.valueSchema();
        Map<Object, Object> transformedMap = new HashMap<>();
        for (Object entry : ((Map) fieldValue).entrySet()) {
          Object key = fromConnectData(keySchema, ((Map.Entry) entry).getKey());
          Object value = fromConnectData(valueSchema, ((Map.Entry) entry).getValue());
          transformedMap.put(key, value);
        }
        return transformedMap;
      default:
        throw new DataException("Unsupported schema:" + connectSchema);
    }
  }
}
