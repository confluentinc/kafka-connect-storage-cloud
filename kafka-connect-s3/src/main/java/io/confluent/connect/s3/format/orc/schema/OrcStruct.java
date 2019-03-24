package io.confluent.connect.s3.format.orc.schema;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.kafka.connect.data.Struct;

import java.util.Iterator;
import java.util.List;

class OrcStruct extends AbstractOrcField {
  private final List<AbstractOrcField> orcFields;

  OrcStruct(List<AbstractOrcField> orcFields) {
    super("root", -1, false);
    this.orcFields = orcFields;
  }

  OrcStruct(String name, List<AbstractOrcField> orcFields, int columnIndex, boolean optional) {
    super(name, columnIndex, optional);
    this.orcFields = orcFields;
  }

  @Override
  void setData(ColumnVector[] orcColumns, Object data, int rowIndex) {
    if (data == null) {
      for (int i = 0; i < orcFields.size(); i++) {
        StructColumnVector structColumns = (StructColumnVector) orcColumns[columnIndex];
        ColumnVector childColumn = structColumns.fields[i];
        childColumn.isNull[rowIndex] = true;
        childColumn.noNulls = false;
      }
    } else {
      if(columnIndex == -1){
        setNotNullData(orcColumns, data, rowIndex);
      }else{
        StructColumnVector structColumns = (StructColumnVector) orcColumns[columnIndex];
        setNotNullData(structColumns.fields, data, rowIndex);
      }

    }
  }

  @Override
  protected void setNotNullData(ColumnVector[] orcColumns, Object data, int rowIndex) {
    Struct struct = (Struct) data;
    for (AbstractOrcField orcField : orcFields) {
      Object fieldValue = struct.get(orcField.name);
      orcField.setData(orcColumns, fieldValue, rowIndex);
    }
  }

  @Override
  protected String getTypeDescription() {
    if (orcFields.isEmpty()) {
      throw new IllegalArgumentException("No fields were specified");
    }
    StringBuilder schema = new StringBuilder("struct<");
    Iterator<AbstractOrcField> fieldIterator = orcFields.iterator();
    while (fieldIterator.hasNext()) {
      AbstractOrcField field = fieldIterator.next();
      schema.append(field.fieldDescription());
      if (fieldIterator.hasNext()) {
        schema.append(",");
      }
    }
    schema.append(">");
    return schema.toString();
  }
}
