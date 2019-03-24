package io.confluent.connect.s3.format.orc.schema;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;

class OrcStringField extends AbstractOrcField {


  OrcStringField(String name, int columnIndex, boolean optional) {
    super(name, columnIndex, optional);
  }

  @Override
  protected void setNotNullData(ColumnVector[] orcColumns, Object data, int rowIndex) {
    BytesColumnVector col = (BytesColumnVector) orcColumns[columnIndex];
    col.setVal(rowIndex, ((String) data).getBytes());
  }

  @Override
  protected String getTypeDescription() {
    return "string";
  }
}
