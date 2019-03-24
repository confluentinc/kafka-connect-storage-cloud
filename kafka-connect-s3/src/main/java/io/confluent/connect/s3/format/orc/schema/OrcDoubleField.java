package io.confluent.connect.s3.format.orc.schema;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;

class OrcDoubleField extends AbstractOrcField {

  OrcDoubleField(String name, int columnIndex, boolean optional) {
    super(name, columnIndex, optional);
  }

  @Override
  protected void setNotNullData(ColumnVector[] orcColumns, Object data, int rowIndex) {
    DoubleColumnVector col = (DoubleColumnVector) orcColumns[columnIndex];
    col.vector[rowIndex] = (Double) data;
  }

  @Override
  protected String getTypeDescription() {
    return "double";
  }
}
