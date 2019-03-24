package io.confluent.connect.s3.format.orc.schema;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;

class OrcBooleanField extends AbstractOrcField {

  OrcBooleanField(String name, int columnIndex, boolean optional) {
    super(name, columnIndex, optional);
  }

  @Override
  protected void setNotNullData(ColumnVector[] orcColumns, Object data, int rowIndex) {
    LongColumnVector col = (LongColumnVector) orcColumns[columnIndex];
    col.vector[rowIndex] = ((Boolean) data) ? 1L : 0L;
  }

  @Override
  protected String getTypeDescription() {
    return "boolean";
  }
}
