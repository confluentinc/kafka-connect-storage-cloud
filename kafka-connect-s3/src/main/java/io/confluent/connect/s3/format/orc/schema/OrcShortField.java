package io.confluent.connect.s3.format.orc.schema;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;

class OrcShortField extends AbstractOrcField {

  OrcShortField(String name, int columnIndex, boolean optional) {
    super(name, columnIndex, optional);
  }

  @Override
  protected void setNotNullData(ColumnVector[] orcColumns, Object data, int rowIndex) {
    LongColumnVector col = (LongColumnVector) orcColumns[columnIndex];
    col.vector[rowIndex] = ((Short) data).longValue();
  }

  @Override
  protected String getTypeDescription() {
    return "smallint";
  }
}
