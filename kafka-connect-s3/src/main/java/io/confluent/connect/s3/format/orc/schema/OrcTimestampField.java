package io.confluent.connect.s3.format.orc.schema;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;

import java.sql.Timestamp;

class OrcTimestampField extends AbstractOrcField {

  OrcTimestampField(String name, int columnIndex, boolean optional) {
    super(name, columnIndex, optional);
  }

  @Override
  protected void setNotNullData(ColumnVector[] orcColumns, Object data, int rowIndex) {
    TimestampColumnVector col = (TimestampColumnVector) orcColumns[columnIndex];
    col.set(rowIndex, new Timestamp((Long) data));
  }

  @Override
  protected String getTypeDescription() {
    return "timestamp";
  }
}
