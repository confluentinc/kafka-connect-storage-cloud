package io.confluent.connect.s3.format.orc.schema;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;

abstract class AbstractOrcField {
  protected final String name;
  protected final int columnIndex;
  protected final boolean optional;

  public AbstractOrcField(String name, int columnIndex, boolean optional) {
    this.name = name;
    this.columnIndex = columnIndex;
    this.optional = optional;
  }

  void setData(ColumnVector[] orcColumns, Object data, int rowIndex){
    if(data == null && optional){
      ColumnVector col = orcColumns[columnIndex];
      col.isNull[rowIndex] = true;
      col.noNulls=false;
    }else{
      setNotNullData(orcColumns, data, rowIndex);
    }
  }

  protected abstract void setNotNullData(ColumnVector[] orcColumns, Object data, int rowIndex);

  String fieldDescription() {
    return name + ":" + getTypeDescription();
  }

  protected abstract String getTypeDescription();

}
