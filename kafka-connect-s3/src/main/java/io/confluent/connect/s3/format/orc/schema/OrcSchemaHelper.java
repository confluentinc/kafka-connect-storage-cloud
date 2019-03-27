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

package io.confluent.connect.s3.format.orc.schema;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.orc.TypeDescription;

import java.util.ArrayList;
import java.util.List;

public class OrcSchemaHelper {

  private final TypeDescription orcSchema;
  private final OrcStruct orcStruct;


  public OrcSchemaHelper(Schema schema) {
    orcStruct = new OrcStruct(parseConnectSchemaToOrcFields(schema));
    orcSchema = TypeDescription.fromString(orcStruct.getTypeDescription());
  }

  public VectorizedRowBatch createBatch() {
    return orcSchema.createRowBatch();
  }

  public void setData(VectorizedRowBatch batch, Object connectData) {
    OrcFieldHelper.convertStruct(batch.cols, (Struct)connectData, batch.size++);
//    orcStruct.setData(batch.cols, connectData, batch.size++);
  }


  private static List<AbstractOrcField> parseConnectSchemaToOrcFields(Schema connectSchema) {
    List<Field> connectFields = connectSchema.fields();
    List<AbstractOrcField> childOrcFields = new ArrayList<>(connectFields.size());
    for (Field connectField : connectFields) {

      Schema fieldSchema = connectField.schema();
      boolean optional = fieldSchema.isOptional();
      Schema.Type fieldType = fieldSchema.type();
      int fieldIndex = childOrcFields.size();
      String fieldName = connectField.name();
      switch (fieldType) {
        case BOOLEAN:
          childOrcFields.add(new OrcBooleanField(fieldName, fieldIndex, optional));
          break;
        case INT16:
          childOrcFields.add(new OrcShortField(fieldName, fieldIndex, optional));
          break;
        case INT32: {
          childOrcFields.add(new OrcIntegerField(fieldName, fieldIndex, optional));
          break;
        }
        case INT64: {
          childOrcFields.add(new OrcLongField(fieldName, fieldIndex, optional));
          break;
        }
        case STRING: {
          childOrcFields.add(new OrcStringField(fieldName, fieldIndex, optional));
          break;
        }
        case FLOAT64:
          childOrcFields.add(new OrcDoubleField(fieldName, fieldIndex, optional));
          break;
        case FLOAT32: {
          childOrcFields.add(new OrcFloatField(fieldName, fieldIndex, optional));
          break;
        }
        case STRUCT:
          childOrcFields.add(new OrcStruct(fieldName, parseConnectSchemaToOrcFields(fieldSchema), fieldIndex, optional));
          break;
        //todo map, list
        default:
          if (Timestamp.SCHEMA.type().equals(fieldType)) {
            childOrcFields.add(new OrcTimestampField(fieldName, fieldIndex, optional));
          } else {
            throw new DataException("Unsupported type: " + fieldType);
          }
      }
    }
    return childOrcFields;
  }

  public TypeDescription getOrcSchema() {
    return orcSchema;
  }
}
