/*
 * Copyright 2026 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.s3.format.parquet.variant;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Transforms a Parquet {@link MessageType} by replacing fields that correspond to
 * JSON-carrying columns with the Parquet VARIANT logical type encoding.
 *
 * <p>A VARIANT field in Parquet is a group with two required binary children:
 * <pre>
 * optional group field_name (VARIANT(1)) {
 *   required binary metadata;
 *   required binary value;
 * }
 * </pre>
 *
 * <p>The {@code metadata} field holds the Variant metadata dictionary (field-name deduplication),
 * and the {@code value} field holds the Variant binary-encoded data.
 */
public class VariantSchemaBuilder {

  private static final byte VARIANT_SPEC_VERSION = 1;

  /**
   * Transform the given Parquet schema, replacing fields whose dot-separated path
   * appears in {@code variantFieldPaths} with VARIANT group types.
   *
   * @param original           the original Parquet MessageType (derived from Avro schema)
   * @param variantFieldPaths  set of dot-separated paths identifying JSON fields
   * @return a new MessageType with VARIANT groups substituted
   */
  public static MessageType transformSchema(MessageType original, Set<String> variantFieldPaths) {
    if (variantFieldPaths == null || variantFieldPaths.isEmpty()) {
      return original;
    }
    List<Type> transformedFields = transformFields(original.getFields(), "", variantFieldPaths);
    return new MessageType(original.getName(), transformedFields);
  }

  private static List<Type> transformFields(
      List<Type> fields,
      String prefix,
      Set<String> variantFieldPaths
  ) {
    List<Type> result = new ArrayList<>(fields.size());
    for (Type field : fields) {
      String path = prefix.isEmpty() ? field.getName() : prefix + "." + field.getName();

      if (variantFieldPaths.contains(path)) {
        result.add(buildVariantGroup(field.getName(), field.getRepetition()));
      } else if (field instanceof GroupType) {
        GroupType groupType = (GroupType) field;
        List<Type> children = transformFields(groupType.getFields(), path, variantFieldPaths);
        result.add(groupType.withNewFields(children));
      } else {
        result.add(field);
      }
    }
    return result;
  }

  /**
   * Build a VARIANT group type:
   * <pre>
   * {repetition} group {name} (VARIANT(1)) {
   *   required binary metadata;
   *   required binary value;
   * }
   * </pre>
   */
  static GroupType buildVariantGroup(String name, Type.Repetition repetition) {
    return Types.buildGroup(repetition)
        .as(LogicalTypeAnnotation.variantType(VARIANT_SPEC_VERSION))
        .addField(
            Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                .named("metadata")
        )
        .addField(
            Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                .named("value")
        )
        .named(name);
  }
}
