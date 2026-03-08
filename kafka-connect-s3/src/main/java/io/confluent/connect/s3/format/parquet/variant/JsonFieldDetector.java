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

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

/**
 * Detects fields within a Kafka Connect schema that should be written as
 * Parquet VARIANT columns.
 *
 * <p>Detection strategies (in order of evaluation):
 * <ol>
 *   <li><b>Explicit field names</b> &ndash; matches any field whose
 *       leaf name is in the configured set.</li>
 *   <li><b>Connect schema names</b> &ndash; matches any field whose
 *       Connect schema {@code name()} is in the configured set
 *       (e.g.&nbsp;{@code io.debezium.data.Json}).</li>
 *   <li><b>Recursive schema detection</b> &ndash; automatically
 *       identifies fields whose schema contains circular references
 *       (e.g.&nbsp;{@code google.protobuf.Struct} which recurses
 *       through {@code Value &rarr; Struct &rarr; Value}).
 *       Such schemas cause {@code StackOverflowError} in
 *       {@code AvroSchemaConverter}; converting them to VARIANT
 *       eliminates the recursion.</li>
 * </ol>
 *
 * <p>Returns a set of dot-separated field paths
 * (e.g.&nbsp;{@code "before.gst_details"}) so that nested fields
 * inside structs like Debezium&rsquo;s before/after envelope are
 * properly identified.
 */
public class JsonFieldDetector {

  private final Set<String> connectNames;
  private final Set<String> explicitFieldNames;

  public JsonFieldDetector(
      List<String> connectNames,
      Set<String> explicitFieldNames
  ) {
    this.connectNames = connectNames == null
        ? Collections.emptySet() : new HashSet<>(connectNames);
    this.explicitFieldNames = explicitFieldNames == null
        ? Collections.emptySet() : explicitFieldNames;
  }

  /**
   * Walk the given Connect schema and return the set of
   * dot-separated field paths that should be written as Parquet
   * VARIANT columns.
   */
  public Set<String> detect(Schema schema) {
    if (schema == null) {
      return Collections.emptySet();
    }
    Set<String> result = new HashSet<>();
    Set<Schema> visited = Collections.newSetFromMap(
        new IdentityHashMap<>());
    walkSchema(schema, "", result, visited);
    return result;
  }

  private void walkSchema(
      Schema schema, String prefix,
      Set<String> result, Set<Schema> visited
  ) {
    if (visited.contains(schema)) {
      return;
    }
    visited.add(schema);

    switch (schema.type()) {
      case STRUCT:
        walkStructFields(schema, prefix, result, visited);
        break;
      case ARRAY:
        walkSchema(
            schema.valueSchema(), prefix, result, visited);
        break;
      case MAP:
        walkSchema(
            schema.valueSchema(), prefix, result, visited);
        break;
      default:
        break;
    }
  }

  private void walkStructFields(
      Schema schema, String prefix,
      Set<String> result, Set<Schema> visited
  ) {
    for (Field field : schema.fields()) {
      String path = prefix.isEmpty()
          ? field.name() : prefix + "." + field.name();
      Schema fieldSchema = field.schema();

      if (isVariantField(field.name(), fieldSchema)) {
        result.add(path);
      } else if (isRecursiveSchema(fieldSchema)) {
        result.add(path);
      } else {
        walkSchema(fieldSchema, path, result, visited);
      }
    }
  }

  /**
   * A field is selected for VARIANT encoding if its name appears in
   * the explicit field-name set, or if its Connect schema name
   * matches one of the configured connect names.
   */
  private boolean isVariantField(
      String fieldName, Schema fieldSchema
  ) {
    if (explicitFieldNames.contains(fieldName)) {
      return true;
    }
    String schemaName = fieldSchema.name();
    return schemaName != null
        && connectNames.contains(schemaName);
  }

  /**
   * Detects whether a schema contains circular references that
   * would cause {@code StackOverflowError} in Parquet's
   * {@code AvroSchemaConverter}. Uses identity-based tracking
   * to detect true object cycles (as produced by Confluent's
   * ProtobufConverter for {@code google.protobuf.Struct}).
   */
  static boolean isRecursiveSchema(Schema schema) {
    Set<Schema> ancestors = Collections.newSetFromMap(
        new IdentityHashMap<>());
    return checkRecursive(schema, ancestors);
  }

  private static boolean checkRecursive(
      Schema schema, Set<Schema> ancestors
  ) {
    if (schema == null) {
      return false;
    }
    if (!ancestors.add(schema)) {
      return true;
    }
    boolean recursive = checkChildren(schema, ancestors);
    ancestors.remove(schema);
    return recursive;
  }

  private static boolean checkChildren(
      Schema schema, Set<Schema> ancestors
  ) {
    switch (schema.type()) {
      case STRUCT:
        return checkStructChildren(schema, ancestors);
      case ARRAY:
        return checkRecursive(
            schema.valueSchema(), ancestors);
      case MAP:
        return checkRecursive(
            schema.keySchema(), ancestors)
            || checkRecursive(
                schema.valueSchema(), ancestors);
      default:
        return false;
    }
  }

  private static boolean checkStructChildren(
      Schema schema, Set<Schema> ancestors
  ) {
    for (Field f : schema.fields()) {
      if (checkRecursive(f.schema(), ancestors)) {
        return true;
      }
    }
    return false;
  }
}
