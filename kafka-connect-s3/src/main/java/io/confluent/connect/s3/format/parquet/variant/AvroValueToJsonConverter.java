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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

/**
 * Converts Avro values (GenericRecord, Map, List, primitives) into
 * Jackson {@link JsonNode} trees, which can then be serialized to JSON
 * strings for Variant encoding.
 *
 * <p>Automatically unwraps {@code google.protobuf.Struct},
 * {@code Value}, and {@code ListValue} representations produced by
 * Confluent's ProtobufConverter into clean JSON. The Avro wire
 * format for these types is:
 * <ul>
 *   <li>Struct &rarr; {@code {fields: [{key:.., value:..}]}}</li>
 *   <li>Value &rarr; {@code {kind_0: {string_value:.., ...}}}</li>
 *   <li>ListValue &rarr; {@code {values: [...]}}</li>
 * </ul>
 */
public class AvroValueToJsonConverter {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String FIELDS = "fields";
  private static final String KEY = "key";
  private static final String VALUE = "value";
  private static final String VALUES = "values";
  private static final Set<String> VALUE_KIND_CORE_FIELDS =
      initCoreKindFields();

  private static final int KIND_MATCH_THRESHOLD = 3;

  private static Set<String> initCoreKindFields() {
    Set<String> s = new HashSet<>();
    s.add("null_value");
    s.add("number_value");
    s.add("string_value");
    s.add("bool_value");
    s.add("struct_value");
    s.add("list_value");
    return s;
  }

  /**
   * Convert an arbitrary Avro value to a JSON string.
   *
   * @param avroValue the Avro value
   * @return JSON string representation
   */
  public static String toJsonString(Object avroValue) {
    JsonNode node = toJsonNode(avroValue);
    return node.toString();
  }

  static JsonNode toJsonNode(Object value) {
    if (value == null) {
      return NullNode.instance;
    }
    if (value instanceof IndexedRecord) {
      return recordToJson((IndexedRecord) value);
    }
    if (value instanceof Map) {
      return mapToJson((Map<?, ?>) value);
    }
    if (value instanceof Collection) {
      return smartCollectionToJson((Collection<?>) value);
    }
    return primitiveToJson(value);
  }

  private static JsonNode recordToJson(IndexedRecord record) {
    if (isProtobufStruct(record)) {
      return unwrapStruct(record);
    }
    if (isProtobufValue(record)) {
      return unwrapValue(record);
    }
    if (isProtobufListValue(record)) {
      return unwrapListValue(record);
    }
    return genericRecordToJson(record);
  }

  // --- Protobuf Struct/Value/ListValue detection ---

  private static boolean isProtobufStruct(
      IndexedRecord record
  ) {
    Schema schema = record.getSchema();
    List<Schema.Field> fields = schema.getFields();
    return fields.size() == 1
        && FIELDS.equals(fields.get(0).name())
        && isArrayOfKeyValue(fields.get(0).schema());
  }

  private static boolean isArrayOfKeyValue(Schema schema) {
    Schema resolved = resolveUnion(schema);
    if (resolved.getType() != Schema.Type.ARRAY) {
      return false;
    }
    Schema elem = resolveUnion(resolved.getElementType());
    if (elem.getType() != Schema.Type.RECORD) {
      return false;
    }
    return hasField(elem, KEY) && hasField(elem, VALUE);
  }

  private static boolean isProtobufValue(
      IndexedRecord record
  ) {
    List<Schema.Field> fields = record.getSchema().getFields();
    if (fields.size() != 1) {
      return false;
    }
    String name = fields.get(0).name();
    if (!name.startsWith("kind")) {
      return false;
    }
    Schema inner = resolveUnion(fields.get(0).schema());
    if (inner.getType() != Schema.Type.RECORD) {
      return false;
    }
    return hasValueKindShape(inner);
  }

  private static boolean hasValueKindShape(Schema schema) {
    int matches = 0;
    for (Schema.Field f : schema.getFields()) {
      if (VALUE_KIND_CORE_FIELDS.contains(f.name())) {
        matches++;
      }
    }
    return matches >= KIND_MATCH_THRESHOLD;
  }

  private static boolean isProtobufListValue(
      IndexedRecord record
  ) {
    List<Schema.Field> fields = record.getSchema().getFields();
    return fields.size() == 1
        && VALUES.equals(fields.get(0).name())
        && resolveUnion(fields.get(0).schema()).getType()
            == Schema.Type.ARRAY;
  }

  // --- Unwrap implementations ---

  private static JsonNode unwrapStruct(IndexedRecord record) {
    Object fieldsVal = record.get(0);
    if (fieldsVal == null) {
      return NullNode.instance;
    }
    ObjectNode obj = MAPPER.createObjectNode();
    for (Object entry : (Collection<?>) fieldsVal) {
      IndexedRecord kv = (IndexedRecord) entry;
      String key = kv.get(
          kv.getSchema().getField(KEY).pos()).toString();
      Object val = kv.get(
          kv.getSchema().getField(VALUE).pos());
      obj.set(key, toJsonNode(val));
    }
    return obj;
  }

  private static JsonNode unwrapValue(IndexedRecord record) {
    Object kindWrapper = record.get(0);
    if (kindWrapper == null) {
      return NullNode.instance;
    }
    IndexedRecord kind = (IndexedRecord) kindWrapper;
    return extractFromKind(kind);
  }

  private static JsonNode extractFromKind(IndexedRecord kind) {
    Schema schema = kind.getSchema();
    for (Schema.Field f : schema.getFields()) {
      Object v = kind.get(f.pos());
      if (v == null) {
        continue;
      }
      switch (f.name()) {
        case "string_value":
          return new TextNode(v.toString());
        case "number_value":
          return numberToJson((Number) v);
        case "bool_value":
          return BooleanNode.valueOf((Boolean) v);
        case "null_value":
          return NullNode.instance;
        case "struct_value":
          return toJsonNode(v);
        case "list_value":
          return toJsonNode(v);
        default:
          break;
      }
    }
    return NullNode.instance;
  }

  private static JsonNode unwrapListValue(
      IndexedRecord record
  ) {
    Object valuesObj = record.get(0);
    if (valuesObj == null) {
      return MAPPER.createArrayNode();
    }
    ArrayNode arr = MAPPER.createArrayNode();
    for (Object item : (Collection<?>) valuesObj) {
      arr.add(toJsonNode(item));
    }
    return arr;
  }

  // --- Generic fallback for non-Struct records ---

  private static JsonNode genericRecordToJson(
      IndexedRecord record
  ) {
    if (isSingleFieldWrapper(record)) {
      return unwrapSingleField(record);
    }
    ObjectNode node = MAPPER.createObjectNode();
    for (Schema.Field field : record.getSchema().getFields()) {
      node.set(field.name(),
          toJsonNode(record.get(field.pos())));
    }
    return node;
  }

  /**
   * Detects wrapper records with a single field whose value is a
   * complex type (Struct/Map/Array). Common for Protobuf wrapper
   * messages like {@code StructWrapper { Struct struct = 1; }}.
   * Unwraps to the inner value directly.
   */
  private static boolean isSingleFieldWrapper(
      IndexedRecord record
  ) {
    List<Schema.Field> fields = record.getSchema().getFields();
    if (fields.size() != 1) {
      return false;
    }
    Object val = record.get(0);
    return val instanceof IndexedRecord
        || val instanceof Map
        || val instanceof Collection;
  }

  private static JsonNode unwrapSingleField(
      IndexedRecord record
  ) {
    return toJsonNode(record.get(0));
  }

  // --- Primitives and other types ---

  private static JsonNode primitiveToJson(Object value) {
    if (value instanceof CharSequence) {
      return new TextNode(value.toString());
    }
    if (value instanceof Number) {
      return numberToJson((Number) value);
    }
    if (value instanceof Boolean) {
      return BooleanNode.valueOf((Boolean) value);
    }
    return binaryOrFallbackToJson(value);
  }

  private static JsonNode numberToJson(Number value) {
    if (value instanceof Integer) {
      return IntNode.valueOf(value.intValue());
    }
    if (value instanceof Long) {
      return LongNode.valueOf(value.longValue());
    }
    if (value instanceof Float) {
      return FloatNode.valueOf(value.floatValue());
    }
    return DoubleNode.valueOf(value.doubleValue());
  }

  private static JsonNode binaryOrFallbackToJson(
      Object value
  ) {
    if (value instanceof ByteBuffer) {
      return new TextNode(Base64.getEncoder()
          .encodeToString(bufferToBytes((ByteBuffer) value)));
    }
    if (value instanceof byte[]) {
      return new TextNode(Base64.getEncoder()
          .encodeToString((byte[]) value));
    }
    if (value instanceof GenericFixed) {
      return new TextNode(Base64.getEncoder()
          .encodeToString(((GenericFixed) value).bytes()));
    }
    return new TextNode(value.toString());
  }

  private static JsonNode mapToJson(Map<?, ?> map) {
    ObjectNode node = MAPPER.createObjectNode();
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      node.set(entry.getKey().toString(),
          toJsonNode(entry.getValue()));
    }
    return node;
  }

  /**
   * Converts a collection to JSON. Detects Protobuf
   * map-as-array patterns (array of records with "key" and
   * "value" fields) and converts them to JSON objects.
   */
  private static JsonNode smartCollectionToJson(
      Collection<?> list
  ) {
    if (isMapAsArray(list)) {
      return mapAsArrayToJson(list);
    }
    ArrayNode node = MAPPER.createArrayNode();
    for (Object item : list) {
      node.add(toJsonNode(item));
    }
    return node;
  }

  private static boolean isMapAsArray(Collection<?> list) {
    if (list.isEmpty()) {
      return false;
    }
    Object first = list.iterator().next();
    if (!(first instanceof IndexedRecord)) {
      return false;
    }
    IndexedRecord rec = (IndexedRecord) first;
    return hasField(rec.getSchema(), KEY)
        && hasField(rec.getSchema(), VALUE)
        && rec.getSchema().getFields().size() == 2;
  }

  private static JsonNode mapAsArrayToJson(
      Collection<?> list
  ) {
    ObjectNode obj = MAPPER.createObjectNode();
    for (Object item : list) {
      IndexedRecord rec = (IndexedRecord) item;
      Schema schema = rec.getSchema();
      String key = rec.get(
          schema.getField(KEY).pos()).toString();
      Object val = rec.get(
          schema.getField(VALUE).pos());
      obj.set(key, toJsonNode(val));
    }
    return obj;
  }

  private static byte[] bufferToBytes(ByteBuffer buf) {
    ByteBuffer dup = buf.duplicate();
    byte[] bytes = new byte[dup.remaining()];
    dup.get(bytes);
    return bytes;
  }

  private static Schema resolveUnion(Schema schema) {
    if (schema.getType() == Schema.Type.UNION) {
      for (Schema branch : schema.getTypes()) {
        if (branch.getType() != Schema.Type.NULL) {
          return branch;
        }
      }
    }
    return schema;
  }

  private static boolean hasField(
      Schema schema, String name
  ) {
    return schema.getField(name) != null;
  }
}
