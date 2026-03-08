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
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AvroValueToJsonConverterTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Build Avro schemas that mirror what Confluent's
   * ProtobufConverter produces for google.protobuf.Struct.
   *
   * Struct -> {fields: [{key: string, value: Value}]}
   * Value -> {kind_0: {null_value, number_value, string_value,
   *           bool_value, struct_value, list_value}}
   * ListValue -> {values: [Value]}
   */

  private static Schema kindSchema() {
    return SchemaBuilder.record("Kind").fields()
        .optionalString("null_value")
        .optionalDouble("number_value")
        .optionalString("string_value")
        .optionalBoolean("bool_value")
        .name("struct_value").type().optional()
            .record("StructValueNested").fields()
            .name("fields").type().optional().array()
                .items().record("StructFieldNested").fields()
                .requiredString("key")
                .name("value").type().optional()
                    .record("ValueNested").fields()
                    .name("kind_0").type().optional()
                        .record("KindNested").fields()
                        .optionalString("null_value")
                        .optionalDouble("number_value")
                        .optionalString("string_value")
                        .optionalBoolean("bool_value")
                        .endRecord()
                    .endRecord()
                .endRecord()
            .endRecord()
        .name("list_value").type().optional()
            .record("ListValueInner").fields()
            .name("values").type().optional().array()
                .items().record("ValueInList").fields()
                .name("kind_0").type().optional()
                    .record("KindInList").fields()
                    .optionalString("null_value")
                    .optionalDouble("number_value")
                    .optionalString("string_value")
                    .optionalBoolean("bool_value")
                    .endRecord()
                .endRecord()
            .endRecord()
        .endRecord();
  }

  private static Schema valueSchema() {
    return SchemaBuilder.record("Value").fields()
        .name("kind_0").type().optional().type(kindSchema())
        .endRecord();
  }

  private static Schema structFieldSchema() {
    return SchemaBuilder.record("StructField").fields()
        .requiredString("key")
        .name("value").type().optional().type(valueSchema())
        .endRecord();
  }

  private static Schema structSchema() {
    return SchemaBuilder.record("Struct").fields()
        .name("fields").type().optional().array()
            .items(structFieldSchema())
        .endRecord();
  }

  private GenericRecord makeKind(
      String nullVal, Double numVal,
      String strVal, Boolean boolVal
  ) {
    GenericRecord kind =
        new GenericData.Record(kindSchema());
    kind.put("null_value", nullVal);
    kind.put("number_value", numVal);
    kind.put("string_value", strVal);
    kind.put("bool_value", boolVal);
    kind.put("struct_value", null);
    kind.put("list_value", null);
    return kind;
  }

  private GenericRecord makeValue(GenericRecord kind) {
    GenericRecord value =
        new GenericData.Record(valueSchema());
    value.put("kind_0", kind);
    return value;
  }

  private GenericRecord makeField(
      String key, GenericRecord value
  ) {
    GenericRecord field =
        new GenericData.Record(structFieldSchema());
    field.put("key", key);
    field.put("value", value);
    return field;
  }

  private GenericRecord makeStruct(
      GenericRecord... fieldRecords
  ) {
    GenericRecord struct =
        new GenericData.Record(structSchema());
    struct.put("fields", Arrays.asList(fieldRecords));
    return struct;
  }

  @Test
  public void testUnwrapSimpleStruct() throws Exception {
    GenericRecord struct = makeStruct(
        makeField("name",
            makeValue(makeKind(null, null, "Alice", null))),
        makeField("age",
            makeValue(makeKind(null, 30.0, null, null))),
        makeField("active",
            makeValue(makeKind(null, null, null, true)))
    );

    String json =
        AvroValueToJsonConverter.toJsonString(struct);
    JsonNode node = MAPPER.readTree(json);

    assertEquals("Alice", node.get("name").asText());
    assertEquals(30.0, node.get("age").asDouble(), 0.001);
    assertTrue(node.get("active").asBoolean());
  }

  @Test
  public void testUnwrapNullValue() throws Exception {
    GenericRecord struct = makeStruct(
        makeField("discount",
            makeValue(
                makeKind("NULL_VALUE", null, null, null)))
    );

    String json =
        AvroValueToJsonConverter.toJsonString(struct);
    JsonNode node = MAPPER.readTree(json);

    assertTrue(node.get("discount").isNull());
  }

  @Test
  public void testUnwrapValueWithListValue() throws Exception {
    Schema kindInListSchema = kindSchema().getField(
        "list_value").schema().getTypes().get(1)
        .getField("values").schema().getTypes().get(1)
        .getElementType().getField("kind_0").schema()
        .getTypes().get(1);

    Schema valueInListSchema = kindSchema().getField(
        "list_value").schema().getTypes().get(1)
        .getField("values").schema().getTypes().get(1)
        .getElementType();

    Schema listValueSchema = kindSchema().getField(
        "list_value").schema().getTypes().get(1);

    GenericRecord k1 =
        new GenericData.Record(kindInListSchema);
    k1.put("null_value", null);
    k1.put("number_value", null);
    k1.put("string_value", "fruit");
    k1.put("bool_value", null);

    GenericRecord v1 =
        new GenericData.Record(valueInListSchema);
    v1.put("kind_0", k1);

    GenericRecord k2 =
        new GenericData.Record(kindInListSchema);
    k2.put("null_value", null);
    k2.put("number_value", null);
    k2.put("string_value", "fresh");
    k2.put("bool_value", null);

    GenericRecord v2 =
        new GenericData.Record(valueInListSchema);
    v2.put("kind_0", k2);

    GenericRecord listValue =
        new GenericData.Record(listValueSchema);
    listValue.put("values", Arrays.asList(v1, v2));

    GenericRecord kind = makeKind(null, null, null, null);
    kind.put("list_value", listValue);

    GenericRecord struct = makeStruct(
        makeField("tags", makeValue(kind))
    );

    String json =
        AvroValueToJsonConverter.toJsonString(struct);
    JsonNode node = MAPPER.readTree(json);

    assertTrue(node.get("tags").isArray());
    assertEquals(2, node.get("tags").size());
    assertEquals("fruit", node.get("tags").get(0).asText());
    assertEquals("fresh", node.get("tags").get(1).asText());
  }

  @Test
  public void testUnwrapNestedStruct() throws Exception {
    Schema nestedKind = kindSchema().getField("struct_value")
        .schema().getTypes().get(1).getField("fields")
        .schema().getTypes().get(1).getElementType()
        .getField("value").schema().getTypes().get(1)
        .getField("kind_0").schema().getTypes().get(1);

    Schema nestedValue = kindSchema().getField("struct_value")
        .schema().getTypes().get(1).getField("fields")
        .schema().getTypes().get(1).getElementType()
        .getField("value").schema().getTypes().get(1);

    Schema nestedField = kindSchema().getField("struct_value")
        .schema().getTypes().get(1).getField("fields")
        .schema().getTypes().get(1).getElementType();

    Schema nestedStruct = kindSchema().getField(
        "struct_value").schema().getTypes().get(1);

    GenericRecord innerKind =
        new GenericData.Record(nestedKind);
    innerKind.put("null_value", null);
    innerKind.put("number_value", null);
    innerKind.put("string_value", "Bangalore");
    innerKind.put("bool_value", null);

    GenericRecord innerVal =
        new GenericData.Record(nestedValue);
    innerVal.put("kind_0", innerKind);

    GenericRecord innerFld =
        new GenericData.Record(nestedField);
    innerFld.put("key", "city");
    innerFld.put("value", innerVal);

    GenericRecord innerStruct =
        new GenericData.Record(nestedStruct);
    innerStruct.put("fields",
        Arrays.asList(innerFld));

    GenericRecord kind = makeKind(null, null, null, null);
    kind.put("struct_value", innerStruct);

    GenericRecord struct = makeStruct(
        makeField("location", makeValue(kind))
    );

    String json =
        AvroValueToJsonConverter.toJsonString(struct);
    JsonNode node = MAPPER.readTree(json);

    assertTrue(node.get("location").isObject());
    assertEquals("Bangalore",
        node.get("location").get("city").asText());
  }

  @Test
  public void testGenericRecordNotUnwrapped()
      throws Exception {
    Schema schema = SchemaBuilder.record("Plain").fields()
        .requiredString("id")
        .requiredInt("count")
        .endRecord();

    GenericRecord record =
        new GenericData.Record(schema);
    record.put("id", "abc");
    record.put("count", 42);

    String json =
        AvroValueToJsonConverter.toJsonString(record);
    JsonNode node = MAPPER.readTree(json);

    assertEquals("abc", node.get("id").asText());
    assertEquals(42, node.get("count").asInt());
  }

  @Test
  public void testMapAsArrayUnwrapped() throws Exception {
    Schema entrySchema =
        SchemaBuilder.record("MapEntry").fields()
            .requiredString("key")
            .requiredString("value")
            .endRecord();

    GenericRecord e1 =
        new GenericData.Record(entrySchema);
    e1.put("key", "device_context");
    e1.put("value", "ios");

    GenericRecord e2 =
        new GenericData.Record(entrySchema);
    e2.put("key", "experiment");
    e2.put("value", "B");

    String json = AvroValueToJsonConverter.toJsonString(
        Arrays.asList(e1, e2));
    JsonNode node = MAPPER.readTree(json);

    assertTrue("Should be object", node.isObject());
    assertEquals("ios",
        node.get("device_context").asText());
    assertEquals("B", node.get("experiment").asText());
  }

  @Test
  public void testSingleFieldWrapperUnwrapped()
      throws Exception {
    Schema innerSchema =
        SchemaBuilder.record("InnerStruct").fields()
            .requiredString("app_version")
            .requiredString("platform")
            .endRecord();

    Schema wrapperSchema =
        SchemaBuilder.record("StructWrapper").fields()
            .name("struct").type(innerSchema).noDefault()
            .endRecord();

    GenericRecord inner =
        new GenericData.Record(innerSchema);
    inner.put("app_version", "7.12.0");
    inner.put("platform", "ios");

    GenericRecord wrapper =
        new GenericData.Record(wrapperSchema);
    wrapper.put("struct", inner);

    String json =
        AvroValueToJsonConverter.toJsonString(wrapper);
    JsonNode node = MAPPER.readTree(json);

    assertEquals("7.12.0",
        node.get("app_version").asText());
    assertEquals("ios", node.get("platform").asText());
  }

  @Test
  public void testMapAsArrayWithStructWrapperValues()
      throws Exception {
    Schema innerSchema =
        SchemaBuilder.record("Data").fields()
            .requiredString("device_id")
            .requiredString("platform")
            .endRecord();

    Schema wrapperSchema =
        SchemaBuilder.record("SW").fields()
            .name("struct").type(innerSchema).noDefault()
            .endRecord();

    Schema entrySchema =
        SchemaBuilder.record("ME").fields()
            .requiredString("key")
            .name("value").type(wrapperSchema).noDefault()
            .endRecord();

    GenericRecord inner =
        new GenericData.Record(innerSchema);
    inner.put("device_id", "d789");
    inner.put("platform", "ios");

    GenericRecord wrapper =
        new GenericData.Record(wrapperSchema);
    wrapper.put("struct", inner);

    GenericRecord entry =
        new GenericData.Record(entrySchema);
    entry.put("key", "device_context");
    entry.put("value", wrapper);

    String json = AvroValueToJsonConverter.toJsonString(
        Arrays.asList(entry));
    JsonNode node = MAPPER.readTree(json);

    assertTrue("Should be object", node.isObject());
    JsonNode dc = node.get("device_context");
    assertEquals("d789", dc.get("device_id").asText());
    assertEquals("ios", dc.get("platform").asText());
  }
}
