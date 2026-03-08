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
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JsonFieldDetectorTest {

  @Test
  public void testDetectsDebeziumJsonFields() {
    Schema jsonFieldSchema = SchemaBuilder.string()
        .name("io.debezium.data.Json")
        .optional()
        .build();

    Schema recordSchema = SchemaBuilder.struct()
        .name("test.Value")
        .field("id", Schema.STRING_SCHEMA)
        .field("gst_details", jsonFieldSchema)
        .field("other_fee_details", jsonFieldSchema)
        .field("vendor_name", Schema.OPTIONAL_STRING_SCHEMA)
        .build();

    JsonFieldDetector detector = new JsonFieldDetector(
        Arrays.asList("io.debezium.data.Json"),
        Collections.emptySet()
    );
    Set<String> paths = detector.detect(recordSchema);

    assertEquals(2, paths.size());
    assertTrue(paths.contains("gst_details"));
    assertTrue(paths.contains("other_fee_details"));
  }

  @Test
  public void testDetectsNestedJsonFieldsInDebeziumEnvelope() {
    Schema jsonFieldSchema = SchemaBuilder.string()
        .name("io.debezium.data.Json")
        .optional()
        .build();

    Schema valueSchema = SchemaBuilder.struct()
        .name("de_pg_oms.public.order.Value")
        .field("id", Schema.STRING_SCHEMA)
        .field("gst_details", jsonFieldSchema)
        .field("fees_external_coupon_discount", jsonFieldSchema)
        .field("vendor_name", Schema.OPTIONAL_STRING_SCHEMA)
        .build();

    Schema envelopeSchema = SchemaBuilder.struct()
        .name("de_pg_oms.public.order.Envelope")
        .field("before", SchemaBuilder.struct()
            .name("before_wrapper")
            .field("id", Schema.STRING_SCHEMA)
            .field("gst_details", jsonFieldSchema)
            .build())
        .field("after", SchemaBuilder.struct()
            .name("after_wrapper")
            .field("id", Schema.STRING_SCHEMA)
            .field("gst_details", jsonFieldSchema)
            .field("fees_external_coupon_discount", jsonFieldSchema)
            .build())
        .field("op", Schema.STRING_SCHEMA)
        .build();

    JsonFieldDetector detector = new JsonFieldDetector(
        Arrays.asList("io.debezium.data.Json"),
        Collections.emptySet()
    );
    Set<String> paths = detector.detect(envelopeSchema);

    assertEquals(3, paths.size());
    assertTrue(paths.contains("before.gst_details"));
    assertTrue(paths.contains("after.gst_details"));
    assertTrue(paths.contains("after.fees_external_coupon_discount"));
  }

  @Test
  public void testDetectsExplicitFieldNames() {
    Schema recordSchema = SchemaBuilder.struct()
        .field("id", Schema.STRING_SCHEMA)
        .field("metadata_json", Schema.OPTIONAL_STRING_SCHEMA)
        .field("other_data", Schema.INT32_SCHEMA)
        .build();

    JsonFieldDetector detector = new JsonFieldDetector(
        Collections.emptyList(),
        new HashSet<>(Arrays.asList("metadata_json"))
    );
    Set<String> paths = detector.detect(recordSchema);

    assertEquals(1, paths.size());
    assertTrue(paths.contains("metadata_json"));
  }

  @Test
  public void testIgnoresUnmatchedPrimitiveFields() {
    Schema recordSchema = SchemaBuilder.struct()
        .field("id", Schema.INT32_SCHEMA)
        .field("count", Schema.INT64_SCHEMA)
        .field("active", Schema.BOOLEAN_SCHEMA)
        .build();

    JsonFieldDetector detector = new JsonFieldDetector(
        Arrays.asList("io.debezium.data.Json"),
        Collections.emptySet()
    );
    Set<String> paths = detector.detect(recordSchema);

    assertTrue(paths.isEmpty());
  }

  @Test
  public void testExplicitFieldNameMatchesAnyType() {
    Schema innerStruct = SchemaBuilder.struct()
        .name("test.Inner")
        .field("key", Schema.STRING_SCHEMA)
        .build();

    Schema recordSchema = SchemaBuilder.struct()
        .field("id", Schema.INT32_SCHEMA)
        .field("remote_config", innerStruct)
        .field("model_response", Schema.STRING_SCHEMA)
        .build();

    JsonFieldDetector detector = new JsonFieldDetector(
        Collections.emptyList(),
        new HashSet<>(Arrays.asList(
            "remote_config", "model_response"))
    );
    Set<String> paths = detector.detect(recordSchema);

    assertEquals(2, paths.size());
    assertTrue(paths.contains("remote_config"));
    assertTrue(paths.contains("model_response"));
  }

  @Test
  public void testDetectsMapFieldByName() {
    Schema mapSchema = SchemaBuilder.map(
        Schema.STRING_SCHEMA,
        Schema.STRING_SCHEMA
    ).optional().build();

    Schema recordSchema = SchemaBuilder.struct()
        .field("id", Schema.STRING_SCHEMA)
        .field("deliverability_json", mapSchema)
        .build();

    JsonFieldDetector detector = new JsonFieldDetector(
        Collections.emptyList(),
        new HashSet<>(Arrays.asList("deliverability_json"))
    );
    Set<String> paths = detector.detect(recordSchema);

    assertEquals(1, paths.size());
    assertTrue(paths.contains("deliverability_json"));
  }

  @Test
  public void testDetectsArrayFieldByName() {
    Schema innerStruct = SchemaBuilder.struct()
        .name("test.Detail")
        .field("key", Schema.STRING_SCHEMA)
        .build();

    Schema arraySchema = SchemaBuilder.array(innerStruct)
        .optional().build();

    Schema recordSchema = SchemaBuilder.struct()
        .field("id", Schema.STRING_SCHEMA)
        .field("details", arraySchema)
        .build();

    JsonFieldDetector detector = new JsonFieldDetector(
        Collections.emptyList(),
        new HashSet<>(Arrays.asList("details"))
    );
    Set<String> paths = detector.detect(recordSchema);

    assertEquals(1, paths.size());
    assertTrue(paths.contains("details"));
  }

  @Test
  public void testDetectsStructFieldByConnectName() {
    Schema namedStruct = SchemaBuilder.struct()
        .name("custom.JsonStruct")
        .field("data", Schema.STRING_SCHEMA)
        .optional()
        .build();

    Schema recordSchema = SchemaBuilder.struct()
        .field("id", Schema.STRING_SCHEMA)
        .field("meta", namedStruct)
        .build();

    JsonFieldDetector detector = new JsonFieldDetector(
        Arrays.asList("custom.JsonStruct"),
        Collections.emptySet()
    );
    Set<String> paths = detector.detect(recordSchema);

    assertEquals(1, paths.size());
    assertTrue(paths.contains("meta"));
  }

  @Test
  public void testNullSchemaReturnsEmpty() {
    JsonFieldDetector detector = new JsonFieldDetector(
        Arrays.asList("io.debezium.data.Json"),
        Collections.emptySet()
    );
    Set<String> paths = detector.detect(null);
    assertTrue(paths.isEmpty());
  }

  @Test
  public void testAutoDetectsRecursiveProtobufStruct() {
    Schema valueSchema = SchemaBuilder.struct()
        .name("google.protobuf.Value")
        .field("number_value",
            Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field("string_value",
            Schema.OPTIONAL_STRING_SCHEMA)
        .field("bool_value",
            Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .build();

    Schema structSchema = SchemaBuilder.map(
        Schema.STRING_SCHEMA, valueSchema
    ).optional().build();

    // Wire up the circular reference: Value -> struct_value -> Struct -> Value
    // Kafka Connect's SchemaBuilder won't create true cycles, so
    // we simulate with the same object identity.
    // In practice, Confluent's ProtobufConverter produces schemas
    // with real object-identity cycles.
    Schema recordSchema = SchemaBuilder.struct()
        .name("test.Cart")
        .field("user_id", Schema.STRING_SCHEMA)
        .field("pass_detail", structSchema)
        .field("city", Schema.STRING_SCHEMA)
        .build();

    JsonFieldDetector detector = new JsonFieldDetector(
        Collections.emptyList(),
        Collections.emptySet()
    );
    Set<String> paths = detector.detect(recordSchema);

    // MAP<STRING, STRUCT> is not inherently recursive with
    // SchemaBuilder (no true cycle), so this tests the walkSchema
    // path. True recursive detection tested separately below.
    assertTrue("Non-recursive MAP should not auto-detect",
        paths.isEmpty());
  }

  @Test
  public void testIsRecursiveSchemaDetectsDirectCycle() {
    java.util.List<Field> selfRefFields =
        new java.util.ArrayList<>();
    selfRefFields.add(
        new Field("name", 0, Schema.STRING_SCHEMA));
    org.apache.kafka.connect.data.ConnectSchema selfRef =
        new org.apache.kafka.connect.data.ConnectSchema(
            Schema.Type.STRUCT,
            true, null, "SelfRef", null, null,
            null, selfRefFields, null, null
        );
    selfRefFields.add(new Field("child", 1, selfRef));

    assertTrue("Direct self-reference should be recursive",
        JsonFieldDetector.isRecursiveSchema(selfRef));
  }

  @Test
  public void testIsRecursiveSchemaReturnsFalseForFlat() {
    Schema flat = SchemaBuilder.struct()
        .name("test.Flat")
        .field("id", Schema.STRING_SCHEMA)
        .field("count", Schema.INT32_SCHEMA)
        .build();
    assertFalse("Flat schema should not be recursive",
        JsonFieldDetector.isRecursiveSchema(flat));
  }

  @Test
  public void testIsRecursiveSchemaReturnsFalseForNestedNonCyclic() {
    Schema inner = SchemaBuilder.struct()
        .name("test.Inner")
        .field("val", Schema.STRING_SCHEMA)
        .build();
    Schema outer = SchemaBuilder.struct()
        .name("test.Outer")
        .field("data", inner)
        .field("items",
            SchemaBuilder.array(inner).build())
        .build();
    assertFalse("Nested non-cyclic should not be recursive",
        JsonFieldDetector.isRecursiveSchema(outer));
  }

  @Test
  public void testAutoDetectsRecursiveFieldInWalk() {
    java.util.List<Field> fields = new java.util.ArrayList<>();
    fields.add(
        new Field("name", 0, Schema.STRING_SCHEMA));
    org.apache.kafka.connect.data.ConnectSchema recursive =
        new org.apache.kafka.connect.data.ConnectSchema(
            Schema.Type.STRUCT,
            true, null, "Recursive", null, null,
            null, fields, null, null
        );
    fields.add(new Field("child", 1, recursive));

    Schema root = SchemaBuilder.struct()
        .name("test.Root")
        .field("id", Schema.STRING_SCHEMA)
        .field("payload", recursive)
        .field("status", Schema.STRING_SCHEMA)
        .build();

    JsonFieldDetector detector = new JsonFieldDetector(
        Collections.emptyList(),
        Collections.emptySet()
    );
    Set<String> paths = detector.detect(root);

    assertEquals(1, paths.size());
    assertTrue("Recursive field auto-detected",
        paths.contains("payload"));
  }

  @Test
  public void testHybridAutoDetectPlusExplicitFieldNames() {
    // Strategy 3: recursive fields auto-detected + explicit
    // string field for model inference logs.
    java.util.List<Field> recFields =
        new java.util.ArrayList<>();
    recFields.add(
        new Field("val", 0, Schema.STRING_SCHEMA));
    org.apache.kafka.connect.data.ConnectSchema recursive =
        new org.apache.kafka.connect.data.ConnectSchema(
            Schema.Type.STRUCT,
            true, null, "ProtoStruct", null, null,
            null, recFields, null, null
        );
    recFields.add(new Field("nested", 1, recursive));

    Schema root = SchemaBuilder.struct()
        .name("test.Event")
        .field("user_id", Schema.STRING_SCHEMA)
        .field("fraud_actions", recursive)
        .field("inference_log",
            Schema.OPTIONAL_STRING_SCHEMA)
        .field("city", Schema.STRING_SCHEMA)
        .build();

    JsonFieldDetector detector = new JsonFieldDetector(
        Collections.emptyList(),
        new HashSet<>(
            Collections.singletonList("inference_log"))
    );
    Set<String> paths = detector.detect(root);

    assertEquals(2, paths.size());
    assertTrue("Recursive auto-detected",
        paths.contains("fraud_actions"));
    assertTrue("Explicit field matched",
        paths.contains("inference_log"));
  }
}
