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
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class VariantSchemaBuilderTest {

  @Test
  public void testTransformSimpleSchema() {
    MessageType original = Types.buildMessage()
        .optional(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType()).named("id")
        .optional(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType()).named("gst_details")
        .optional(PrimitiveType.PrimitiveTypeName.INT32).named("count")
        .named("record");

    Set<String> variantPaths = new HashSet<>(Collections.singletonList("gst_details"));
    MessageType transformed = VariantSchemaBuilder.transformSchema(original, variantPaths);

    assertEquals(3, transformed.getFieldCount());

    Type idField = transformed.getType("id");
    assertTrue("id should remain primitive", idField.isPrimitive());

    Type gstField = transformed.getType("gst_details");
    assertFalse("gst_details should become a group", gstField.isPrimitive());
    assertTrue("gst_details should have VARIANT annotation",
        gstField.getLogicalTypeAnnotation()
            instanceof LogicalTypeAnnotation.VariantLogicalTypeAnnotation);

    GroupType variantGroup = gstField.asGroupType();
    assertEquals(2, variantGroup.getFieldCount());
    assertEquals("metadata", variantGroup.getType(0).getName());
    assertEquals("value", variantGroup.getType(1).getName());

    Type countField = transformed.getType("count");
    assertTrue("count should remain primitive", countField.isPrimitive());
  }

  @Test
  public void testTransformNestedSchema() {
    MessageType original = Types.buildMessage()
        .optionalGroup()
            .optional(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType()).named("id")
            .optional(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType()).named("gst_details")
            .named("after")
        .optional(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType()).named("op")
        .named("envelope");

    Set<String> variantPaths = new HashSet<>(Collections.singletonList("after.gst_details"));
    MessageType transformed = VariantSchemaBuilder.transformSchema(original, variantPaths);

    GroupType afterGroup = transformed.getType("after").asGroupType();
    Type idField = afterGroup.getType("id");
    assertTrue("id should remain primitive", idField.isPrimitive());

    Type gstField = afterGroup.getType("gst_details");
    assertFalse("nested gst_details should become a group", gstField.isPrimitive());
    assertTrue("nested gst_details should have VARIANT annotation",
        gstField.getLogicalTypeAnnotation()
            instanceof LogicalTypeAnnotation.VariantLogicalTypeAnnotation);
  }

  @Test
  public void testEmptyVariantPaths() {
    MessageType original = Types.buildMessage()
        .optional(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType()).named("id")
        .named("record");

    MessageType transformed = VariantSchemaBuilder.transformSchema(
        original, Collections.emptySet()
    );

    assertEquals(original.toString(), transformed.toString());
  }

  @Test
  public void testNullVariantPaths() {
    MessageType original = Types.buildMessage()
        .optional(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType()).named("id")
        .named("record");

    MessageType transformed = VariantSchemaBuilder.transformSchema(original, null);
    assertEquals(original.toString(), transformed.toString());
  }

  @Test
  public void testMultipleVariantFields() {
    MessageType original = Types.buildMessage()
        .optional(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType()).named("gst_details")
        .optional(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType()).named("other_fee_details")
        .optional(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType()).named("fees_discount")
        .named("record");

    Set<String> variantPaths = new HashSet<>(
        Arrays.asList("gst_details", "other_fee_details", "fees_discount")
    );
    MessageType transformed = VariantSchemaBuilder.transformSchema(original, variantPaths);

    for (String path : variantPaths) {
      Type field = transformed.getType(path);
      assertFalse(path + " should be a group", field.isPrimitive());
      assertNotNull(path + " should have VARIANT annotation",
          field.getLogicalTypeAnnotation());
    }
  }

  @Test
  public void testBuildVariantGroup() {
    GroupType variant = VariantSchemaBuilder.buildVariantGroup(
        "test_field", Type.Repetition.OPTIONAL
    );

    assertEquals("test_field", variant.getName());
    assertEquals(Type.Repetition.OPTIONAL, variant.getRepetition());
    assertTrue(variant.getLogicalTypeAnnotation()
        instanceof LogicalTypeAnnotation.VariantLogicalTypeAnnotation);
    assertEquals(2, variant.getFieldCount());
    assertEquals("metadata", variant.getType(0).getName());
    assertEquals(Type.Repetition.REQUIRED, variant.getType(0).getRepetition());
    assertEquals("value", variant.getType(1).getName());
    assertEquals(Type.Repetition.REQUIRED, variant.getType(1).getRepetition());
  }
}
