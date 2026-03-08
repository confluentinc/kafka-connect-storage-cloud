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

import org.apache.parquet.variant.Variant;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class JsonToVariantConverterTest {

  @Test
  public void testNullInput() throws IOException {
    assertNull(JsonToVariantConverter.convert(null));
  }

  @Test
  public void testSimpleString() throws IOException {
    Variant variant = JsonToVariantConverter.convert("\"hello\"");
    assertNotNull(variant);
    assertEquals(Variant.Type.STRING, variant.getType());
    assertEquals("hello", variant.getString());
  }

  @Test
  public void testInteger() throws IOException {
    Variant variant = JsonToVariantConverter.convert("42");
    assertNotNull(variant);
    assertEquals(42, variant.getLong());
  }

  @Test
  public void testLong() throws IOException {
    Variant variant = JsonToVariantConverter.convert("9999999999");
    assertNotNull(variant);
    assertEquals(9999999999L, variant.getLong());
  }

  @Test
  public void testDecimalFloat() throws IOException {
    Variant variant = JsonToVariantConverter.convert("3.14");
    assertNotNull(variant);
    Variant.Type type = variant.getType();
    assertTrue(
        "Expected DECIMAL4/8/16, got " + type,
        type == Variant.Type.DECIMAL4
            || type == Variant.Type.DECIMAL8
            || type == Variant.Type.DECIMAL16
    );
    assertEquals(
        0,
        new BigDecimal("3.14").compareTo(variant.getDecimal())
    );
  }

  @Test
  public void testScientificNotationDouble() throws IOException {
    Variant variant = JsonToVariantConverter.convert("3.14e2");
    assertNotNull(variant);
    assertEquals(Variant.Type.DOUBLE, variant.getType());
    assertEquals(314.0, variant.getDouble(), 0.001);
  }

  @Test
  public void testBoolean() throws IOException {
    Variant variant = JsonToVariantConverter.convert("true");
    assertNotNull(variant);
    assertEquals(Variant.Type.BOOLEAN, variant.getType());
    assertEquals(true, variant.getBoolean());
  }

  @Test
  public void testNull() throws IOException {
    Variant variant = JsonToVariantConverter.convert("null");
    assertNotNull(variant);
    assertEquals(Variant.Type.NULL, variant.getType());
  }

  @Test
  public void testSimpleObject() throws IOException {
    String json = "{\"name\":\"John\",\"age\":30}";
    Variant variant = JsonToVariantConverter.convert(json);
    assertNotNull(variant);
    assertEquals(Variant.Type.OBJECT, variant.getType());
    assertEquals(2, variant.numObjectElements());
    assertEquals("John",
        variant.getFieldByKey("name").getString());
    assertEquals(30,
        variant.getFieldByKey("age").getLong());
  }

  @Test
  public void testNestedObject() throws IOException {
    String json =
        "{\"user\":{\"id\":100,\"country\":\"US\"},"
            + "\"active\":true}";
    Variant variant = JsonToVariantConverter.convert(json);
    assertNotNull(variant);
    assertEquals(Variant.Type.OBJECT, variant.getType());

    Variant user = variant.getFieldByKey("user");
    assertNotNull(user);
    assertEquals(Variant.Type.OBJECT, user.getType());
    assertEquals(100, user.getFieldByKey("id").getLong());
    assertEquals("US",
        user.getFieldByKey("country").getString());

    assertEquals(true,
        variant.getFieldByKey("active").getBoolean());
  }

  @Test
  public void testArray() throws IOException {
    String json = "[1,2,3,\"four\"]";
    Variant variant = JsonToVariantConverter.convert(json);
    assertNotNull(variant);
    assertEquals(Variant.Type.ARRAY, variant.getType());
    assertEquals(4, variant.numArrayElements());
    assertEquals(1, variant.getElementAtIndex(0).getLong());
    assertEquals(2, variant.getElementAtIndex(1).getLong());
    assertEquals(3, variant.getElementAtIndex(2).getLong());
    assertEquals("four",
        variant.getElementAtIndex(3).getString());
  }

  @Test
  public void testGstDetailsLikePayload() throws IOException {
    String json =
        "{\"cgst\":9.0,\"sgst\":9.0,\"igst\":0,"
            + "\"total_gst\":18.0,\"hsn_code\":\"9961\"}";
    Variant variant = JsonToVariantConverter.convert(json);
    assertNotNull(variant);
    assertEquals(Variant.Type.OBJECT, variant.getType());
    assertEquals(0,
        new BigDecimal("9.0").compareTo(
            variant.getFieldByKey("cgst").getDecimal()));
    assertEquals("9961",
        variant.getFieldByKey("hsn_code").getString());
  }

  @Test
  public void testComplexNestedPayload() throws IOException {
    String json =
        "{\"fees\":[{\"type\":\"delivery\",\"amount\":50},"
            + "{\"type\":\"packaging\",\"amount\":10}],"
            + "\"currency\":\"INR\"}";
    Variant variant = JsonToVariantConverter.convert(json);
    assertNotNull(variant);
    assertEquals(Variant.Type.OBJECT, variant.getType());

    Variant fees = variant.getFieldByKey("fees");
    assertEquals(Variant.Type.ARRAY, fees.getType());
    assertEquals(2, fees.numArrayElements());

    Variant firstFee = fees.getElementAtIndex(0);
    assertEquals("delivery",
        firstFee.getFieldByKey("type").getString());
    assertEquals(50,
        firstFee.getFieldByKey("amount").getLong());
  }

  @Test
  public void testObjectWithNullValues() throws IOException {
    String json = "{\"name\":\"test\",\"value\":null}";
    Variant variant = JsonToVariantConverter.convert(json);
    assertNotNull(variant);
    assertEquals(Variant.Type.OBJECT, variant.getType());
    assertEquals(Variant.Type.NULL,
        variant.getFieldByKey("value").getType());
  }

  @Test(expected = IOException.class)
  public void testMalformedJson() throws IOException {
    JsonToVariantConverter.convert("{invalid json");
  }

  @Test
  public void testIntegerOverflowToDecimal() throws IOException {
    String bigNum = "99999999999999999999";
    Variant variant = JsonToVariantConverter.convert(bigNum);
    assertNotNull(variant);
    Variant.Type type = variant.getType();
    assertTrue(
        "Expected DECIMAL type for big integer, got " + type,
        type == Variant.Type.DECIMAL4
            || type == Variant.Type.DECIMAL8
            || type == Variant.Type.DECIMAL16
    );
    assertEquals(0,
        new BigDecimal(bigNum).compareTo(
            variant.getDecimal()));
  }
}
