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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.exc.InputCoercionException;
import org.apache.parquet.variant.Variant;
import org.apache.parquet.variant.VariantArrayBuilder;
import org.apache.parquet.variant.VariantBuilder;
import org.apache.parquet.variant.VariantObjectBuilder;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * Converts JSON strings into Parquet Variant binary encoding using
 * Jackson streaming parser and parquet-variant's
 * {@link VariantBuilder}.
 *
 * <p>Ported from Apache Spark's {@code VariantBuilder.buildJson}
 * (common/variant module), adapted to use parquet-variant's
 * builder API instead of Spark's internal builder. This uses
 * single-pass streaming (no intermediate {@code JsonNode} tree),
 * matching Spark's production-proven parsing logic.
 *
 * <p>Number handling follows Spark's approach:
 * <ul>
 *   <li>Integers use the smallest fitting type</li>
 *   <li>Floats prefer decimal when possible (no scientific
 *       notation), falling back to double</li>
 *   <li>Integer overflow falls back to decimal/double</li>
 * </ul>
 *
 * @see <a href="https://github.com/apache/spark/blob/master/common/variant/src/main/java/org/apache/spark/types/variant/VariantBuilder.java">
 *   Spark VariantBuilder</a>
 */
public class JsonToVariantConverter {

  private static final JsonFactory JSON_FACTORY =
      new JsonFactory();

  private static final int MAX_DECIMAL16_PRECISION = 38;

  private JsonToVariantConverter() {
  }

  /**
   * Convert a JSON string to a Parquet {@link Variant}.
   *
   * @param json the JSON string; may be null
   * @return the Variant encoding, or null if input is null
   * @throws IOException if the JSON is malformed
   */
  public static Variant convert(String json)
      throws IOException {
    if (json == null) {
      return null;
    }
    try (JsonParser parser =
             JSON_FACTORY.createParser(json)) {
      parser.nextToken();
      VariantBuilder builder = new VariantBuilder();
      buildJson(builder, parser);
      return builder.build();
    }
  }

  private static void buildJson(
      VariantBuilder builder, JsonParser parser
  ) throws IOException {
    JsonToken token = parser.currentToken();
    if (token == null) {
      throw new JsonParseException(
          parser, "Unexpected null token");
    }
    switch (token) {
      case START_OBJECT:
        buildObject(builder, parser);
        break;
      case START_ARRAY:
        buildArray(builder, parser);
        break;
      case VALUE_STRING:
        builder.appendString(parser.getText());
        break;
      case VALUE_NUMBER_INT:
        handleInteger(builder, parser);
        break;
      case VALUE_NUMBER_FLOAT:
        handleFloat(builder, parser);
        break;
      case VALUE_TRUE:
        builder.appendBoolean(true);
        break;
      case VALUE_FALSE:
        builder.appendBoolean(false);
        break;
      case VALUE_NULL:
        builder.appendNull();
        break;
      default:
        throw new JsonParseException(
            parser, "Unexpected token " + token);
    }
  }

  private static void buildObject(
      VariantBuilder builder, JsonParser parser
  ) throws IOException {
    VariantObjectBuilder obj = builder.startObject();
    while (parser.nextToken() != JsonToken.END_OBJECT) {
      obj.appendKey(parser.currentName());
      parser.nextToken();
      buildJson(obj, parser);
    }
    builder.endObject();
  }

  private static void buildArray(
      VariantBuilder builder, JsonParser parser
  ) throws IOException {
    VariantArrayBuilder arr = builder.startArray();
    while (parser.nextToken() != JsonToken.END_ARRAY) {
      buildJson(arr, parser);
    }
    builder.endArray();
  }

  private static void handleInteger(
      VariantBuilder builder, JsonParser parser
  ) throws IOException {
    try {
      builder.appendLong(parser.getLongValue());
    } catch (InputCoercionException ignored) {
      handleFloat(builder, parser);
    }
  }

  private static void handleFloat(
      VariantBuilder builder, JsonParser parser
  ) throws IOException {
    if (!tryAppendDecimal(builder, parser.getText())) {
      builder.appendDouble(parser.getDoubleValue());
    }
  }

  /**
   * Try to parse a number as a decimal. Returns true if
   * successful. Only accepts plain decimal format (no
   * scientific notation) that fits within DECIMAL16 precision.
   * Ported from Spark's {@code tryParseDecimal}.
   */
  private static boolean tryAppendDecimal(
      VariantBuilder builder, String input
  ) {
    for (int i = 0; i < input.length(); i++) {
      char ch = input.charAt(i);
      if (ch != '-' && ch != '.'
          && !(ch >= '0' && ch <= '9')) {
        return false;
      }
    }
    BigDecimal d = new BigDecimal(input);
    if (d.scale() <= MAX_DECIMAL16_PRECISION
        && d.precision() <= MAX_DECIMAL16_PRECISION) {
      builder.appendDecimal(d);
      return true;
    }
    return false;
  }
}
