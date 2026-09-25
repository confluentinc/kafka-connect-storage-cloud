/*
 * Copyright 2025 Confluent Inc.
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

package io.confluent.connect.s3.format.json;

import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.storage.S3Storage;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.json.JsonConverter;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JsonFormatTest {

  private static final int CACHE_SIZE = 1000;
  private static final String DECIMAL_FORMAT = "BASE64";
  private static final String TEST_TOPIC = "t";
  private static final String TEST_VALUE = "hello";
  private static final String SCHEMA_JSON_PREFIX = "{\"schema\"";

  private S3Storage storage;
  private S3SinkConnectorConfig config;

  @Before
  public void setUp() {
    storage = mock(S3Storage.class);
    config = mock(S3SinkConnectorConfig.class);
    when(storage.conf()).thenReturn(config);
    when(config.get(S3SinkConnectorConfig.SCHEMA_CACHE_SIZE_CONFIG)).thenReturn(CACHE_SIZE);
    when(config.getJsonDecimalFormat()).thenReturn(DECIMAL_FORMAT);
  }

  @Test
  public void testConstructorEnablesSchemasWhenJsonSchemaEmbedded() throws Exception {
    when(config.isJsonSchemaEmbedded()).thenReturn(true);

    JsonFormat format = new JsonFormat(storage);

    assertTrue("with schemas.enable=true the JsonConverter emits a {schema,payload} envelope",
        fromConnectDataHasSchemaEnvelope(format));
    assertNotNull(format.getRecordWriterProvider());
  }

  @Test
  public void testConstructorDisablesSchemasWhenJsonSchemaNotEmbedded() throws Exception {
    when(config.isJsonSchemaEmbedded()).thenReturn(false);

    JsonFormat format = new JsonFormat(storage);

    assertFalse("with schemas.enable=false the JsonConverter emits just the payload",
        fromConnectDataHasSchemaEnvelope(format));
  }

  @Test
  public void testGetSchemaFileReaderIsUnsupported() {
    when(config.isJsonSchemaEmbedded()).thenReturn(false);
    JsonFormat format = new JsonFormat(storage);
    try {
      format.getSchemaFileReader();
      fail("expected UnsupportedOperationException");
    } catch (UnsupportedOperationException expected) {
      assertTrue(expected.getMessage().contains("Reading schemas from S3"));
    }
  }

  @Test
  public void testGetHiveFactoryIsUnsupported() {
    when(config.isJsonSchemaEmbedded()).thenReturn(false);
    JsonFormat format = new JsonFormat(storage);
    try {
      format.getHiveFactory();
      fail("expected UnsupportedOperationException");
    } catch (UnsupportedOperationException expected) {
      assertTrue(expected.getMessage().contains("Hive integration"));
    }
  }

  // schemas.enable=true → {"schema":...,"payload":...}; false → payload only.
  private static boolean fromConnectDataHasSchemaEnvelope(JsonFormat format) throws Exception {
    Field cfield = JsonFormat.class.getDeclaredField("converter");
    cfield.setAccessible(true);
    JsonConverter converter = (JsonConverter) cfield.get(format);
    byte[] out = converter.fromConnectData(TEST_TOPIC, Schema.STRING_SCHEMA, TEST_VALUE);
    return new String(out, StandardCharsets.UTF_8).startsWith(SCHEMA_JSON_PREFIX);
  }
}
