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

import io.confluent.connect.avro.AvroData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class VariantAwareWriteSupportTest {

  private static final AvroData AVRO_DATA = new AvroData(100);

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testWriteWithVariantFields() throws IOException {
    Schema jsonFieldSchema = SchemaBuilder.string()
        .name("io.debezium.data.Json")
        .optional()
        .build();

    Schema connectSchema = SchemaBuilder.struct()
        .name("test.Record")
        .field("id", Schema.STRING_SCHEMA)
        .field("gst_details", jsonFieldSchema)
        .field("count", Schema.INT32_SCHEMA)
        .build();

    Struct record = new Struct(connectSchema)
        .put("id", "order-123")
        .put("gst_details", "{\"cgst\":9.0,\"sgst\":9.0}")
        .put("count", 5);

    org.apache.avro.Schema avroSchema =
        AVRO_DATA.fromConnectSchema(connectSchema);
    GenericRecord avroRecord = (GenericRecord) AVRO_DATA
        .fromConnectData(connectSchema, record);

    Set<String> variantPaths = new HashSet<>(
        Collections.singletonList("gst_details"));

    File outputFile = tempFolder.newFile("test.parquet");
    outputFile.delete();

    VariantAwareWriteSupport writeSupport =
        new VariantAwareWriteSupport(avroSchema, variantPaths, true);

    OutputFile out = new LocalOutputFile(outputFile.toPath());
    ParquetWriter<GenericRecord> writer =
        new TestWriterBuilder(out, writeSupport)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .build();

    writer.write(avroRecord);
    writer.close();

    assertTrue("Parquet file should exist", outputFile.exists());
    assertTrue("Parquet file should have content",
        outputFile.length() > 0);

    ParquetFileReader reader = ParquetFileReader.open(
        new LocalInputFile(outputFile.toPath()));
    MessageType parquetSchema =
        reader.getFooter().getFileMetaData().getSchema();
    reader.close();

    Type gstField = parquetSchema.getType("gst_details");
    assertNotNull("gst_details should exist in schema", gstField);
    assertFalse("gst_details should be a group type",
        gstField.isPrimitive());
    assertTrue("gst_details should have VARIANT annotation",
        gstField.getLogicalTypeAnnotation()
            instanceof LogicalTypeAnnotation.VariantLogicalTypeAnnotation);
  }

  @Test
  public void testWriteWithNestedVariantFields() throws IOException {
    Schema jsonFieldSchema = SchemaBuilder.string()
        .name("io.debezium.data.Json")
        .optional()
        .build();

    Schema valueSchema = SchemaBuilder.struct()
        .name("test.Value")
        .field("id", Schema.STRING_SCHEMA)
        .field("gst_details", jsonFieldSchema)
        .field("name", Schema.OPTIONAL_STRING_SCHEMA)
        .build();

    Schema envelopeSchema = SchemaBuilder.struct()
        .name("test.Envelope")
        .field("before", valueSchema)
        .field("after", valueSchema)
        .field("op", Schema.STRING_SCHEMA)
        .build();

    Struct beforeValue = new Struct(valueSchema)
        .put("id", "order-1")
        .put("gst_details", "{\"cgst\":4.5}")
        .put("name", "test");

    Struct afterValue = new Struct(valueSchema)
        .put("id", "order-1")
        .put("gst_details", "{\"cgst\":9.0,\"sgst\":9.0}")
        .put("name", "test");

    Struct envelope = new Struct(envelopeSchema)
        .put("before", beforeValue)
        .put("after", afterValue)
        .put("op", "u");

    org.apache.avro.Schema avroSchema =
        AVRO_DATA.fromConnectSchema(envelopeSchema);
    GenericRecord avroRecord = (GenericRecord) AVRO_DATA
        .fromConnectData(envelopeSchema, envelope);

    Set<String> variantPaths = new HashSet<>(
        Arrays.asList("before.gst_details", "after.gst_details"));

    File outputFile = tempFolder.newFile("test_nested.parquet");
    outputFile.delete();

    VariantAwareWriteSupport writeSupport =
        new VariantAwareWriteSupport(avroSchema, variantPaths, true);

    OutputFile out = new LocalOutputFile(outputFile.toPath());
    ParquetWriter<GenericRecord> writer =
        new TestWriterBuilder(out, writeSupport)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .build();

    writer.write(avroRecord);
    writer.close();

    ParquetFileReader reader = ParquetFileReader.open(
        new LocalInputFile(outputFile.toPath()));
    MessageType schema =
        reader.getFooter().getFileMetaData().getSchema();
    reader.close();

    Type beforeGst = schema.getType("before")
        .asGroupType().getType("gst_details");
    assertTrue("before.gst_details should have VARIANT annotation",
        beforeGst.getLogicalTypeAnnotation()
            instanceof LogicalTypeAnnotation.VariantLogicalTypeAnnotation);

    Type afterGst = schema.getType("after")
        .asGroupType().getType("gst_details");
    assertTrue("after.gst_details should have VARIANT annotation",
        afterGst.getLogicalTypeAnnotation()
            instanceof LogicalTypeAnnotation.VariantLogicalTypeAnnotation);
  }

  @Test
  public void testWriteWithNullVariantField() throws IOException {
    Schema jsonFieldSchema = SchemaBuilder.string()
        .name("io.debezium.data.Json")
        .optional()
        .build();

    Schema connectSchema = SchemaBuilder.struct()
        .name("test.Record")
        .field("id", Schema.STRING_SCHEMA)
        .field("gst_details", jsonFieldSchema)
        .build();

    Struct record = new Struct(connectSchema)
        .put("id", "order-456")
        .put("gst_details", null);

    org.apache.avro.Schema avroSchema =
        AVRO_DATA.fromConnectSchema(connectSchema);
    GenericRecord avroRecord = (GenericRecord) AVRO_DATA
        .fromConnectData(connectSchema, record);

    Set<String> variantPaths = new HashSet<>(
        Collections.singletonList("gst_details"));

    File outputFile = tempFolder.newFile("test_null.parquet");
    outputFile.delete();

    VariantAwareWriteSupport writeSupport =
        new VariantAwareWriteSupport(avroSchema, variantPaths, true);

    OutputFile out = new LocalOutputFile(outputFile.toPath());
    ParquetWriter<GenericRecord> writer =
        new TestWriterBuilder(out, writeSupport)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .build();

    writer.write(avroRecord);
    writer.close();

    assertTrue("Parquet file should exist", outputFile.exists());
    assertTrue("Parquet file should have content",
        outputFile.length() > 0);
  }

  @Test
  public void testWriteMultipleRecords() throws IOException {
    Schema jsonFieldSchema = SchemaBuilder.string()
        .name("io.debezium.data.Json")
        .optional()
        .build();

    Schema connectSchema = SchemaBuilder.struct()
        .name("test.Record")
        .field("id", Schema.STRING_SCHEMA)
        .field("gst_details", jsonFieldSchema)
        .field("count", Schema.INT32_SCHEMA)
        .build();

    org.apache.avro.Schema avroSchema =
        AVRO_DATA.fromConnectSchema(connectSchema);
    Set<String> variantPaths = new HashSet<>(
        Collections.singletonList("gst_details"));

    File outputFile = tempFolder.newFile("test_multi.parquet");
    outputFile.delete();

    VariantAwareWriteSupport writeSupport =
        new VariantAwareWriteSupport(avroSchema, variantPaths, true);

    OutputFile out = new LocalOutputFile(outputFile.toPath());
    ParquetWriter<GenericRecord> writer =
        new TestWriterBuilder(out, writeSupport)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .build();

    for (int i = 0; i < 10; i++) {
      Struct record = new Struct(connectSchema)
          .put("id", "order-" + i)
          .put("gst_details",
              "{\"cgst\":" + (i * 1.5) + ",\"sgst\":" + (i * 1.5) + "}")
          .put("count", i);
      GenericRecord avroRecord = (GenericRecord) AVRO_DATA
          .fromConnectData(connectSchema, record);
      writer.write(avroRecord);
    }
    writer.close();

    ParquetFileReader reader = ParquetFileReader.open(
        new LocalInputFile(outputFile.toPath()));
    long rowCount = reader.getRecordCount();
    reader.close();

    assertEquals("Should have written 10 records", 10, rowCount);
  }

  @Test
  public void testWriteWithStructVariantField() throws IOException {
    Schema innerSchema = SchemaBuilder.struct()
        .name("test.DeviceIntelligence")
        .field("is_jailbroken", Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .field("bundle_id", Schema.OPTIONAL_STRING_SCHEMA)
        .field("is_emulated", Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .build();

    Schema connectSchema = SchemaBuilder.struct()
        .name("test.Event")
        .field("user_id", Schema.STRING_SCHEMA)
        .field("device_intelligence", innerSchema)
        .build();

    Struct inner = new Struct(innerSchema)
        .put("is_jailbroken", false)
        .put("bundle_id", "com.app.test")
        .put("is_emulated", false);

    Struct record = new Struct(connectSchema)
        .put("user_id", "user-1")
        .put("device_intelligence", inner);

    org.apache.avro.Schema avroSchema =
        AVRO_DATA.fromConnectSchema(connectSchema);
    GenericRecord avroRecord = (GenericRecord) AVRO_DATA
        .fromConnectData(connectSchema, record);

    Set<String> variantPaths = new HashSet<>(
        Collections.singletonList("device_intelligence"));

    File outputFile = tempFolder.newFile("test_struct_variant.parquet");
    outputFile.delete();

    VariantAwareWriteSupport writeSupport =
        new VariantAwareWriteSupport(
            avroSchema, variantPaths, true);

    OutputFile out = new LocalOutputFile(outputFile.toPath());
    ParquetWriter<GenericRecord> writer =
        new TestWriterBuilder(out, writeSupport)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .build();

    writer.write(avroRecord);
    writer.close();

    assertTrue("Parquet file should exist", outputFile.exists());

    ParquetFileReader reader = ParquetFileReader.open(
        new LocalInputFile(outputFile.toPath()));
    MessageType schema =
        reader.getFooter().getFileMetaData().getSchema();
    reader.close();

    Type diField = schema.getType("device_intelligence");
    assertNotNull("device_intelligence should exist", diField);
    assertFalse("Should be a group", diField.isPrimitive());
    assertTrue("Should have VARIANT annotation",
        diField.getLogicalTypeAnnotation()
            instanceof
            LogicalTypeAnnotation.VariantLogicalTypeAnnotation);
  }

  @Test
  public void testWriteWithMapVariantField() throws IOException {
    Schema mapSchema = SchemaBuilder.map(
        Schema.STRING_SCHEMA,
        Schema.OPTIONAL_STRING_SCHEMA
    ).optional().build();

    Schema connectSchema = SchemaBuilder.struct()
        .name("test.Event")
        .field("user_id", Schema.STRING_SCHEMA)
        .field("remote_config", mapSchema)
        .build();

    Map<String, String> configMap = new HashMap<>();
    configMap.put("feature_a", "enabled");
    configMap.put("feature_b", "disabled");

    Struct record = new Struct(connectSchema)
        .put("user_id", "user-1")
        .put("remote_config", configMap);

    org.apache.avro.Schema avroSchema =
        AVRO_DATA.fromConnectSchema(connectSchema);
    GenericRecord avroRecord = (GenericRecord) AVRO_DATA
        .fromConnectData(connectSchema, record);

    Set<String> variantPaths = new HashSet<>(
        Collections.singletonList("remote_config"));

    File outputFile =
        tempFolder.newFile("test_map_variant.parquet");
    outputFile.delete();

    VariantAwareWriteSupport writeSupport =
        new VariantAwareWriteSupport(
            avroSchema, variantPaths, true);

    OutputFile out = new LocalOutputFile(outputFile.toPath());
    ParquetWriter<GenericRecord> writer =
        new TestWriterBuilder(out, writeSupport)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .build();

    writer.write(avroRecord);
    writer.close();

    assertTrue("Parquet file should exist", outputFile.exists());

    ParquetFileReader reader = ParquetFileReader.open(
        new LocalInputFile(outputFile.toPath()));
    MessageType schema =
        reader.getFooter().getFileMetaData().getSchema();
    reader.close();

    Type rcField = schema.getType("remote_config");
    assertNotNull("remote_config should exist", rcField);
    assertTrue("Should have VARIANT annotation",
        rcField.getLogicalTypeAnnotation()
            instanceof
            LogicalTypeAnnotation.VariantLogicalTypeAnnotation);
  }

  @Test
  public void testWriteWithArrayVariantField() throws IOException {
    Schema innerSchema = SchemaBuilder.struct()
        .name("test.Detail")
        .field("key", Schema.STRING_SCHEMA)
        .field("value", Schema.OPTIONAL_STRING_SCHEMA)
        .build();

    Schema arraySchema = SchemaBuilder.array(innerSchema)
        .optional().build();

    Schema connectSchema = SchemaBuilder.struct()
        .name("test.Event")
        .field("user_id", Schema.STRING_SCHEMA)
        .field("details", arraySchema)
        .build();

    Struct detail1 = new Struct(innerSchema)
        .put("key", "login").put("value", "success");
    Struct detail2 = new Struct(innerSchema)
        .put("key", "purchase").put("value", "99.99");

    Struct record = new Struct(connectSchema)
        .put("user_id", "user-1")
        .put("details", Arrays.asList(detail1, detail2));

    org.apache.avro.Schema avroSchema =
        AVRO_DATA.fromConnectSchema(connectSchema);
    GenericRecord avroRecord = (GenericRecord) AVRO_DATA
        .fromConnectData(connectSchema, record);

    Set<String> variantPaths = new HashSet<>(
        Collections.singletonList("details"));

    File outputFile =
        tempFolder.newFile("test_array_variant.parquet");
    outputFile.delete();

    VariantAwareWriteSupport writeSupport =
        new VariantAwareWriteSupport(
            avroSchema, variantPaths, true);

    OutputFile out = new LocalOutputFile(outputFile.toPath());
    ParquetWriter<GenericRecord> writer =
        new TestWriterBuilder(out, writeSupport)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .build();

    writer.write(avroRecord);
    writer.close();

    assertTrue("Parquet file should exist", outputFile.exists());

    ParquetFileReader reader = ParquetFileReader.open(
        new LocalInputFile(outputFile.toPath()));
    MessageType schema =
        reader.getFooter().getFileMetaData().getSchema();
    reader.close();

    Type dField = schema.getType("details");
    assertNotNull("details should exist", dField);
    assertTrue("Should have VARIANT annotation",
        dField.getLogicalTypeAnnotation()
            instanceof
            LogicalTypeAnnotation.VariantLogicalTypeAnnotation);
  }

  @Test
  public void testWriteMixedStringAndStructVariants()
      throws IOException {
    Schema jsonFieldSchema = SchemaBuilder.string()
        .name("io.debezium.data.Json")
        .optional()
        .build();

    Schema innerSchema = SchemaBuilder.struct()
        .name("test.Meta")
        .field("source", Schema.OPTIONAL_STRING_SCHEMA)
        .build();

    Schema connectSchema = SchemaBuilder.struct()
        .name("test.Record")
        .field("id", Schema.STRING_SCHEMA)
        .field("gst_details", jsonFieldSchema)
        .field("meta", innerSchema)
        .build();

    Struct meta = new Struct(innerSchema)
        .put("source", "web");

    Struct record = new Struct(connectSchema)
        .put("id", "order-1")
        .put("gst_details", "{\"cgst\":9.0}")
        .put("meta", meta);

    org.apache.avro.Schema avroSchema =
        AVRO_DATA.fromConnectSchema(connectSchema);
    GenericRecord avroRecord = (GenericRecord) AVRO_DATA
        .fromConnectData(connectSchema, record);

    Set<String> variantPaths = new HashSet<>(
        Arrays.asList("gst_details", "meta"));

    File outputFile =
        tempFolder.newFile("test_mixed_variant.parquet");
    outputFile.delete();

    VariantAwareWriteSupport writeSupport =
        new VariantAwareWriteSupport(
            avroSchema, variantPaths, true);

    OutputFile out = new LocalOutputFile(outputFile.toPath());
    ParquetWriter<GenericRecord> writer =
        new TestWriterBuilder(out, writeSupport)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .build();

    writer.write(avroRecord);
    writer.close();

    ParquetFileReader reader = ParquetFileReader.open(
        new LocalInputFile(outputFile.toPath()));
    MessageType schema =
        reader.getFooter().getFileMetaData().getSchema();
    reader.close();

    assertTrue("gst_details should be VARIANT",
        schema.getType("gst_details").getLogicalTypeAnnotation()
            instanceof
            LogicalTypeAnnotation.VariantLogicalTypeAnnotation);
    assertTrue("meta should be VARIANT",
        schema.getType("meta").getLogicalTypeAnnotation()
            instanceof
            LogicalTypeAnnotation.VariantLogicalTypeAnnotation);
  }

  @Test
  public void testWriteWithRecursiveStructNoStackOverflow()
      throws IOException {
    Schema valueSchema = SchemaBuilder.struct()
        .name("google.protobuf.Value")
        .field("string_value", Schema.OPTIONAL_STRING_SCHEMA)
        .field("number_value", Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field("bool_value", Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .build();

    Schema mapSchema = SchemaBuilder.map(
        Schema.STRING_SCHEMA, valueSchema
    ).optional().build();

    Schema connectSchema = SchemaBuilder.struct()
        .name("test.WithStruct")
        .field("user_id", Schema.STRING_SCHEMA)
        .field("fraud_level_actions", mapSchema)
        .build();

    Map<String, Struct> fla = new HashMap<>();
    fla.put("block", new Struct(valueSchema)
        .put("bool_value", true));

    Struct record = new Struct(connectSchema)
        .put("user_id", "user-1")
        .put("fraud_level_actions", fla);

    org.apache.avro.Schema avroSchema =
        AVRO_DATA.fromConnectSchema(connectSchema);
    GenericRecord avroRecord = (GenericRecord) AVRO_DATA
        .fromConnectData(connectSchema, record);

    Set<String> variantPaths = new HashSet<>(
        Collections.singletonList("fraud_level_actions"));

    File outputFile =
        tempFolder.newFile("test_recursive.parquet");
    outputFile.delete();

    VariantAwareWriteSupport writeSupport =
        new VariantAwareWriteSupport(
            avroSchema, variantPaths, true);

    OutputFile out = new LocalOutputFile(outputFile.toPath());
    ParquetWriter<GenericRecord> writer =
        new TestWriterBuilder(out, writeSupport)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .build();

    writer.write(avroRecord);
    writer.close();

    assertTrue("File should exist", outputFile.exists());

    ParquetFileReader reader = ParquetFileReader.open(
        new LocalInputFile(outputFile.toPath()));
    MessageType schema =
        reader.getFooter().getFileMetaData().getSchema();
    reader.close();

    Type flaField = schema.getType("fraud_level_actions");
    assertTrue("Should be VARIANT",
        flaField.getLogicalTypeAnnotation()
            instanceof
            LogicalTypeAnnotation.VariantLogicalTypeAnnotation);
  }

  @Test
  public void testWriteDebeziumUnavailableValue()
      throws IOException {
    Schema jsonFieldSchema = SchemaBuilder.string()
        .name("io.debezium.data.Json")
        .optional()
        .build();

    Schema connectSchema = SchemaBuilder.struct()
        .name("test.Record")
        .field("id", Schema.STRING_SCHEMA)
        .field("gst_details", jsonFieldSchema)
        .build();

    Struct normalRecord = new Struct(connectSchema)
        .put("id", "order-1")
        .put("gst_details", "{\"cgst\":9.0,\"sgst\":9.0}");

    Struct unavailableRecord = new Struct(connectSchema)
        .put("id", "order-2")
        .put("gst_details", "__debezium_unavailable_value");

    Struct nullRecord = new Struct(connectSchema)
        .put("id", "order-3")
        .put("gst_details", null);

    org.apache.avro.Schema avroSchema =
        AVRO_DATA.fromConnectSchema(connectSchema);

    Set<String> variantPaths = new HashSet<>(
        Collections.singletonList("gst_details"));

    File outputFile =
        tempFolder.newFile("test_debezium_unavailable.parquet");
    outputFile.delete();

    VariantAwareWriteSupport writeSupport =
        new VariantAwareWriteSupport(
            avroSchema, variantPaths, true);

    OutputFile out = new LocalOutputFile(outputFile.toPath());
    ParquetWriter<GenericRecord> writer =
        new TestWriterBuilder(out, writeSupport)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .withCompressionCodec(
                CompressionCodecName.UNCOMPRESSED)
            .build();

    GenericRecord avroNormal = (GenericRecord) AVRO_DATA
        .fromConnectData(connectSchema, normalRecord);
    GenericRecord avroUnavail = (GenericRecord) AVRO_DATA
        .fromConnectData(connectSchema, unavailableRecord);
    GenericRecord avroNull = (GenericRecord) AVRO_DATA
        .fromConnectData(connectSchema, nullRecord);

    writer.write(avroNormal);
    writer.write(avroUnavail);
    writer.write(avroNull);
    writer.close();

    assertTrue("Parquet file should exist",
        outputFile.exists());
    assertTrue("Parquet file should have content",
        outputFile.length() > 0);

    ParquetFileReader reader = ParquetFileReader.open(
        new LocalInputFile(outputFile.toPath()));
    MessageType schema =
        reader.getFooter().getFileMetaData().getSchema();
    long rowCount = reader.getRecordCount();
    reader.close();

    assertEquals(
        "All 3 records should be written (normal + "
            + "unavailable + null)",
        3, rowCount);

    Type gstField = schema.getType("gst_details");
    assertTrue(
        "gst_details should remain VARIANT even with "
            + "__debezium_unavailable_value rows",
        gstField.getLogicalTypeAnnotation()
            instanceof
            LogicalTypeAnnotation.VariantLogicalTypeAnnotation);
  }

  private static class TestWriterBuilder
      extends ParquetWriter.Builder<GenericRecord, TestWriterBuilder> {

    private final VariantAwareWriteSupport writeSupport;

    TestWriterBuilder(
        OutputFile outputFile,
        VariantAwareWriteSupport writeSupport
    ) {
      super(outputFile);
      this.writeSupport = writeSupport;
    }

    @Override
    protected TestWriterBuilder self() {
      return this;
    }

    @Override
    protected WriteSupport<GenericRecord> getWriteSupport(
        org.apache.hadoop.conf.Configuration conf
    ) {
      return writeSupport;
    }

    @Override
    protected WriteSupport<GenericRecord> getWriteSupport(
        org.apache.parquet.conf.ParquetConfiguration conf
    ) {
      return writeSupport;
    }
  }
}
