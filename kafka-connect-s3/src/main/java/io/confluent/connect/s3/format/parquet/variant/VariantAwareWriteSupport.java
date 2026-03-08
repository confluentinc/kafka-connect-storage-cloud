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

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.variant.Variant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A thin {@link WriteSupport} wrapper that pre-processes Avro
 * {@link GenericRecord}s, converting variant-targeted fields
 * (JSON strings, complex structs, maps, arrays) to Variant binary
 * sub-records, then delegates all Parquet writing to the native
 * {@link AvroWriteSupport} (which handles VARIANT logical types
 * in parquet-avro 1.17+).
 *
 * <p>Supports three field types as VARIANT sources:
 * <ul>
 *   <li>STRING fields containing JSON (Debezium, JSON schema)</li>
 *   <li>RECORD/MAP fields (Protobuf Struct, custom messages)</li>
 *   <li>ARRAY fields (repeated Protobuf Struct)</li>
 * </ul>
 *
 * <p>This avoids reimplementing Avro-to-Parquet writing logic.
 */
public class VariantAwareWriteSupport extends WriteSupport<GenericRecord> {

  private static final Logger log =
      LoggerFactory.getLogger(VariantAwareWriteSupport.class);

  private static final String METADATA_FIELD = "metadata";
  private static final String VALUE_FIELD = "value";

  private final Schema originalAvroSchema;
  private final Set<String> variantFieldPaths;
  private final boolean writeOldListStructure;

  private Schema variantAvroSchema;
  private AvroWriteSupport<GenericRecord> delegate;

  public VariantAwareWriteSupport(
      Schema avroSchema,
      Set<String> variantFieldPaths,
      boolean writeOldListStructure
  ) {
    this.originalAvroSchema = avroSchema;
    this.variantFieldPaths = variantFieldPaths;
    this.writeOldListStructure = writeOldListStructure;
  }

  @Override
  public WriteContext init(
      org.apache.hadoop.conf.Configuration configuration
  ) {
    if (!writeOldListStructure) {
      configuration.setBoolean(
          AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, false);
    }
    return doInit(configuration);
  }

  @Override
  public WriteContext init(ParquetConfiguration configuration) {
    if (!writeOldListStructure) {
      configuration.setBoolean(
          AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, false);
    }
    return doInit(configuration);
  }

  private WriteContext doInit(Object configuration) {
    Map<Schema, Schema> schemaCache = new IdentityHashMap<>();
    this.variantAvroSchema = transformAvroSchema(
        originalAvroSchema, "", variantFieldPaths, schemaCache);

    MessageType parquetSchema = VariantSchemaBuilder.transformSchema(
        convertToParquet(variantAvroSchema, configuration),
        variantFieldPaths);

    this.delegate = new AvroWriteSupport<>(
        parquetSchema, variantAvroSchema, GenericData.get());
    initDelegate(configuration);

    Map<String, String> extraMeta = new HashMap<>();
    extraMeta.put("avro.schema", originalAvroSchema.toString());
    return new WriteContext(parquetSchema, extraMeta);
  }

  private MessageType convertToParquet(
      Schema avroSchema, Object configuration
  ) {
    if (configuration
        instanceof org.apache.hadoop.conf.Configuration) {
      return new org.apache.parquet.avro.AvroSchemaConverter(
          (org.apache.hadoop.conf.Configuration) configuration)
          .convert(avroSchema);
    }
    return new org.apache.parquet.avro.AvroSchemaConverter(
        (ParquetConfiguration) configuration)
        .convert(avroSchema);
  }

  private void initDelegate(Object configuration) {
    if (configuration
        instanceof org.apache.hadoop.conf.Configuration) {
      delegate.init(
          (org.apache.hadoop.conf.Configuration) configuration);
    } else {
      delegate.init((ParquetConfiguration) configuration);
    }
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    delegate.prepareForWrite(recordConsumer);
  }

  @Override
  public void write(GenericRecord record) {
    GenericRecord transformed = transformRecord(
        record, originalAvroSchema, variantAvroSchema,
        "", variantFieldPaths);
    delegate.write(transformed);
  }

  /**
   * Builds a variant-compatible Avro schema: fields on variant
   * paths (STRING, RECORD, MAP, ARRAY) become
   * RECORD{metadata: BYTES, value: BYTES}.
   */
  static Schema transformAvroSchema(
      Schema schema, String prefix, Set<String> variantPaths,
      Map<Schema, Schema> cache
  ) {
    if (schema.getType() != Schema.Type.RECORD) {
      return schema;
    }
    Schema cached = cache.get(schema);
    if (cached != null) {
      return cached;
    }
    Schema result = Schema.createRecord(
        schema.getName() + "_variant",
        schema.getDoc(), schema.getNamespace(), false);
    cache.put(schema, result);

    List<Schema.Field> newFields = new ArrayList<>();
    for (Schema.Field field : schema.getFields()) {
      String path = prefix.isEmpty()
          ? field.name() : prefix + "." + field.name();
      Schema transformed = transformFieldSchema(
          field.schema(), path, variantPaths, cache);
      newFields.add(new Schema.Field(
          field.name(), transformed, field.doc(),
          field.defaultVal(), field.order()));
    }
    result.setFields(newFields);
    return result;
  }

  private static Schema transformFieldSchema(
      Schema fieldSchema, String path,
      Set<String> variantPaths, Map<Schema, Schema> cache
  ) {
    switch (fieldSchema.getType()) {
      case UNION:
        return transformUnionSchema(
            fieldSchema, path, variantPaths, cache);
      case RECORD:
        if (variantPaths.contains(path)) {
          return createVariantRecordSchema(path);
        }
        return transformAvroSchema(
            fieldSchema, path, variantPaths, cache);
      default:
        if (variantPaths.contains(path)) {
          return createVariantRecordSchema(path);
        }
        return fieldSchema;
    }
  }

  private static Schema transformUnionSchema(
      Schema unionSchema, String path,
      Set<String> variantPaths, Map<Schema, Schema> cache
  ) {
    List<Schema> branches = new ArrayList<>();
    for (Schema branch : unionSchema.getTypes()) {
      if (branch.getType() == Schema.Type.NULL) {
        branches.add(branch);
      } else {
        branches.add(transformFieldSchema(
            branch, path, variantPaths, cache));
      }
    }
    return Schema.createUnion(branches);
  }

  private static Schema createVariantRecordSchema(String path) {
    String safeName = path.replace('.', '_') + "_variant";
    Schema record = Schema.createRecord(
        safeName, null, null, false);
    record.setFields(java.util.Arrays.asList(
        new Schema.Field(
            METADATA_FIELD, Schema.create(Schema.Type.BYTES)),
        new Schema.Field(
            VALUE_FIELD, Schema.create(Schema.Type.BYTES))
    ));
    return record;
  }

  /**
   * Transforms a GenericRecord: values on variant paths (strings,
   * records, maps, arrays) become sub-records with Variant
   * metadata + value bytes.
   */
  static GenericRecord transformRecord(
      IndexedRecord record,
      Schema originalSchema,
      Schema variantSchema,
      String prefix,
      Set<String> variantPaths
  ) {
    GenericRecordBuilder builder =
        new GenericRecordBuilder(variantSchema);
    for (Schema.Field origField : originalSchema.getFields()) {
      String path = prefix.isEmpty()
          ? origField.name() : prefix + "." + origField.name();
      Object value = record.get(origField.pos());
      Schema.Field varField =
          variantSchema.getField(origField.name());
      if (value == null) {
        builder.set(varField, null);
        continue;
      }
      Object transformed = transformValue(
          value, origField.schema(), varField.schema(),
          path, variantPaths);
      builder.set(varField, transformed);
    }
    return builder.build();
  }

  private static Object transformValue(
      Object value, Schema origSchema, Schema varSchema,
      String path, Set<String> variantPaths
  ) {
    Schema resolvedOrig = resolveNonNull(origSchema);
    Schema resolvedVar = resolveNonNull(varSchema);

    if (variantPaths.contains(path)
        && resolvedVar.getType() == Schema.Type.RECORD) {
      return convertAnyToVariantRecord(
          value, resolvedOrig, resolvedVar);
    }

    if (resolvedOrig.getType() == Schema.Type.RECORD
        && value instanceof IndexedRecord) {
      return transformRecord(
          (IndexedRecord) value, resolvedOrig,
          resolvedVar, path, variantPaths);
    }

    return value;
  }

  private static GenericRecord convertAnyToVariantRecord(
      Object value, Schema origSchema,
      Schema variantRecordSchema
  ) {
    String json;
    if (value instanceof CharSequence) {
      json = value.toString();
    } else {
      json = AvroValueToJsonConverter.toJsonString(value);
    }
    return convertToVariantRecord(json, variantRecordSchema);
  }

  private static GenericRecord convertToVariantRecord(
      String json, Schema variantRecordSchema
  ) {
    try {
      Variant variant = JsonToVariantConverter.convert(json);
      if (variant == null) {
        return null;
      }
      return variantToRecord(variant, variantRecordSchema);
    } catch (IOException e) {
      log.warn("Failed to convert JSON to Variant: {}. "
          + "Falling back to string encoding.", e.getMessage());
      return convertStringFallback(json, variantRecordSchema);
    }
  }

  private static GenericRecord variantToRecord(
      Variant variant, Schema schema
  ) {
    GenericRecord rec = new GenericData.Record(schema);
    rec.put(VALUE_FIELD, copyBuffer(variant.getValueBuffer()));
    rec.put(METADATA_FIELD,
        copyBuffer(variant.getMetadataBuffer()));
    return rec;
  }

  private static ByteBuffer copyBuffer(ByteBuffer source) {
    ByteBuffer copy = ByteBuffer.allocate(source.remaining());
    copy.put(source.duplicate());
    copy.flip();
    return copy;
  }

  private static GenericRecord convertStringFallback(
      String value, Schema variantRecordSchema
  ) {
    try {
      String escaped = "\""
          + value.replace("\\", "\\\\").replace("\"", "\\\"")
          + "\"";
      Variant variant = JsonToVariantConverter.convert(escaped);
      if (variant == null) {
        return null;
      }
      return variantToRecord(variant, variantRecordSchema);
    } catch (IOException e) {
      log.error("Failed to encode string as Variant: {}",
          e.getMessage());
      return null;
    }
  }

  private static Schema resolveNonNull(Schema schema) {
    if (schema.getType() != Schema.Type.UNION) {
      return schema;
    }
    for (Schema branch : schema.getTypes()) {
      if (branch.getType() != Schema.Type.NULL) {
        return branch;
      }
    }
    return schema;
  }
}
