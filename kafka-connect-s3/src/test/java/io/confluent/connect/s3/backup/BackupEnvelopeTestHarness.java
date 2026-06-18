package io.confluent.connect.s3.backup;

import io.confluent.connect.avro.AvroConverter;
import io.confluent.connect.json.JsonSchemaConverter;
import io.confluent.connect.protobuf.ProtobufConverter;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.StringConverter;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class BackupEnvelopeTestHarness {

  private static final String TOPIC = "test-topic";
  private static int passed = 0;
  private static int failed = 0;

  public static void main(String[] args) throws Exception {
    System.out.println("=== Backup Envelope Test Harness ===\n");

    // SR-backed converters
    testAvroValueStringKey();
    testAvroKeyAvroValue();

    // Cross-SR converter combos
    testProtoValueStringKey();
    testJsonSchemaValueStringKey();
    testAvroValueProtoKey();
    testSchemafulJson();

    // Non-SR converters
    testStringValueStringKey();
    testNullValue();
    testNullKey();
    testSchemalessJson();
    testIntegerKey();
    testBothNull();

    System.out.println("\n=== RESULTS: " + passed + " passed, "
        + failed + " failed ===");
    if (failed > 0) {
      System.exit(1);
    }
  }

  // ==================== SR Converter Tests ====================

  static void testAvroValueStringKey() throws Exception {
    System.out.println("--- Test: Avro value + String key ---");
    MockSchemaRegistryClient srClient = new MockSchemaRegistryClient();
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder
        .record("User").namespace("com.example")
        .fields().requiredString("name").requiredInt("age")
        .endRecord();
    int schemaId = srClient.register(TOPIC + "-value",
        new AvroSchema(avroSchema));

    GenericRecord avroRecord = new GenericData.Record(avroSchema);
    avroRecord.put("name", "Alice");
    avroRecord.put("age", 30);
    byte[] valueBytes = serializeAvro(schemaId, avroSchema, avroRecord);

    SinkRecord record = buildSinkRecord(
        "user-123", valueBytes, srClient, true);

    // Verify wrapper
    System.out.println("  DEBUG record.valueSchema()=" + record.valueSchema());
    System.out.println("  DEBUG record.valueSchema().name()=" + record.valueSchema().name());
    System.out.println("  DEBUG isWrapper=" + io.confluent.connect.schema.backup.BackupWrapper.isWrapper(record.valueSchema()));
    assertWrapper(record, schemaId, "AVRO");

    // Verify envelope
    SinkRecord envelope = writeEnvelope(record, "STRING", "AVRO");
    Struct env = (Struct) envelope.value();
    System.out.println("  DEBUG envelope schema: " + env.schema().name());
    System.out.println("  DEBUG envelope fields: " + env.schema().fields());
    System.out.println("  DEBUG valueSchemaId: " + env.get("valueSchemaId"));
    System.out.println("  DEBUG keySchemaId: " + env.get("keySchemaId"));
    assertEquals("key", "user-123", env.get("key"));
    assertEquals("valueSchemaId", schemaId,
        (int) env.getInt32("valueSchemaId"));
    assertEquals("valueSchemaType", "AVRO",
        env.getString("valueSchemaType"));
    assertEquals("keySchemaType", "STRING",
        env.getString("keySchemaType"));
    assertNull("keySchemaId", env.get("keySchemaId"));
    pass();
  }

  static void testAvroKeyAvroValue() throws Exception {
    System.out.println("--- Test: Avro key + Avro value ---");
    MockSchemaRegistryClient srClient = new MockSchemaRegistryClient();

    org.apache.avro.Schema keySchema = org.apache.avro.SchemaBuilder
        .record("Key").namespace("com.example")
        .fields().requiredString("id").endRecord();
    org.apache.avro.Schema valueSchema = org.apache.avro.SchemaBuilder
        .record("Value").namespace("com.example")
        .fields().requiredString("data").endRecord();

    int keySchemaId = srClient.register(TOPIC + "-key",
        new AvroSchema(keySchema));
    int valueSchemaId = srClient.register(TOPIC + "-value",
        new AvroSchema(valueSchema));

    GenericRecord keyRec = new GenericData.Record(keySchema);
    keyRec.put("id", "k1");
    GenericRecord valueRec = new GenericData.Record(valueSchema);
    valueRec.put("data", "hello");

    byte[] keyBytes = serializeAvro(keySchemaId, keySchema, keyRec);
    byte[] valueBytes = serializeAvro(valueSchemaId, valueSchema,
        valueRec);

    // Key converter
    AvroConverter keyConverter = new AvroConverter(srClient);
    Map<String, Object> keyCfg = new HashMap<>();
    keyCfg.put("schema.registry.url", "mock://");
    keyCfg.put("backup.mode", "envelope");
    keyConverter.configure(keyCfg, true);

    // Value converter
    AvroConverter valueConverter = new AvroConverter(srClient);
    Map<String, Object> valCfg = new HashMap<>();
    valCfg.put("schema.registry.url", "mock://");
    valCfg.put("backup.mode", "envelope");
    valueConverter.configure(valCfg, false);

    RecordHeaders headers = new RecordHeaders();
    SchemaAndValue keyResult = keyConverter.toConnectData(
        TOPIC, headers, keyBytes);
    SchemaAndValue valueResult = valueConverter.toConnectData(
        TOPIC, headers, valueBytes);

    SinkRecord record = new SinkRecord(TOPIC, 0,
        keyResult.schema(), keyResult.value(),
        valueResult.schema(), valueResult.value(),
        1L, System.currentTimeMillis(),
        org.apache.kafka.common.record.TimestampType.CREATE_TIME,
        new ConnectHeaders());

    // Both should be wrapped
    assertWrapper(record, valueSchemaId, "AVRO");
    assertEquals("key wrapper", "io.confluent.connect.backup.Wrapper",
        record.keySchema().name());

    SinkRecord envelope = writeEnvelope(record, "AVRO", "AVRO");
    Struct env = (Struct) envelope.value();
    assertEquals("keySchemaType", "AVRO",
        env.getString("keySchemaType"));
    assertEquals("valueSchemaType", "AVRO",
        env.getString("valueSchemaType"));
    assertEquals("keySchemaId", keySchemaId,
        (int) env.getInt32("keySchemaId"));
    assertEquals("valueSchemaId", valueSchemaId,
        (int) env.getInt32("valueSchemaId"));
    pass();
  }

  // ==================== Cross-SR Converter Tests ====================

  static void testProtoValueStringKey() throws Exception {
    System.out.println("--- Test: Protobuf value + String key ---");
    MockSchemaRegistryClient srClient = new MockSchemaRegistryClient();

    String protoDef = "syntax = \"proto3\";\n"
        + "message TestMessage {\n"
        + "  string name = 1;\n"
        + "  int32 age = 2;\n"
        + "}\n";
    ProtobufSchema protoSchema = new ProtobufSchema(protoDef);
    int schemaId = srClient.register(TOPIC + "-value", protoSchema);

    // Use the converter to serialize (fromConnectData) then
    // deserialize (toConnectData) to get proper wire format
    ProtobufConverter protoConverter = new ProtobufConverter(srClient);
    Map<String, Object> cfg = new HashMap<>();
    cfg.put("schema.registry.url", "mock://");
    protoConverter.configure(cfg, false);

    // Build a Connect Struct to serialize
    Schema connectSchema = org.apache.kafka.connect.data.SchemaBuilder
        .struct().name("TestMessage")
        .field("name", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .build();
    Struct connectValue = new Struct(connectSchema)
        .put("name", "Alice").put("age", 30);

    // Serialize to wire format, then deserialize with backup mode
    byte[] wireBytes = protoConverter.fromConnectData(
        TOPIC, connectSchema, connectValue);
    RecordHeaders h = new RecordHeaders();
    SchemaAndValue vr = protoConverter.toConnectData(TOPIC, h, wireBytes);

    StringConverter kc = new StringConverter();
    kc.configure(Collections.emptyMap(), true);
    SchemaAndValue kr = kc.toConnectData(TOPIC, h, "pk1".getBytes());

    SinkRecord record = new SinkRecord(TOPIC, 0,
        kr.schema(), kr.value(), vr.schema(), vr.value(),
        100L, null, null, new ConnectHeaders());

    // Using released converter (no backup wrapping), so no wrapper
    // The envelope writer handles it as non-wrapped with type from config
    SinkRecord envelope = writeEnvelope(record, "STRING", "PROTOBUF");
    Struct env = (Struct) envelope.value();
    assertEquals("valueSchemaType", "PROTOBUF",
        env.getString("valueSchemaType"));
    assertEquals("key", "pk1", env.get("key"));
    assertNotNull("value", env.get("value"));
    pass();
  }

  static void testJsonSchemaValueStringKey() throws Exception {
    System.out.println("--- Test: JSON Schema value + String key ---");
    MockSchemaRegistryClient srClient = new MockSchemaRegistryClient();

    String jsonSchemaDef = "{"
        + "\"type\":\"object\","
        + "\"properties\":{"
        + "  \"name\":{\"type\":\"string\"},"
        + "  \"score\":{\"type\":\"integer\"}"
        + "},"
        + "\"required\":[\"name\",\"score\"]"
        + "}";
    JsonSchema jsonSchema = new JsonSchema(jsonSchemaDef);
    int schemaId = srClient.register(TOPIC + "-value", jsonSchema);

    JsonSchemaConverter jsConverter = new JsonSchemaConverter(srClient);
    Map<String, Object> cfg = new HashMap<>();
    cfg.put("schema.registry.url", "mock://");
    jsConverter.configure(cfg, false);

    Schema connectSchema = org.apache.kafka.connect.data.SchemaBuilder
        .struct()
        .field("name", Schema.STRING_SCHEMA)
        .field("score", Schema.INT64_SCHEMA)
        .build();
    Struct connectValue = new Struct(connectSchema)
        .put("name", "Bob").put("score", 95L);

    byte[] wireBytes = jsConverter.fromConnectData(
        TOPIC, connectSchema, connectValue);
    RecordHeaders h = new RecordHeaders();
    SchemaAndValue vr = jsConverter.toConnectData(TOPIC, h, wireBytes);

    StringConverter kc = new StringConverter();
    kc.configure(Collections.emptyMap(), true);
    SchemaAndValue kr = kc.toConnectData(TOPIC, h, "jk1".getBytes());

    SinkRecord record = new SinkRecord(TOPIC, 0,
        kr.schema(), kr.value(), vr.schema(), vr.value(),
        110L, null, null, new ConnectHeaders());

    // Using released converter (no backup wrapping)
    SinkRecord envelope = writeEnvelope(
        record, "STRING", "JSON_SCHEMA");
    Struct env = (Struct) envelope.value();
    assertEquals("valueSchemaType", "JSON_SCHEMA",
        env.getString("valueSchemaType"));
    assertNotNull("value", env.get("value"));
    pass();
  }

  static void testAvroValueProtoKey() throws Exception {
    System.out.println("--- Test: Avro value + Protobuf key ---");
    MockSchemaRegistryClient srClient = new MockSchemaRegistryClient();

    // Proto key
    String protoDef = "syntax = \"proto3\";\n"
        + "message KeyMsg { string id = 1; }\n";
    ProtobufSchema protoSchema = new ProtobufSchema(protoDef);
    int keySchemaId = srClient.register(TOPIC + "-key", protoSchema);

    ProtobufConverter keyConverter = new ProtobufConverter(srClient);
    Map<String, Object> keyCfg = new HashMap<>();
    keyCfg.put("schema.registry.url", "mock://");
    keyConverter.configure(keyCfg, true);

    Schema keyConnectSchema = org.apache.kafka.connect.data.SchemaBuilder
        .struct().name("KeyMsg")
        .field("id", Schema.STRING_SCHEMA).build();
    Struct keyValue = new Struct(keyConnectSchema).put("id", "k99");
    byte[] keyWire = keyConverter.fromConnectData(
        TOPIC, keyConnectSchema, keyValue);

    // Avro value
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder
        .record("Payload").namespace("com.example")
        .fields().requiredString("data").endRecord();
    int valueSchemaId = srClient.register(
        TOPIC + "-value", new AvroSchema(avroSchema));

    GenericRecord avroRec = new GenericData.Record(avroSchema);
    avroRec.put("data", "payload-1");
    byte[] valueWire = serializeAvro(
        valueSchemaId, avroSchema, avroRec);

    AvroConverter valueConverter = new AvroConverter(srClient);
    Map<String, Object> valCfg = new HashMap<>();
    valCfg.put("schema.registry.url", "mock://");
    valCfg.put("backup.mode", "envelope");
    valueConverter.configure(valCfg, false);

    RecordHeaders h = new RecordHeaders();
    SchemaAndValue kr = keyConverter.toConnectData(TOPIC, h, keyWire);
    SchemaAndValue vr = valueConverter.toConnectData(
        TOPIC, h, valueWire);

    SinkRecord record = new SinkRecord(TOPIC, 0,
        kr.schema(), kr.value(), vr.schema(), vr.value(),
        120L, null, null, new ConnectHeaders());

    // Key: released proto converter (no wrapper)
    // Value: our modified avro converter (with wrapper)
    assertWrapper(record, valueSchemaId, "AVRO");

    SinkRecord envelope = writeEnvelope(
        record, "PROTOBUF", "AVRO");
    Struct env = (Struct) envelope.value();
    assertEquals("keySchemaType", "PROTOBUF",
        env.getString("keySchemaType"));
    assertEquals("valueSchemaType", "AVRO",
        env.getString("valueSchemaType"));
    assertEquals("valueSchemaId", valueSchemaId,
        (int) env.getInt32("valueSchemaId"));
    assertNotNull("key", env.get("key"));
    pass();
  }

  static void testSchemafulJson() throws Exception {
    System.out.println("--- Test: Schemaful JSON (non-SR) ---");
    org.apache.kafka.connect.json.JsonConverter jc =
        new org.apache.kafka.connect.json.JsonConverter();
    Map<String, Object> cfg = new HashMap<>();
    cfg.put("schemas.enable", "true");
    jc.configure(cfg, false);

    // Build schema+payload JSON
    String jsonWithSchema = "{\"schema\":{\"type\":\"struct\","
        + "\"fields\":[{\"type\":\"string\",\"field\":\"msg\"}]},"
        + "\"payload\":{\"msg\":\"hello\"}}";
    RecordHeaders h = new RecordHeaders();
    SchemaAndValue vr = jc.toConnectData(
        TOPIC, h, jsonWithSchema.getBytes());

    StringConverter kc = new StringConverter();
    kc.configure(Collections.emptyMap(), true);
    SchemaAndValue kr = kc.toConnectData(TOPIC, h, "jk".getBytes());

    SinkRecord record = new SinkRecord(TOPIC, 0,
        kr.schema(), kr.value(), vr.schema(), vr.value(),
        130L, null, null, new ConnectHeaders());

    // Non-SR: no wrapper
    assert !"io.confluent.connect.backup.Wrapper"
        .equals(record.valueSchema().name())
        : "Should NOT be wrapped (non-SR)";

    SinkRecord envelope = writeEnvelope(
        record, "STRING", "JSON_CONNECT_SCHEMAFUL");
    Struct env = (Struct) envelope.value();
    assertEquals("valueSchemaType", "JSON_CONNECT_SCHEMAFUL",
        env.getString("valueSchemaType"));
    assertNull("valueSchemaId", env.get("valueSchemaId"));
    assertNotNull("value", env.get("value"));
    pass();
  }

  // ==================== Non-SR Converter Tests ====================

  static void testStringValueStringKey() throws Exception {
    System.out.println("--- Test: String value + String key ---");
    StringConverter kc = new StringConverter();
    kc.configure(Collections.emptyMap(), true);
    StringConverter vc = new StringConverter();
    vc.configure(Collections.emptyMap(), false);

    RecordHeaders h = new RecordHeaders();
    SchemaAndValue kr = kc.toConnectData(TOPIC, h, "k".getBytes());
    SchemaAndValue vr = vc.toConnectData(TOPIC, h, "v".getBytes());

    SinkRecord record = new SinkRecord(TOPIC, 0,
        kr.schema(), kr.value(), vr.schema(), vr.value(),
        10L, null, null, new ConnectHeaders());

    SinkRecord envelope = writeEnvelope(record, "STRING", "STRING");
    Struct env = (Struct) envelope.value();
    assertEquals("key", "k", env.get("key"));
    assertEquals("value", "v", env.get("value"));
    assertEquals("keySchemaType", "STRING",
        env.getString("keySchemaType"));
    assertEquals("valueSchemaType", "STRING",
        env.getString("valueSchemaType"));
    assertNull("keySchemaId", env.get("keySchemaId"));
    assertNull("valueSchemaId", env.get("valueSchemaId"));
    pass();
  }

  static void testNullValue() throws Exception {
    System.out.println("--- Test: Null value (tombstone) ---");
    StringConverter kc = new StringConverter();
    kc.configure(Collections.emptyMap(), true);

    RecordHeaders h = new RecordHeaders();
    SchemaAndValue kr = kc.toConnectData(TOPIC, h, "del-key".getBytes());

    SinkRecord record = new SinkRecord(TOPIC, 0,
        kr.schema(), kr.value(), null, null,
        20L, null, null, new ConnectHeaders());

    SinkRecord envelope = writeEnvelope(record, "STRING", "AVRO");
    Struct env = (Struct) envelope.value();
    assertEquals("valueSchemaType", "NONE",
        env.getString("valueSchemaType"));
    assertNull("value", env.get("value"));
    assertNull("valueSchemaId", env.get("valueSchemaId"));
    assertEquals("key", "del-key", env.get("key"));
    pass();
  }

  static void testNullKey() throws Exception {
    System.out.println("--- Test: Null key + String value ---");
    StringConverter vc = new StringConverter();
    vc.configure(Collections.emptyMap(), false);

    RecordHeaders h = new RecordHeaders();
    SchemaAndValue vr = vc.toConnectData(TOPIC, h, "data".getBytes());

    SinkRecord record = new SinkRecord(TOPIC, 0,
        null, null, vr.schema(), vr.value(),
        30L, null, null, new ConnectHeaders());

    SinkRecord envelope = writeEnvelope(record, "STRING", "STRING");
    Struct env = (Struct) envelope.value();
    assertEquals("keySchemaType", "NONE",
        env.getString("keySchemaType"));
    assertNull("key", env.get("key"));
    assertEquals("value", "data", env.get("value"));
    pass();
  }

  static void testSchemalessJson() throws Exception {
    System.out.println("--- Test: Schemaless JSON ---");
    org.apache.kafka.connect.json.JsonConverter jc =
        new org.apache.kafka.connect.json.JsonConverter();
    Map<String, Object> cfg = new HashMap<>();
    cfg.put("schemas.enable", "false");
    jc.configure(cfg, false);

    byte[] json = "{\"name\":\"Bob\",\"age\":25}".getBytes();
    RecordHeaders h = new RecordHeaders();
    SchemaAndValue vr = jc.toConnectData(TOPIC, h, json);

    StringConverter kc = new StringConverter();
    kc.configure(Collections.emptyMap(), true);
    SchemaAndValue kr = kc.toConnectData(TOPIC, h, "k".getBytes());

    SinkRecord record = new SinkRecord(TOPIC, 0,
        kr.schema(), kr.value(), vr.schema(), vr.value(),
        40L, null, null, new ConnectHeaders());

    SinkRecord envelope = writeEnvelope(record, "STRING", "SCHEMALESS");
    Struct env = (Struct) envelope.value();
    // Schemaless JSON is converted to string
    assertNotNull("value", env.get("value"));
    assertEquals("valueSchemaType", "SCHEMALESS",
        env.getString("valueSchemaType"));
    pass();
  }

  static void testIntegerKey() throws Exception {
    System.out.println("--- Test: Integer key + Avro value ---");
    MockSchemaRegistryClient srClient = new MockSchemaRegistryClient();
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder
        .record("Event").namespace("com.example")
        .fields().requiredString("msg").endRecord();
    int schemaId = srClient.register(TOPIC + "-value",
        new AvroSchema(avroSchema));

    GenericRecord avroRec = new GenericData.Record(avroSchema);
    avroRec.put("msg", "hello");
    byte[] valueBytes = serializeAvro(schemaId, avroSchema, avroRec);

    // Integer key (4 bytes big-endian)
    byte[] keyBytes = ByteBuffer.allocate(4).putInt(42).array();
    org.apache.kafka.connect.converters.IntegerConverter intConverter =
        new org.apache.kafka.connect.converters.IntegerConverter();
    intConverter.configure(Collections.emptyMap(), true);

    AvroConverter valueConverter = new AvroConverter(srClient);
    Map<String, Object> valCfg = new HashMap<>();
    valCfg.put("schema.registry.url", "mock://");
    valCfg.put("backup.mode", "envelope");
    valueConverter.configure(valCfg, false);

    RecordHeaders h = new RecordHeaders();
    SchemaAndValue kr = intConverter.toConnectData(TOPIC, keyBytes);
    SchemaAndValue vr = valueConverter.toConnectData(TOPIC, h, valueBytes);

    SinkRecord record = new SinkRecord(TOPIC, 0,
        kr.schema(), kr.value(), vr.schema(), vr.value(),
        50L, null, null, new ConnectHeaders());

    SinkRecord envelope = writeEnvelope(record, "INT32", "AVRO");
    Struct env = (Struct) envelope.value();
    assertEquals("key", 42, env.get("key"));
    assertEquals("keySchemaType", "INT32",
        env.getString("keySchemaType"));
    assertEquals("valueSchemaType", "AVRO",
        env.getString("valueSchemaType"));
    assertEquals("valueSchemaId", schemaId,
        (int) env.getInt32("valueSchemaId"));
    pass();
  }

  static void testBothNull() throws Exception {
    System.out.println("--- Test: Both key and value null ---");
    SinkRecord record = new SinkRecord(TOPIC, 0,
        null, null, null, null,
        60L, null, null, new ConnectHeaders());

    SinkRecord envelope = writeEnvelope(record, "NONE", "NONE");
    Struct env = (Struct) envelope.value();
    assertNull("key", env.get("key"));
    assertNull("value", env.get("value"));
    assertEquals("keySchemaType", "NONE",
        env.getString("keySchemaType"));
    assertEquals("valueSchemaType", "NONE",
        env.getString("valueSchemaType"));
    pass();
  }

  // ==================== Helpers ====================

  static SinkRecord buildSinkRecord(
      String key, byte[] valueBytes,
      MockSchemaRegistryClient srClient, boolean backupMode
  ) {
    Map<String, Object> valCfg = new HashMap<>();
    valCfg.put("schema.registry.url", "mock://");
    if (backupMode) {
      valCfg.put("backup.mode", "envelope");
    }
    AvroConverter vc = new AvroConverter(srClient);
    vc.configure(valCfg, false);

    StringConverter kc = new StringConverter();
    kc.configure(Collections.emptyMap(), true);

    RecordHeaders h = new RecordHeaders();
    SchemaAndValue kr = kc.toConnectData(TOPIC, h, key.getBytes());
    SchemaAndValue vr = vc.toConnectData(TOPIC, h, valueBytes);

    return new SinkRecord(TOPIC, 0,
        kr.schema(), kr.value(), vr.schema(), vr.value(),
        42L, System.currentTimeMillis(),
        org.apache.kafka.common.record.TimestampType.CREATE_TIME,
        new ConnectHeaders());
  }

  static SinkRecord writeEnvelope(
      SinkRecord record, String keyType, String valueType
  ) {
    io.confluent.connect.storage.format.backup.EnvelopeTransformer transformer =
        new io.confluent.connect.storage.format.backup.EnvelopeTransformer(
            new NoOpBackupStore(), keyType, valueType);
    return transformer.wrap(record);
  }

  static void assertWrapper(SinkRecord record, int expectedId,
      String expectedType) {
    assert "io.confluent.connect.backup.Wrapper"
        .equals(record.valueSchema().name())
        : "Expected wrapper schema, got: "
            + record.valueSchema().name();
    Struct w = (Struct) record.value();
    assert w.getInt32("schemaId") == expectedId
        : "schemaId mismatch";
    assert expectedType.equals(w.getString("schemaType"))
        : "schemaType mismatch";
  }

  static byte[] serializeAvro(int schemaId,
      org.apache.avro.Schema schema, GenericRecord record)
      throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    baos.write(0x00);
    baos.write(ByteBuffer.allocate(4).putInt(schemaId).array());
    GenericDatumWriter<GenericRecord> writer =
        new GenericDatumWriter<>(schema);
    BinaryEncoder encoder = EncoderFactory.get()
        .binaryEncoder(baos, null);
    writer.write(record, encoder);
    encoder.flush();
    return baos.toByteArray();
  }

  static void assertEquals(String field, Object expected,
      Object actual) {
    if (expected == null ? actual != null : !expected.equals(actual)) {
      System.out.println("  FAIL: " + field + " expected="
          + expected + " actual=" + actual);
      throw new AssertionError(field + " mismatch");
    }
  }

  static void assertNull(String field, Object actual) {
    if (actual != null) {
      System.out.println("  FAIL: " + field + " expected null, got="
          + actual);
      throw new AssertionError(field + " should be null");
    }
  }

  static void assertNotNull(String field, Object actual) {
    if (actual == null) {
      System.out.println("  FAIL: " + field + " should not be null");
      throw new AssertionError(field + " should not be null");
    }
  }

  static void pass() {
    passed++;
    System.out.println("  PASSED ✓\n");
  }

  static class TestRecordWriter
      implements io.confluent.connect.storage.format.RecordWriter {
    SinkRecord lastRecord;
    public void write(SinkRecord r) { this.lastRecord = r; }
    public void commit() {}
    public void close() {}
  }

  static class NoOpBackupStore
      implements io.confluent.connect.storage.backup.SchemaBackupStore {
    public void backupIfNeeded(String t, int id, int version,
        String type, String subject, String raw,
        java.util.List<io.confluent.connect.storage.backup.SchemaManifest.SchemaReferenceEntry> refs) {
      System.out.println("  [Backup] id=" + id + " type=" + type
          + " subject=" + subject
          + " refs=" + (refs != null ? refs.size() : 0));
    }
    public void persistManifest(String t) {}
    public void load(String t) {}
    public io.confluent.connect.storage.backup.SchemaManifest getManifest(String t) {
      return null;
    }
  }
}
