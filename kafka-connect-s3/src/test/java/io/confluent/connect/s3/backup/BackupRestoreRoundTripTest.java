package io.confluent.connect.s3.backup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.confluent.connect.avro.AvroConverter;
import io.confluent.connect.schema.backup.BackupSchemaFetcher;
import io.confluent.connect.schema.backup.BackupWrapper;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * E2E round-trip test: Source SR → backup converter → wrapper →
 * target converter → target SR → verify data + schema + refs.
 *
 * Tests the most complex scenario: nested references (User→Address→Country)
 * with schema evolution across 3 versions.
 */
public class BackupRestoreRoundTripTest {

  private static final String TOPIC = "e2e-test-topic";

  // ========================== SCHEMAS ==========================

  static final String COUNTRY_SCHEMA =
      "{\"type\":\"record\",\"name\":\"Country\","
      + "\"namespace\":\"com.example\","
      + "\"fields\":["
      + "{\"name\":\"code\",\"type\":\"string\"}"
      + "]}";

  static final String ADDRESS_SCHEMA =
      "{\"type\":\"record\",\"name\":\"Address\","
      + "\"namespace\":\"com.example\","
      + "\"fields\":["
      + "{\"name\":\"city\",\"type\":\"string\"}"
      + "]}";

  static final String ADDRESS_WITH_COUNTRY_SCHEMA =
      "{\"type\":\"record\",\"name\":\"Address\","
      + "\"namespace\":\"com.example\","
      + "\"fields\":["
      + "{\"name\":\"city\",\"type\":\"string\"},"
      + "{\"name\":\"country\",\"type\":\"com.example.Country\"}"
      + "]}";

  static final String USER_V1_SCHEMA =
      "{\"type\":\"record\",\"name\":\"User\","
      + "\"namespace\":\"com.example\","
      + "\"fields\":["
      + "{\"name\":\"name\",\"type\":\"string\"},"
      + "{\"name\":\"age\",\"type\":\"int\"}"
      + "]}";

  static final String USER_V2_SCHEMA =
      "{\"type\":\"record\",\"name\":\"User\","
      + "\"namespace\":\"com.example\","
      + "\"fields\":["
      + "{\"name\":\"name\",\"type\":\"string\"},"
      + "{\"name\":\"age\",\"type\":\"int\"},"
      + "{\"name\":\"email\",\"type\":\"string\"}"
      + "]}";

  // ========================== TEST 1 ==========================
  // Simple schema, no refs — baseline round-trip

  @Test
  public void simpleAvro_roundTrip() throws Exception {
    SchemaRegistryClient sourceSR = new MockSchemaRegistryClient();
    org.apache.avro.Schema avro =
        new org.apache.avro.Schema.Parser().parse(USER_V1_SCHEMA);
    int schemaId = sourceSR.register(
        TOPIC + "-value", new AvroSchema(avro));

    GenericRecord original = new GenericData.Record(avro);
    original.put("name", "Alice");
    original.put("age", 30);

    byte[] restored = backupAndRestore(
        sourceSR, schemaId, avro, original);

    SchemaRegistryClient targetSR = new MockSchemaRegistryClient();
    AvroConverter verifier = createConverter(targetSR, false);
    SchemaAndValue result = verifier.toConnectData(
        TOPIC, new RecordHeaders(), restored);

    Struct s = (Struct) result.value();
    assertEquals("Alice", s.getString("name"));
    assertEquals(30, (int) s.getInt32("age"));
  }

  // ========================== TEST 2 ==========================
  // Schema with direct reference (User → Address)

  @Test
  public void directReference_roundTrip() throws Exception {
    SchemaRegistryClient sourceSR = new MockSchemaRegistryClient();

    AvroSchema addressSchema = new AvroSchema(ADDRESS_SCHEMA);
    sourceSR.register("address-value", addressSchema);

    SchemaReference addressRef = new SchemaReference(
        "com.example.Address", "address-value", 1);
    String userWithAddr =
        "{\"type\":\"record\",\"name\":\"User\","
        + "\"namespace\":\"com.example\","
        + "\"fields\":["
        + "{\"name\":\"name\",\"type\":\"string\"},"
        + "{\"name\":\"address\",\"type\":\"com.example.Address\"}"
        + "]}";
    AvroSchema userSchema = new AvroSchema(userWithAddr,
        Collections.singletonList(addressRef),
        Collections.singletonMap(
            "com.example.Address", ADDRESS_SCHEMA), null);
    int userId = sourceSR.register(TOPIC + "-value", userSchema);

    org.apache.avro.Schema parsed = userSchema.rawSchema();
    GenericRecord addrRec = new GenericData.Record(
        parsed.getField("address").schema());
    addrRec.put("city", "NYC");
    GenericRecord userRec = new GenericData.Record(parsed);
    userRec.put("name", "Alice");
    userRec.put("address", addrRec);

    // Backup
    AvroConverter backupConv = createConverter(sourceSR, true);
    RecordHeaders headers = new RecordHeaders();
    SchemaAndValue backed = backupConv.toConnectData(
        TOPIC, headers, serializeAvro(userId, parsed, userRec));

    // Verify wrapper has refs
    Struct wrapper = (Struct) backed.value();
    assertNotNull("referenceTree",
        wrapper.getString(BackupWrapper.FIELD_REFERENCE_TREE));
    assertNotNull("directRefs",
        wrapper.getString(BackupWrapper.FIELD_DIRECT_REFS));

    // Restore to fresh SR
    SchemaRegistryClient targetSR = new MockSchemaRegistryClient();
    AvroConverter restoreConv = createConverter(targetSR, false);
    byte[] restored = restoreConv.fromConnectData(
        TOPIC, headers, backed.schema(), backed.value());
    assertNotNull("restored bytes", restored);

    // Verify Address registered in target
    int targetAddrV = targetSR.getVersion(
        "address-value", new AvroSchema(ADDRESS_SCHEMA));
    assertTrue("Address in target SR", targetAddrV > 0);

    // Verify data
    SchemaAndValue result = restoreConv.toConnectData(
        TOPIC, headers, restored);
    Struct s = (Struct) result.value();
    assertEquals("Alice", s.getString("name"));
    assertEquals("NYC",
        s.getStruct("address").getString("city"));
  }

  // ========================== TEST 3 ==========================
  // Nested references (User → Address → Country) — the complex case

  @Test
  public void nestedReferences_roundTrip() throws Exception {
    SchemaRegistryClient sourceSR = new MockSchemaRegistryClient();

    // Register Country
    AvroSchema countrySchema = new AvroSchema(COUNTRY_SCHEMA);
    sourceSR.register("country-value", countrySchema);

    // Register Address (refs Country)
    SchemaReference countryRef = new SchemaReference(
        "com.example.Country", "country-value", 1);
    AvroSchema addressSchema = new AvroSchema(
        ADDRESS_WITH_COUNTRY_SCHEMA,
        Collections.singletonList(countryRef),
        Collections.singletonMap(
            "com.example.Country", COUNTRY_SCHEMA), null);
    sourceSR.register("address-value", addressSchema);

    // Register User (refs Address)
    SchemaReference addressRef = new SchemaReference(
        "com.example.Address", "address-value", 1);
    String userSchemaText =
        "{\"type\":\"record\",\"name\":\"User\","
        + "\"namespace\":\"com.example\","
        + "\"fields\":["
        + "{\"name\":\"name\",\"type\":\"string\"},"
        + "{\"name\":\"address\",\"type\":\"com.example.Address\"}"
        + "]}";
    Map<String, String> allResolved = new HashMap<>();
    allResolved.put("com.example.Address",
        ADDRESS_WITH_COUNTRY_SCHEMA);
    allResolved.put("com.example.Country", COUNTRY_SCHEMA);
    AvroSchema userSchema = new AvroSchema(userSchemaText,
        Collections.singletonList(addressRef),
        allResolved, null);
    int userId = sourceSR.register(TOPIC + "-value", userSchema);

    // Build test data
    org.apache.avro.Schema parsed = userSchema.rawSchema();
    org.apache.avro.Schema addrParsed =
        parsed.getField("address").schema();
    GenericRecord countryRec = new GenericData.Record(
        addrParsed.getField("country").schema());
    countryRec.put("code", "US");
    GenericRecord addrRec = new GenericData.Record(addrParsed);
    addrRec.put("city", "NYC");
    addrRec.put("country", countryRec);
    GenericRecord userRec = new GenericData.Record(parsed);
    userRec.put("name", "Alice");
    userRec.put("address", addrRec);

    // Backup
    AvroConverter backupConv = createConverter(sourceSR, true);
    RecordHeaders headers = new RecordHeaders();
    SchemaAndValue backed = backupConv.toConnectData(
        TOPIC, headers, serializeAvro(userId, parsed, userRec));

    // Verify tree has both refs
    Struct wrapper = (Struct) backed.value();
    String tree = wrapper.getString(
        BackupWrapper.FIELD_REFERENCE_TREE);
    assertTrue("Tree has Address",
        tree.contains("com.example.Address"));
    assertTrue("Tree has Country",
        tree.contains("com.example.Country"));

    // Restore to fresh SR
    SchemaRegistryClient targetSR = new MockSchemaRegistryClient();
    AvroConverter restoreConv = createConverter(targetSR, false);
    byte[] restored = restoreConv.fromConnectData(
        TOPIC, headers, backed.schema(), backed.value());
    assertNotNull(restored);

    // Verify depth-first: Country registered before Address
    int targetCountryV = targetSR.getVersion(
        "country-value", new AvroSchema(COUNTRY_SCHEMA));
    assertTrue("Country in target", targetCountryV > 0);

    // Verify nested data
    SchemaAndValue result = restoreConv.toConnectData(
        TOPIC, headers, restored);
    Struct s = (Struct) result.value();
    assertEquals("Alice", s.getString("name"));
    Struct addr = s.getStruct("address");
    assertEquals("NYC", addr.getString("city"));
    Struct country = addr.getStruct("country");
    assertEquals("US", country.getString("code"));
  }

  // ========================== TEST 4 ==========================
  // Schema evolution: V1 then V2, both restored correctly

  @Test
  public void schemaEvolution_twoVersions_roundTrip()
      throws Exception {
    SchemaRegistryClient sourceSR = new MockSchemaRegistryClient();

    org.apache.avro.Schema v1 =
        new org.apache.avro.Schema.Parser().parse(USER_V1_SCHEMA);
    org.apache.avro.Schema v2 =
        new org.apache.avro.Schema.Parser().parse(USER_V2_SCHEMA);

    int id1 = sourceSR.register(
        TOPIC + "-value", new AvroSchema(v1));
    int id2 = sourceSR.register(
        TOPIC + "-value", new AvroSchema(v2));

    // Verify different versions
    BackupSchemaFetcher fetcher = new BackupSchemaFetcher(sourceSR);
    assertEquals(Integer.valueOf(1),
        fetcher.fetchSchemaInfo(id1)
            .getVersionForSubject(TOPIC + "-value"));
    assertEquals(Integer.valueOf(2),
        fetcher.fetchSchemaInfo(id2)
            .getVersionForSubject(TOPIC + "-value"));

    // Record with V1
    GenericRecord rec1 = new GenericData.Record(v1);
    rec1.put("name", "Alice");
    rec1.put("age", 30);

    // Record with V2
    GenericRecord rec2 = new GenericData.Record(v2);
    rec2.put("name", "Bob");
    rec2.put("age", 25);
    rec2.put("email", "bob@test.com");

    // Backup + restore both
    AvroConverter backupConv = createConverter(sourceSR, true);
    SchemaRegistryClient targetSR = new MockSchemaRegistryClient();
    AvroConverter restoreConv = createConverter(targetSR, false);
    RecordHeaders h = new RecordHeaders();

    // V1 round-trip
    SchemaAndValue backed1 = backupConv.toConnectData(
        TOPIC, h, serializeAvro(id1, v1, rec1));
    byte[] restored1 = restoreConv.fromConnectData(
        TOPIC, h, backed1.schema(), backed1.value());
    SchemaAndValue result1 = restoreConv.toConnectData(
        TOPIC, h, restored1);
    assertEquals("Alice",
        ((Struct) result1.value()).getString("name"));
    assertEquals(30,
        (int) ((Struct) result1.value()).getInt32("age"));

    // V2 round-trip
    SchemaAndValue backed2 = backupConv.toConnectData(
        TOPIC, h, serializeAvro(id2, v2, rec2));
    byte[] restored2 = restoreConv.fromConnectData(
        TOPIC, h, backed2.schema(), backed2.value());
    SchemaAndValue result2 = restoreConv.toConnectData(
        TOPIC, h, restored2);
    Struct s2 = (Struct) result2.value();
    assertEquals("Bob", s2.getString("name"));
    assertEquals(25, (int) s2.getInt32("age"));
    assertEquals("bob@test.com", s2.getString("email"));
  }

  // ========================== TEST 5 ==========================
  // Tombstone round-trip

  @Test
  public void tombstone_roundTrip() throws Exception {
    SchemaRegistryClient sourceSR = new MockSchemaRegistryClient();
    AvroConverter backupConv = createConverter(sourceSR, true);

    SchemaAndValue backed = backupConv.toConnectData(
        TOPIC, new RecordHeaders(), null);
    assertEquals(SchemaAndValue.NULL, backed);

    SchemaRegistryClient targetSR = new MockSchemaRegistryClient();
    AvroConverter restoreConv = createConverter(targetSR, false);
    byte[] restored = restoreConv.fromConnectData(
        TOPIC, new RecordHeaders(), null, null);
    assertNull("Tombstone restores as null", restored);
  }

  // ========================== TEST 6 ==========================
  // Version mapping: same schema under multiple subjects

  @Test
  public void multipleSubjects_versionMapping() throws Exception {
    SchemaRegistryClient sourceSR = new MockSchemaRegistryClient();

    org.apache.avro.Schema avro =
        new org.apache.avro.Schema.Parser().parse(USER_V1_SCHEMA);
    AvroSchema schema = new AvroSchema(avro);

    int idA = sourceSR.register("topic-a-value", schema);
    sourceSR.register("topic-b-value", schema);

    BackupSchemaFetcher fetcher = new BackupSchemaFetcher(sourceSR);
    BackupSchemaFetcher.BackupSchemaInfo info =
        fetcher.fetchSchemaInfo(idA);

    assertEquals(Integer.valueOf(1),
        info.getVersionForSubject("topic-a-value"));
    assertEquals(Integer.valueOf(1),
        info.getVersionForSubject("topic-b-value"));
    assertNull(info.getVersionForSubject("nonexistent"));
  }

  // ========================== TEST 7 ==========================
  // Wrapper field completeness verification

  @Test
  public void wrapperFields_complete() throws Exception {
    SchemaRegistryClient sourceSR = new MockSchemaRegistryClient();
    org.apache.avro.Schema avro =
        new org.apache.avro.Schema.Parser().parse(USER_V1_SCHEMA);
    int schemaId = sourceSR.register(
        TOPIC + "-value", new AvroSchema(avro));

    GenericRecord rec = new GenericData.Record(avro);
    rec.put("name", "Test");
    rec.put("age", 1);

    AvroConverter conv = createConverter(sourceSR, true);
    SchemaAndValue backed = conv.toConnectData(
        TOPIC, new RecordHeaders(),
        serializeAvro(schemaId, avro, rec));

    assertTrue(BackupWrapper.isWrapper(backed.schema()));
    Struct w = (Struct) backed.value();

    assertNotNull(w.get(BackupWrapper.FIELD_DATA));
    assertEquals(schemaId,
        (int) w.getInt32(BackupWrapper.FIELD_SCHEMA_ID));
    assertNotNull(
        w.getInt32(BackupWrapper.FIELD_SCHEMA_VERSION));
    assertEquals("AVRO",
        w.getString(BackupWrapper.FIELD_SCHEMA_TYPE));
    assertEquals(TOPIC + "-value",
        w.getString(BackupWrapper.FIELD_SCHEMA_SUBJECT));
    assertNotNull(
        w.getString(BackupWrapper.FIELD_RAW_SCHEMA));
  }

  // ========================== HELPERS ==========================

  private byte[] backupAndRestore(
      SchemaRegistryClient sourceSR, int schemaId,
      org.apache.avro.Schema schema, GenericRecord record)
      throws Exception {
    AvroConverter backupConv = createConverter(sourceSR, true);
    RecordHeaders headers = new RecordHeaders();
    SchemaAndValue backed = backupConv.toConnectData(
        TOPIC, headers,
        serializeAvro(schemaId, schema, record));

    SchemaRegistryClient targetSR = new MockSchemaRegistryClient();
    AvroConverter restoreConv = createConverter(targetSR, false);
    return restoreConv.fromConnectData(
        TOPIC, headers, backed.schema(), backed.value());
  }

  private AvroConverter createConverter(
      SchemaRegistryClient sr, boolean backupMode) {
    AvroConverter converter = new AvroConverter(sr);
    Map<String, Object> cfg = new HashMap<>();
    cfg.put("schema.registry.url", "mock://");
    if (backupMode) {
      cfg.put("backup.mode", "envelope");
    }
    converter.configure(cfg, false);
    return converter;
  }

  private byte[] serializeAvro(int schemaId,
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
}
