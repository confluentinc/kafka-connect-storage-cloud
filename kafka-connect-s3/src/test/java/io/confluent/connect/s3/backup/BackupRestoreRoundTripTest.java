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

public class BackupRestoreRoundTripTest {

  private static final String TOPIC = "round-trip-topic";

  @Test
  public void avroSimpleRoundTrip() throws Exception {
    SchemaRegistryClient sourceSR = new MockSchemaRegistryClient();
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder
        .record("User").namespace("com.example")
        .fields()
        .requiredString("name")
        .requiredInt("age")
        .endRecord();
    int schemaId = sourceSR.register(
        TOPIC + "-value", new AvroSchema(avroSchema));

    GenericRecord original = new GenericData.Record(avroSchema);
    original.put("name", "Alice");
    original.put("age", 30);
    byte[] wireBytes = serializeAvro(schemaId, avroSchema, original);

    AvroConverter backupConverter = createConverter(sourceSR, true);
    RecordHeaders headers = new RecordHeaders();
    SchemaAndValue backed = backupConverter.toConnectData(
        TOPIC, headers, wireBytes);

    assertTrue("Should be BackupWrapper",
        BackupWrapper.isWrapper(backed.schema()));
    Struct wrapper = (Struct) backed.value();
    assertEquals(schemaId,
        (int) wrapper.getInt32(BackupWrapper.FIELD_SCHEMA_ID));
    assertEquals("AVRO",
        wrapper.getString(BackupWrapper.FIELD_SCHEMA_TYPE));
    assertNotNull(wrapper.getString(BackupWrapper.FIELD_RAW_SCHEMA));

    SchemaRegistryClient targetSR = new MockSchemaRegistryClient();
    AvroConverter restoreConverter = createConverter(targetSR, false);
    byte[] restored = restoreConverter.fromConnectData(
        TOPIC, headers, backed.schema(), backed.value());

    assertNotNull("Restored bytes should not be null", restored);

    SchemaAndValue result = restoreConverter.toConnectData(
        TOPIC, headers, restored);
    assertNotNull(result.value());
    Struct resultStruct = (Struct) result.value();
    assertEquals("Alice", resultStruct.getString("name"));
    assertEquals(30, (int) resultStruct.getInt32("age"));
  }

  @Test
  public void avroWithDirectReference() throws Exception {
    SchemaRegistryClient sourceSR = new MockSchemaRegistryClient();

    String addressText = "{\"type\":\"record\","
        + "\"name\":\"Address\",\"namespace\":\"com.example\","
        + "\"fields\":["
        + "{\"name\":\"city\",\"type\":\"string\"}]}";
    AvroSchema addressSchema = new AvroSchema(addressText);
    sourceSR.register("address-value", addressSchema);

    SchemaReference addressRef = new SchemaReference(
        "com.example.Address", "address-value", 1);
    String userText = "{\"type\":\"record\","
        + "\"name\":\"User\",\"namespace\":\"com.example\","
        + "\"fields\":["
        + "{\"name\":\"name\",\"type\":\"string\"},"
        + "{\"name\":\"address\",\"type\":\"com.example.Address\"}"
        + "]}";
    AvroSchema userSchema = new AvroSchema(userText,
        Collections.singletonList(addressRef),
        Collections.singletonMap(
            "com.example.Address", addressText),
        null);
    int userId = sourceSR.register(TOPIC + "-value", userSchema);

    org.apache.avro.Schema parsed = userSchema.rawSchema();
    GenericRecord addressRec = new GenericData.Record(
        parsed.getField("address").schema());
    addressRec.put("city", "NYC");
    GenericRecord userRec = new GenericData.Record(parsed);
    userRec.put("name", "Alice");
    userRec.put("address", addressRec);

    byte[] wireBytes = serializeAvro(userId, parsed, userRec);

    AvroConverter backupConverter = createConverter(sourceSR, true);
    RecordHeaders headers = new RecordHeaders();
    SchemaAndValue backed = backupConverter.toConnectData(
        TOPIC, headers, wireBytes);

    Struct wrapper = (Struct) backed.value();
    assertNotNull("referenceTree should be present",
        wrapper.getString(BackupWrapper.FIELD_REFERENCE_TREE));
    assertNotNull("directRefs should be present",
        wrapper.getString(BackupWrapper.FIELD_DIRECT_REFS));

    SchemaRegistryClient targetSR = new MockSchemaRegistryClient();
    AvroConverter restoreConverter = createConverter(targetSR, false);
    byte[] restored = restoreConverter.fromConnectData(
        TOPIC, headers, backed.schema(), backed.value());

    assertNotNull("Restored bytes should not be null", restored);

    int targetAddrVersion = targetSR.getVersion(
        "address-value", new AvroSchema(addressText));
    assertTrue("Address should be registered in target SR",
        targetAddrVersion > 0);

    SchemaAndValue result = restoreConverter.toConnectData(
        TOPIC, headers, restored);
    Struct resultStruct = (Struct) result.value();
    assertEquals("Alice", resultStruct.getString("name"));
    Struct resultAddr = resultStruct.getStruct("address");
    assertEquals("NYC", resultAddr.getString("city"));
  }

  @Test
  public void avroWithNestedReferences() throws Exception {
    SchemaRegistryClient sourceSR = new MockSchemaRegistryClient();

    String countryText = "{\"type\":\"record\","
        + "\"name\":\"Country\",\"namespace\":\"com.example\","
        + "\"fields\":["
        + "{\"name\":\"code\",\"type\":\"string\"}]}";
    AvroSchema countrySchema = new AvroSchema(countryText);
    sourceSR.register("country-value", countrySchema);

    SchemaReference countryRef = new SchemaReference(
        "com.example.Country", "country-value", 1);
    String addressText = "{\"type\":\"record\","
        + "\"name\":\"Address\",\"namespace\":\"com.example\","
        + "\"fields\":["
        + "{\"name\":\"city\",\"type\":\"string\"},"
        + "{\"name\":\"country\","
        + "\"type\":\"com.example.Country\"}"
        + "]}";
    AvroSchema addressSchema = new AvroSchema(addressText,
        Collections.singletonList(countryRef),
        Collections.singletonMap(
            "com.example.Country", countryText),
        null);
    sourceSR.register("address-value", addressSchema);

    SchemaReference addressRef = new SchemaReference(
        "com.example.Address", "address-value", 1);
    String userText = "{\"type\":\"record\","
        + "\"name\":\"User\",\"namespace\":\"com.example\","
        + "\"fields\":["
        + "{\"name\":\"name\",\"type\":\"string\"},"
        + "{\"name\":\"address\","
        + "\"type\":\"com.example.Address\"}"
        + "]}";
    Map<String, String> allResolved = new HashMap<>();
    allResolved.put("com.example.Address", addressText);
    allResolved.put("com.example.Country", countryText);
    AvroSchema userSchema = new AvroSchema(userText,
        Collections.singletonList(addressRef),
        allResolved, null);
    int userId = sourceSR.register(TOPIC + "-value", userSchema);

    org.apache.avro.Schema parsed = userSchema.rawSchema();
    org.apache.avro.Schema addrParsed =
        parsed.getField("address").schema();
    GenericRecord countryRec = new GenericData.Record(
        addrParsed.getField("country").schema());
    countryRec.put("code", "US");
    GenericRecord addressRec = new GenericData.Record(addrParsed);
    addressRec.put("city", "NYC");
    addressRec.put("country", countryRec);
    GenericRecord userRec = new GenericData.Record(parsed);
    userRec.put("name", "Alice");
    userRec.put("address", addressRec);

    byte[] wireBytes = serializeAvro(userId, parsed, userRec);

    AvroConverter backupConverter = createConverter(sourceSR, true);
    RecordHeaders headers = new RecordHeaders();
    SchemaAndValue backed = backupConverter.toConnectData(
        TOPIC, headers, wireBytes);

    Struct wrapper = (Struct) backed.value();
    String treeJson = wrapper.getString(
        BackupWrapper.FIELD_REFERENCE_TREE);
    assertNotNull("referenceTree should exist", treeJson);
    assertTrue("Tree should contain Address",
        treeJson.contains("com.example.Address"));
    assertTrue("Tree should contain Country",
        treeJson.contains("com.example.Country"));

    SchemaRegistryClient targetSR = new MockSchemaRegistryClient();
    AvroConverter restoreConverter = createConverter(targetSR, false);
    byte[] restored = restoreConverter.fromConnectData(
        TOPIC, headers, backed.schema(), backed.value());

    assertNotNull(restored);

    int targetCountryV = targetSR.getVersion(
        "country-value", new AvroSchema(countryText));
    assertTrue("Country registered in target", targetCountryV > 0);

    SchemaAndValue result = restoreConverter.toConnectData(
        TOPIC, headers, restored);
    Struct resultStruct = (Struct) result.value();
    assertEquals("Alice", resultStruct.getString("name"));
    Struct addr = resultStruct.getStruct("address");
    assertEquals("NYC", addr.getString("city"));
    Struct country = addr.getStruct("country");
    assertEquals("US", country.getString("code"));
  }

  @Test
  public void schemaEvolution_threeVersions() throws Exception {
    SchemaRegistryClient sourceSR = new MockSchemaRegistryClient();

    org.apache.avro.Schema v1 = org.apache.avro.SchemaBuilder
        .record("User").namespace("com.example")
        .fields().requiredString("name").endRecord();
    org.apache.avro.Schema v2 = org.apache.avro.SchemaBuilder
        .record("User").namespace("com.example")
        .fields().requiredString("name").requiredInt("age")
        .endRecord();
    org.apache.avro.Schema v3 = org.apache.avro.SchemaBuilder
        .record("User").namespace("com.example")
        .fields().requiredString("name").requiredInt("age")
        .requiredString("email").endRecord();

    int id1 = sourceSR.register(TOPIC + "-value",
        new AvroSchema(v1));
    int id2 = sourceSR.register(TOPIC + "-value",
        new AvroSchema(v2));
    int id3 = sourceSR.register(TOPIC + "-value",
        new AvroSchema(v3));

    BackupSchemaFetcher fetcher = new BackupSchemaFetcher(sourceSR);
    assertEquals(Integer.valueOf(1),
        fetcher.fetchSchemaInfo(id1)
            .getVersionForSubject(TOPIC + "-value"));
    assertEquals(Integer.valueOf(2),
        fetcher.fetchSchemaInfo(id2)
            .getVersionForSubject(TOPIC + "-value"));
    assertEquals(Integer.valueOf(3),
        fetcher.fetchSchemaInfo(id3)
            .getVersionForSubject(TOPIC + "-value"));

    AvroConverter backupConverter = createConverter(sourceSR, true);
    SchemaRegistryClient targetSR = new MockSchemaRegistryClient();
    AvroConverter restoreConverter = createConverter(targetSR, false);
    RecordHeaders headers = new RecordHeaders();

    GenericRecord rec1 = new GenericData.Record(v1);
    rec1.put("name", "Alice");
    byte[] wire1 = serializeAvro(id1, v1, rec1);
    SchemaAndValue backed1 = backupConverter.toConnectData(
        TOPIC, headers, wire1);
    byte[] restored1 = restoreConverter.fromConnectData(
        TOPIC, headers, backed1.schema(), backed1.value());
    SchemaAndValue result1 = restoreConverter.toConnectData(
        TOPIC, headers, restored1);
    assertEquals("Alice",
        ((Struct) result1.value()).getString("name"));

    GenericRecord rec3 = new GenericData.Record(v3);
    rec3.put("name", "Charlie");
    rec3.put("age", 25);
    rec3.put("email", "c@test.com");
    byte[] wire3 = serializeAvro(id3, v3, rec3);
    SchemaAndValue backed3 = backupConverter.toConnectData(
        TOPIC, headers, wire3);
    byte[] restored3 = restoreConverter.fromConnectData(
        TOPIC, headers, backed3.schema(), backed3.value());
    SchemaAndValue result3 = restoreConverter.toConnectData(
        TOPIC, headers, restored3);
    Struct r3 = (Struct) result3.value();
    assertEquals("Charlie", r3.getString("name"));
    assertEquals(25, (int) r3.getInt32("age"));
    assertEquals("c@test.com", r3.getString("email"));
  }

  @Test
  public void tombstoneRoundTrip() throws Exception {
    SchemaRegistryClient sourceSR = new MockSchemaRegistryClient();
    AvroConverter backupConverter = createConverter(sourceSR, true);
    RecordHeaders headers = new RecordHeaders();

    SchemaAndValue backed = backupConverter.toConnectData(
        TOPIC, headers, null);

    assertEquals(SchemaAndValue.NULL, backed);

    SchemaRegistryClient targetSR = new MockSchemaRegistryClient();
    AvroConverter restoreConverter = createConverter(targetSR, false);
    byte[] restored = restoreConverter.fromConnectData(
        TOPIC, headers, null, null);

    assertNull("Tombstone should restore as null", restored);
  }

  @Test
  public void versionCorrectForMultipleSubjects() throws Exception {
    SchemaRegistryClient sourceSR = new MockSchemaRegistryClient();

    org.apache.avro.Schema schema = org.apache.avro.SchemaBuilder
        .record("Event").namespace("com.example")
        .fields().requiredString("data").endRecord();
    AvroSchema avro = new AvroSchema(schema);

    int idA = sourceSR.register("topic-a-value", avro);
    sourceSR.register("topic-b-value", avro);

    BackupSchemaFetcher fetcher = new BackupSchemaFetcher(sourceSR);
    BackupSchemaFetcher.BackupSchemaInfo info =
        fetcher.fetchSchemaInfo(idA);

    assertEquals(Integer.valueOf(1),
        info.getVersionForSubject("topic-a-value"));
    assertEquals(Integer.valueOf(1),
        info.getVersionForSubject("topic-b-value"));
    assertNull(info.getVersionForSubject("nonexistent"));
  }

  @Test
  public void wrapperFieldsComplete() throws Exception {
    SchemaRegistryClient sourceSR = new MockSchemaRegistryClient();
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder
        .record("Test").namespace("com.example")
        .fields().requiredString("x").endRecord();
    int schemaId = sourceSR.register(
        TOPIC + "-value", new AvroSchema(avroSchema));

    GenericRecord rec = new GenericData.Record(avroSchema);
    rec.put("x", "val");
    byte[] wireBytes = serializeAvro(
        schemaId, avroSchema, rec);

    AvroConverter converter = createConverter(sourceSR, true);
    RecordHeaders headers = new RecordHeaders();
    SchemaAndValue backed = converter.toConnectData(
        TOPIC, headers, wireBytes);

    assertTrue(BackupWrapper.isWrapper(backed.schema()));
    Struct w = (Struct) backed.value();

    assertNotNull("data", w.get(BackupWrapper.FIELD_DATA));
    assertEquals(schemaId,
        (int) w.getInt32(BackupWrapper.FIELD_SCHEMA_ID));
    assertNotNull("schemaVersion",
        w.getInt32(BackupWrapper.FIELD_SCHEMA_VERSION));
    assertEquals("AVRO",
        w.getString(BackupWrapper.FIELD_SCHEMA_TYPE));
    assertEquals(TOPIC + "-value",
        w.getString(BackupWrapper.FIELD_SCHEMA_SUBJECT));
    assertNotNull("rawSchema",
        w.getString(BackupWrapper.FIELD_RAW_SCHEMA));
  }

  // ==================== Helpers ====================

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
