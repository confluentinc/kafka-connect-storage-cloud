package io.confluent.connect.s3;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.confluent.connect.s3.format.avro.AvroFormat;
import io.confluent.connect.s3.format.bytearray.ByteArrayFormat;
import io.confluent.connect.s3.format.json.JsonFormat;
import io.confluent.connect.s3.format.parquet.ParquetFormat;
import io.confluent.connect.storage.StorageSinkConnectorConfig.Mode;
import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.format.SchemaFileReader;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.confluent.connect.s3.S3SinkConnectorConfig.COMPRESSION_TYPE_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.HEADERS_FORMAT_CLASS_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.KEYS_FORMAT_CLASS_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.STORE_KAFKA_HEADERS_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.STORE_KAFKA_KEYS_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorValidator.FORMAT_CONFIG_ERROR_MESSAGE;
import static io.confluent.connect.storage.StorageSinkConnectorConfig.FORMAT_CLASS_CONFIG;
import static io.confluent.connect.storage.StorageSinkConnectorConfig.MODE_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class S3SinkConnectorValidatorTest extends S3SinkConnectorTestBase{
  private static final String STRING_CONVERTER =
      "org.apache.kafka.connect.storage.StringConverter";
  private static final String AVRO_CONVERTER = "io.confluent.connect.avro.AvroConverter";
  private static final String KEY_CONVERTER_CONFIG = "key.converter";
  private static final String VALUE_CONVERTER_CONFIG = "value.converter";
  private static final String KEY_ENHANCED_AVRO = "key.converter.enhanced.avro.schema.support";
  private static final String KEY_SCHEMA_BACKUP_ENABLED = "key.converter.schema.backup.enabled";
  private static final String VALUE_ENHANCED_AVRO = "value.converter.enhanced.avro.schema.support";
  private static final String VALUE_SCHEMA_BACKUP_ENABLED =
      "value.converter.schema.backup.enabled";
  private static final String FORMAT_JSON_SCHEMA_ENABLE_CONFIG = "format.json.schema.enable";
  private static final String BYTE_ARRAY_FORMAT_ERROR_SNIPPET =
      "format.class=ByteArrayFormat cannot be used";
  private static final String MUST_BE_SET_EXPLICITLY_SNIPPET =
      "must be set explicitly at the connector";
  private static final String STORE_KAFKA_KEYS_ERROR_SNIPPET =
      "store.kafka.keys=true cannot be used";
  private static final String TRANSFORMS_CONFIG = "transforms";
  private static final String SMT_ALIAS = "Customer";
  private static final String CAST_KEY_SMT_TYPE =
      "org.apache.kafka.connect.transforms.Cast$Key";

  protected Map<String, String> localProps = new HashMap<>();
  private S3SinkConnectorValidator s3SinkConnectorValidator;

  private class CustomFormatClass implements Format<S3SinkConnectorConfig, String> {

    @Override
    public RecordWriterProvider<S3SinkConnectorConfig> getRecordWriterProvider() {
      return null;
    }

    @Override
    public SchemaFileReader<S3SinkConnectorConfig, String> getSchemaFileReader() {
      return null;
    }

    @Override
    @Deprecated
    public Object getHiveFactory() {
      return null;
    }
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    s3SinkConnectorValidator = new S3SinkConnectorValidator(
        S3SinkConnectorConfig.getConfig(), createProps(), createConfigValues());
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    localProps.clear();
  }

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.putAll(localProps);
    return props;
  }

  private List<ConfigValue> createConfigValues() {
    return S3SinkConnectorConfig.getConfig().validate(createProps());
  }

  @Test
  public void testValidate() {
    // FORMAT_CLASS, STORE_KEY, KEY_FORMAT, STORE_HEADER, HEADER_FORMAT, COMPRESSION_TYPE
    Set<List<String>> testCases = Sets.cartesianProduct(
        ImmutableSet.of(AvroFormat.class.getName(), ParquetFormat.class.getName(),
            JsonFormat.class.getName(), ByteArrayFormat.class.getName(),
            CustomFormatClass.class.getName()),
        ImmutableSet.of("true", "false"),
        ImmutableSet.of(AvroFormat.class.getName(), ParquetFormat.class.getName(),
            JsonFormat.class.getName(), ByteArrayFormat.class.getName()),
        ImmutableSet.of("true", "false"),
        ImmutableSet.of(AvroFormat.class.getName(), ParquetFormat.class.getName(),
            JsonFormat.class.getName(), ByteArrayFormat.class.getName()),
        ImmutableSet.of("none", "gzip")
    );

    Set<List<String>> noErrorCases = new HashSet<>();
    // None compression
    noErrorCases.addAll(Sets.cartesianProduct(
        ImmutableSet.of(AvroFormat.class.getName(), ParquetFormat.class.getName(),
            JsonFormat.class.getName(), ByteArrayFormat.class.getName(),
            CustomFormatClass.class.getName()),
        ImmutableSet.of("true", "false"),
        ImmutableSet.of(AvroFormat.class.getName(), ParquetFormat.class.getName(),
            JsonFormat.class.getName(), ByteArrayFormat.class.getName()),
        ImmutableSet.of("true", "false"),
        ImmutableSet.of(AvroFormat.class.getName(), ParquetFormat.class.getName(),
            JsonFormat.class.getName(), ByteArrayFormat.class.getName()),
        ImmutableSet.of("none")
    ));
    // Gzip compression with disable keys and headers and format class with json and bytes array
    noErrorCases.addAll(Sets.cartesianProduct(
        ImmutableSet.of(JsonFormat.class.getName(), ByteArrayFormat.class.getName()),
        ImmutableSet.of("false"),
        ImmutableSet.of(AvroFormat.class.getName(), ParquetFormat.class.getName(),
            JsonFormat.class.getName(), ByteArrayFormat.class.getName()),
        ImmutableSet.of("false"),
        ImmutableSet.of(AvroFormat.class.getName(), ParquetFormat.class.getName(),
            JsonFormat.class.getName(), ByteArrayFormat.class.getName()),
        ImmutableSet.of("gzip")
    ));
    // Gzip compression with keys and headers format as json and byte array format
    noErrorCases.addAll(Sets.cartesianProduct(
        ImmutableSet.of(JsonFormat.class.getName(), ByteArrayFormat.class.getName()),
        ImmutableSet.of("true"),
        ImmutableSet.of(JsonFormat.class.getName(), ByteArrayFormat.class.getName()),
        ImmutableSet.of("true"),
        ImmutableSet.of(JsonFormat.class.getName(), ByteArrayFormat.class.getName()),
        ImmutableSet.of("gzip")
    ));


    // Gzip compression with keys format as json and byte array format
    noErrorCases.addAll(Sets.cartesianProduct(
        ImmutableSet.of(JsonFormat.class.getName(), ByteArrayFormat.class.getName()),
        ImmutableSet.of("true"),
        ImmutableSet.of(JsonFormat.class.getName(), ByteArrayFormat.class.getName()),
        ImmutableSet.of("false"),
        ImmutableSet.of(AvroFormat.class.getName(), ParquetFormat.class.getName(),
            JsonFormat.class.getName(), ByteArrayFormat.class.getName()),
        ImmutableSet.of("gzip")
    ));

    // Gzip compression with headers format as json and byte array format
    noErrorCases.addAll(Sets.cartesianProduct(
        ImmutableSet.of(JsonFormat.class.getName(), ByteArrayFormat.class.getName()),
        ImmutableSet.of("false"),
        ImmutableSet.of(AvroFormat.class.getName(), ParquetFormat.class.getName(),
            JsonFormat.class.getName(), ByteArrayFormat.class.getName()),
        ImmutableSet.of("true"),
        ImmutableSet.of(JsonFormat.class.getName(), ByteArrayFormat.class.getName()),
        ImmutableSet.of("gzip")
    ));

    // data format Error cases
    Set<List<String>> dataErrorCases = Sets.cartesianProduct(
        ImmutableSet.of(AvroFormat.class.getName(), ParquetFormat.class.getName(),
            CustomFormatClass.class.getName()),
        ImmutableSet.of("true", "false"),
        ImmutableSet.of(AvroFormat.class.getName(), ParquetFormat.class.getName(),
            JsonFormat.class.getName(), ByteArrayFormat.class.getName()),
        ImmutableSet.of("true", "false"),
        ImmutableSet.of(AvroFormat.class.getName(), ParquetFormat.class.getName(),
            JsonFormat.class.getName(), ByteArrayFormat.class.getName()),
        ImmutableSet.of("gzip")
    );

    // Keys format Error cases
    Set<List<String>> keysErrorCases = Sets.cartesianProduct(
        ImmutableSet.of(AvroFormat.class.getName(), ParquetFormat.class.getName(),
            JsonFormat.class.getName(), ByteArrayFormat.class.getName(),
            CustomFormatClass.class.getName()),
        ImmutableSet.of("true"),
        ImmutableSet.of(AvroFormat.class.getName(), ParquetFormat.class.getName()),
        ImmutableSet.of("true", "false"),
        ImmutableSet.of(AvroFormat.class.getName(), ParquetFormat.class.getName(),
            JsonFormat.class.getName(), ByteArrayFormat.class.getName()),
        ImmutableSet.of("gzip")
    );

    // Headers format Error cases
    Set<List<String>> headersErrorCases = Sets.cartesianProduct(
        ImmutableSet.of(AvroFormat.class.getName(), ParquetFormat.class.getName(),
            JsonFormat.class.getName(), ByteArrayFormat.class.getName(),
            CustomFormatClass.class.getName()),
        ImmutableSet.of("true", "false"),
        ImmutableSet.of(AvroFormat.class.getName(), ParquetFormat.class.getName(),
            JsonFormat.class.getName(), ByteArrayFormat.class.getName()),
        ImmutableSet.of("true"),
        ImmutableSet.of(AvroFormat.class.getName(), ParquetFormat.class.getName()),
        ImmutableSet.of("gzip")
    );

    for (List<String> matrix: testCases) {
      localProps.put(FORMAT_CLASS_CONFIG, matrix.get(0));
      localProps.put(STORE_KAFKA_KEYS_CONFIG, matrix.get(1));
      localProps.put(KEYS_FORMAT_CLASS_CONFIG, matrix.get(2));
      localProps.put(STORE_KAFKA_HEADERS_CONFIG, matrix.get(3));
      localProps.put(S3SinkConnectorConfig.HEADERS_FORMAT_CLASS_CONFIG, matrix.get(4));
      localProps.put(S3SinkConnectorConfig.COMPRESSION_TYPE_CONFIG, matrix.get(5));
      s3SinkConnectorValidator = new S3SinkConnectorValidator(
          S3SinkConnectorConfig.getConfig(), createProps(), createConfigValues());
      Config configs = s3SinkConnectorValidator.validate();
      if (noErrorCases.contains(matrix)) {
        for(ConfigValue configValue: configs.configValues()) {
          for(String error: configValue.errorMessages()){
            System.out.println(error);
          }
          assertEquals(0, configValue.errorMessages().size());
        }
      } else {
        if(dataErrorCases.contains(matrix)) {
          assertContainError(
              String.format(FORMAT_CONFIG_ERROR_MESSAGE, matrix.get(5), "data", matrix.get(0)),
              FORMAT_CLASS_CONFIG, configs.configValues());
          assertContainError(
              String.format(FORMAT_CONFIG_ERROR_MESSAGE, matrix.get(5), "data", matrix.get(0)),
              COMPRESSION_TYPE_CONFIG, configs.configValues());
        }
        if(keysErrorCases.contains(matrix)) {
          assertContainError(
              String.format(FORMAT_CONFIG_ERROR_MESSAGE, matrix.get(5), "keys", matrix.get(2)),
              STORE_KAFKA_KEYS_CONFIG, configs.configValues());
          assertContainError(
              String.format(FORMAT_CONFIG_ERROR_MESSAGE, matrix.get(5), "keys", matrix.get(2)),
              KEYS_FORMAT_CLASS_CONFIG, configs.configValues());
          assertContainError(
              String.format(FORMAT_CONFIG_ERROR_MESSAGE, matrix.get(5), "keys", matrix.get(2)),
              COMPRESSION_TYPE_CONFIG, configs.configValues());
        }
        if(headersErrorCases.contains(matrix)) {
          assertContainError(
              String.format(FORMAT_CONFIG_ERROR_MESSAGE, matrix.get(5), "headers", matrix.get(4)),
              STORE_KAFKA_HEADERS_CONFIG, configs.configValues());
          assertContainError(
              String.format(FORMAT_CONFIG_ERROR_MESSAGE, matrix.get(5), "headers", matrix.get(4)),
              HEADERS_FORMAT_CLASS_CONFIG, configs.configValues());
          assertContainError(
              String.format(FORMAT_CONFIG_ERROR_MESSAGE, matrix.get(5), "headers", matrix.get(4)),
              COMPRESSION_TYPE_CONFIG, configs.configValues());
        }
      }

    }
  }

  @Test
  public void testValidateBackupModeSkippedWhenGeneric() {
    localProps.put(MODE_CONFIG, Mode.GENERIC.name());
    localProps.put(FORMAT_CLASS_CONFIG, AvroFormat.class.getName());
    s3SinkConnectorValidator = new S3SinkConnectorValidator(
        S3SinkConnectorConfig.getConfig(), createProps(), createConfigValues());

    Config configs = s3SinkConnectorValidator.validate();

    for (ConfigValue cv : configs.configValues()) {
      if (MODE_CONFIG.equals(cv.name()) || FORMAT_CLASS_CONFIG.equals(cv.name())) {
        assertEquals(
            "GENERIC mode should not surface backup-mode validation errors on " + cv.name(),
            0, cv.errorMessages().size());
      }
    }
  }

  @Test
  public void testValidateBackupModeAttachesByteArrayErrorToFormatClass() {
    localProps.put(MODE_CONFIG, Mode.BACKUP_FULL_RECORD.name());
    localProps.put(FORMAT_CLASS_CONFIG, ByteArrayFormat.class.getName());
    localProps.put(KEY_CONVERTER_CONFIG, STRING_CONVERTER);
    localProps.put(VALUE_CONVERTER_CONFIG, AVRO_CONVERTER);
    localProps.put(VALUE_ENHANCED_AVRO, "true");
    localProps.put(VALUE_SCHEMA_BACKUP_ENABLED, "true");
    s3SinkConnectorValidator = new S3SinkConnectorValidator(
        S3SinkConnectorConfig.getConfig(), createProps(), createConfigValues());

    Config configs = s3SinkConnectorValidator.validate();

    assertTrue(
        "expected ByteArrayFormat error to surface on format.class",
        anyErrorContains(configs, FORMAT_CLASS_CONFIG, BYTE_ARRAY_FORMAT_ERROR_SNIPPET));
  }

  @Test
  public void testValidateBackupModeMissingConverterAttachesToConverterKey() {
    localProps.put(MODE_CONFIG, Mode.BACKUP_FULL_RECORD.name());
    localProps.put(FORMAT_CLASS_CONFIG, AvroFormat.class.getName());
    s3SinkConnectorValidator = new S3SinkConnectorValidator(
        S3SinkConnectorConfig.getConfig(), createProps(), createConfigValues());

    Config configs = s3SinkConnectorValidator.validate();

    assertTrue(
        "expected key.converter-must-be-set error on key.converter",
        anyErrorContains(configs, KEY_CONVERTER_CONFIG, MUST_BE_SET_EXPLICITLY_SNIPPET));
    assertTrue(
        "expected value.converter-must-be-set error on value.converter",
        anyErrorContains(configs, VALUE_CONVERTER_CONFIG, MUST_BE_SET_EXPLICITLY_SNIPPET));
  }

  @Test
  public void testValidateBackupModeAttachesJsonSchemaEnableErrorToItsOwnKey() {
    localProps.put(MODE_CONFIG, Mode.BACKUP_FULL_RECORD.name());
    localProps.put(FORMAT_CLASS_CONFIG, JsonFormat.class.getName());
    localProps.put(KEY_CONVERTER_CONFIG, STRING_CONVERTER);
    localProps.put(VALUE_CONVERTER_CONFIG, STRING_CONVERTER);
    s3SinkConnectorValidator = new S3SinkConnectorValidator(
        S3SinkConnectorConfig.getConfig(), createProps(), createConfigValues());

    Config configs = s3SinkConnectorValidator.validate();

    assertTrue(
        "expected json-schema-enable error on its own key",
        anyErrorContains(configs, FORMAT_JSON_SCHEMA_ENABLE_CONFIG,
            FORMAT_JSON_SCHEMA_ENABLE_CONFIG));
  }

  @Test
  public void testValidateBackupModeStoreKafkaKeysErrorAttachesToStoreKafkaKeys() {
    localProps.put(MODE_CONFIG, Mode.BACKUP_FULL_RECORD.name());
    localProps.put(FORMAT_CLASS_CONFIG, AvroFormat.class.getName());
    localProps.put(KEY_CONVERTER_CONFIG, STRING_CONVERTER);
    localProps.put(VALUE_CONVERTER_CONFIG, AVRO_CONVERTER);
    localProps.put(VALUE_ENHANCED_AVRO, "true");
    localProps.put(VALUE_SCHEMA_BACKUP_ENABLED, "true");
    localProps.put(STORE_KAFKA_KEYS_CONFIG, "true");
    s3SinkConnectorValidator = new S3SinkConnectorValidator(
        S3SinkConnectorConfig.getConfig(), createProps(), createConfigValues());

    Config configs = s3SinkConnectorValidator.validate();

    assertTrue(
        "expected store.kafka.keys error on store.kafka.keys",
        anyErrorContains(configs, STORE_KAFKA_KEYS_CONFIG, STORE_KAFKA_KEYS_ERROR_SNIPPET));
  }

  @Test
  public void testValidateBackupModeConverterSubKeysAttachToConverterUmbrella() {
    localProps.put(MODE_CONFIG, Mode.BACKUP_FULL_RECORD.name());
    localProps.put(FORMAT_CLASS_CONFIG, AvroFormat.class.getName());
    localProps.put(KEY_CONVERTER_CONFIG, AVRO_CONVERTER);
    localProps.put(KEY_SCHEMA_BACKUP_ENABLED, "true");
    localProps.put(VALUE_CONVERTER_CONFIG, AVRO_CONVERTER);
    localProps.put(VALUE_ENHANCED_AVRO, "true");
    localProps.put(VALUE_SCHEMA_BACKUP_ENABLED, "true");
    // key.converter.enhanced.avro.schema.support deliberately not set
    s3SinkConnectorValidator = new S3SinkConnectorValidator(
        S3SinkConnectorConfig.getConfig(), createProps(), createConfigValues());

    Config configs = s3SinkConnectorValidator.validate();

    assertTrue(
        "expected key.converter.enhanced.avro.schema.support error on key.converter",
        anyErrorContains(configs, KEY_CONVERTER_CONFIG, KEY_ENHANCED_AVRO));
  }

  @Test
  public void testValidateBackupModeErrorOnFrameworkKeyPreservesUserValue() {
    localProps.put(MODE_CONFIG, Mode.BACKUP_FULL_RECORD.name());
    localProps.put(FORMAT_CLASS_CONFIG, AvroFormat.class.getName());
    localProps.put(KEY_CONVERTER_CONFIG, STRING_CONVERTER);
    localProps.put(VALUE_CONVERTER_CONFIG, AVRO_CONVERTER);
    localProps.put(VALUE_ENHANCED_AVRO, "true");
    localProps.put(VALUE_SCHEMA_BACKUP_ENABLED, "true");
    localProps.put(TRANSFORMS_CONFIG, SMT_ALIAS);
    localProps.put(TRANSFORMS_CONFIG + "." + SMT_ALIAS + ".type", CAST_KEY_SMT_TYPE);
    s3SinkConnectorValidator = new S3SinkConnectorValidator(
        S3SinkConnectorConfig.getConfig(), createProps(), createConfigValues());

    Config configs = s3SinkConnectorValidator.validate();

    ConfigValue transforms = configs.configValues().stream()
        .filter(cv -> TRANSFORMS_CONFIG.equals(cv.name()))
        .findFirst()
        .orElseThrow(() -> new AssertionError("no ConfigValue for transforms"));
    assertEquals(SMT_ALIAS, transforms.value());
    assertTrue("expected SMT rejection error on transforms",
        transforms.errorMessages().stream().anyMatch(m -> m.contains("SMT")));
  }

  private boolean anyErrorContains(Config configs, String field, String needle) {
    return configs.configValues().stream()
        .filter(cv -> field.equals(cv.name()))
        .flatMap(cv -> cv.errorMessages().stream())
        .anyMatch(msg -> msg.contains(needle));
  }

  private void assertContainError(String message, String field, List<ConfigValue> configValues) {
    configValues.stream().filter(cv -> cv.name().equals(field)).forEach(cv->
        cv.errorMessages().stream().filter(
            e -> e.equals(message)).findAny().orElseThrow(
                () -> new AssertionError("No error found with message " + message)));
  }
}
