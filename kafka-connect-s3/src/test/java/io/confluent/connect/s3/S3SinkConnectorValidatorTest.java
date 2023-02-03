package io.confluent.connect.s3;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.confluent.connect.s3.format.avro.AvroFormat;
import io.confluent.connect.s3.format.bytearray.ByteArrayFormat;
import io.confluent.connect.s3.format.json.JsonFormat;
import io.confluent.connect.s3.format.parquet.ParquetFormat;
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
import static org.junit.Assert.assertEquals;

public class S3SinkConnectorValidatorTest extends S3SinkConnectorTestBase{
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

  private void assertContainError(String message, String field, List<ConfigValue> configValues) {
    configValues.stream().filter(cv -> cv.name().equals(field)).forEach(cv->
        cv.errorMessages().stream().filter(
            e -> e.equals(message)).findAny().orElseThrow(
                () -> new AssertionError("No error found with message " + message)));
  }
}
