/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.connect.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;

import io.confluent.connect.s3.format.bytearray.ByteArrayFormat;
import io.confluent.connect.s3.format.parquet.ParquetFormat;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.json.DecimalFormat;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.confluent.connect.s3.auth.AwsAssumeRoleCredentialsProvider;
import io.confluent.connect.s3.format.avro.AvroFormat;
import io.confluent.connect.s3.format.json.JsonFormat;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.partitioner.DailyPartitioner;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.FieldPartitioner;
import io.confluent.connect.storage.partitioner.HourlyPartitioner;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;
import io.confluent.connect.avro.AvroDataConfig;

import static io.confluent.connect.s3.S3SinkConnectorConfig.AffixType;
import static io.confluent.connect.s3.S3SinkConnectorConfig.DECIMAL_FORMAT_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.DECIMAL_FORMAT_DEFAULT;
import static io.confluent.connect.s3.S3SinkConnectorConfig.HEADERS_FORMAT_CLASS_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.KEYS_FORMAT_CLASS_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.SCHEMA_PARTITION_AFFIX_TYPE_CONFIG;

import static io.confluent.connect.s3.S3SinkConnectorConfig.SEND_DIGEST_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class S3SinkConnectorConfigTest extends S3SinkConnectorTestBase {

  protected Map<String, String> localProps = new HashMap<>();

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
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

  @Test
  public void testStorageClass() {
    // No real test case yet
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertEquals(
        S3Storage.class,
        connectorConfig.getClass(StorageCommonConfig.STORAGE_CLASS_CONFIG)
    );
  }

  @Test
  public void testUndefinedURL() {
    properties.remove(StorageCommonConfig.STORE_URL_CONFIG);
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertNull(connectorConfig.getString(StorageCommonConfig.STORE_URL_CONFIG));
  }

  @Test
  public void testRecommendedValues() {
    List<Object> expectedStorageClasses = Arrays.<Object>asList(S3Storage.class);
    List<Object> expectedFormatClasses = Arrays.<Object>asList(
        AvroFormat.class,
        JsonFormat.class,
        ByteArrayFormat.class,
        ParquetFormat.class
    );
    List<Object> expectedPartitionerClasses = Arrays.<Object>asList(
        DefaultPartitioner.class,
        HourlyPartitioner.class,
        DailyPartitioner.class,
        TimeBasedPartitioner.class,
        FieldPartitioner.class
    );
    List<Object> expectedSchemaPartitionerAffixTypes = Arrays.stream(
        S3SinkConnectorConfig.AffixType.names()).collect(Collectors.toList());

    List<ConfigValue> values = S3SinkConnectorConfig.getConfig().validate(properties);
    for (ConfigValue val : values) {
      switch (val.name()) {
        case StorageCommonConfig.STORAGE_CLASS_CONFIG:
          assertEquals(expectedStorageClasses, val.recommendedValues());
          break;
        case S3SinkConnectorConfig.FORMAT_CLASS_CONFIG:
          assertEquals(expectedFormatClasses, val.recommendedValues());
          break;
        case PartitionerConfig.PARTITIONER_CLASS_CONFIG:
          assertEquals(expectedPartitionerClasses, val.recommendedValues());
          break;
        case SCHEMA_PARTITION_AFFIX_TYPE_CONFIG:
          assertEquals(expectedSchemaPartitionerAffixTypes, val.recommendedValues());
          break;
      }
    }
  }

  @Test
  public void testAvroDataConfigSupported() {
    properties.put(AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, "true");
    properties.put(AvroDataConfig.CONNECT_META_DATA_CONFIG, "false");
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertEquals(true, connectorConfig.get(AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG));
    assertEquals(false, connectorConfig.get(AvroDataConfig.CONNECT_META_DATA_CONFIG));
  }

  @Test
  public void testVisibilityForPartitionerClassDependentConfigs() {
    properties.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, DefaultPartitioner.class.getName());
    List<ConfigValue> values = S3SinkConnectorConfig.getConfig().validate(properties);
    assertDefaultPartitionerVisibility(values);

    properties.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, FieldPartitioner.class.getName());
    assertFieldPartitionerVisibility();

    properties.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, DailyPartitioner.class.getName());
    values = S3SinkConnectorConfig.getConfig().validate(properties);
    assertTimeBasedPartitionerVisibility(values);

    properties.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, HourlyPartitioner.class.getName());
    values = S3SinkConnectorConfig.getConfig().validate(properties);
    assertTimeBasedPartitionerVisibility(values);

    properties.put(
        PartitionerConfig.PARTITIONER_CLASS_CONFIG,
        TimeBasedPartitioner.class.getName()
    );
    values = S3SinkConnectorConfig.getConfig().validate(properties);
    assertNullPartitionerVisibility(values);

    Partitioner<?> klass = new Partitioner<Object>() {
      @Override
      public void configure(Map<String, Object> config) {}

      @Override
      public String encodePartition(SinkRecord sinkRecord) {
        return null;
      }

      @Override
      public String generatePartitionedPath(String topic, String encodedPartition) {
        return null;
      }

      @Override
      public List<Object> partitionFields() {
        throw new UnsupportedOperationException(
            "Hive integration is not currently supported in S3 Connector"
        );
      }
    };

    properties.put(
        PartitionerConfig.PARTITIONER_CLASS_CONFIG,
        klass.getClass().getName()
    );
    values = S3SinkConnectorConfig.getConfig().validate(properties);
    assertNullPartitionerVisibility(values);
  }

  @Test
  public void testConfigurableCredentialProvider() {
    final String ACCESS_KEY_VALUE = "AKIAAAAAKKKKIIIIAAAA";
    final String SECRET_KEY_VALUE = "WhoIsJohnGalt?";

    properties.put(
        S3SinkConnectorConfig.CREDENTIALS_PROVIDER_CLASS_CONFIG,
        DummyAssertiveCredentialsProvider.class.getName()
    );
    String configPrefix = S3SinkConnectorConfig.CREDENTIALS_PROVIDER_CONFIG_PREFIX;
    properties.put(
        configPrefix.concat(DummyAssertiveCredentialsProvider.ACCESS_KEY_NAME),
        ACCESS_KEY_VALUE
    );
    properties.put(
        configPrefix.concat(DummyAssertiveCredentialsProvider.SECRET_KEY_NAME),
        SECRET_KEY_VALUE
    );
    properties.put(
        configPrefix.concat(DummyAssertiveCredentialsProvider.CONFIGS_NUM_KEY_NAME),
        "5"
    );
    connectorConfig = new S3SinkConnectorConfig(properties);

    AWSCredentialsProvider credentialsProvider = connectorConfig.getCredentialsProvider();

    assertEquals(ACCESS_KEY_VALUE, credentialsProvider.getCredentials().getAWSAccessKeyId());
    assertEquals(SECRET_KEY_VALUE, credentialsProvider.getCredentials().getAWSSecretKey());
  }

  @Test
  public void testConfigurableAwsAssumeRoleCredentialsProvider() {
    properties.put(
        S3SinkConnectorConfig.CREDENTIALS_PROVIDER_CLASS_CONFIG,
        AwsAssumeRoleCredentialsProvider.class.getName()
    );
    String configPrefix = S3SinkConnectorConfig.CREDENTIALS_PROVIDER_CONFIG_PREFIX;
    properties.put(
        configPrefix.concat(AwsAssumeRoleCredentialsProvider.ROLE_ARN_CONFIG),
        "arn:aws:iam::012345678901:role/my-restricted-role"
    );
    properties.put(
        configPrefix.concat(AwsAssumeRoleCredentialsProvider.ROLE_SESSION_NAME_CONFIG),
        "my-session-name"
    );
    properties.put(
        configPrefix.concat(AwsAssumeRoleCredentialsProvider.ROLE_EXTERNAL_ID_CONFIG),
        "my-external-id"
    );
    connectorConfig = new S3SinkConnectorConfig(properties);

    AwsAssumeRoleCredentialsProvider credentialsProvider =
        (AwsAssumeRoleCredentialsProvider) connectorConfig.getCredentialsProvider();
  }

  @Test
  public void testUseExpectContinueDefault() throws Exception {
    setUp();
    S3Storage storage = new S3Storage(connectorConfig, url, S3_TEST_BUCKET_NAME, null);
    ClientConfiguration clientConfig = storage.newClientConfiguration(connectorConfig);
    assertEquals(true, clientConfig.isUseExpectContinue());
  }

  @Test
  public void testUseExpectContinueFalse() throws Exception {
    localProps.put(S3SinkConnectorConfig.HEADERS_USE_EXPECT_CONTINUE_CONFIG, "false");
    setUp();
    S3Storage storage = new S3Storage(connectorConfig, url, S3_TEST_BUCKET_NAME, null);
    ClientConfiguration clientConfig = storage.newClientConfiguration(connectorConfig);
    assertEquals(false, clientConfig.isUseExpectContinue());
  }

  @Test
  public void testConfigurableCredentialProviderMissingConfigs() {
    String configPrefix = S3SinkConnectorConfig.CREDENTIALS_PROVIDER_CONFIG_PREFIX;
    properties.put(
        S3SinkConnectorConfig.CREDENTIALS_PROVIDER_CLASS_CONFIG,
        DummyAssertiveCredentialsProvider.class.getName()
    );
    properties.put(
        configPrefix.concat(DummyAssertiveCredentialsProvider.CONFIGS_NUM_KEY_NAME),
        "2"
    );

    connectorConfig = new S3SinkConnectorConfig(properties);
    assertThrows(
        "are mandatory configuration properties",
        ConfigException.class,
        () -> connectorConfig.getCredentialsProvider()
    );
  }

  @Test
  public void testConfigurableAwsAssumeRoleCredentialsProviderMissingConfigs() {
    properties.put(
        S3SinkConnectorConfig.CREDENTIALS_PROVIDER_CLASS_CONFIG,
        AwsAssumeRoleCredentialsProvider.class.getName()
    );
    String configPrefix = S3SinkConnectorConfig.CREDENTIALS_PROVIDER_CONFIG_PREFIX;
    properties.put(
        configPrefix.concat(AwsAssumeRoleCredentialsProvider.ROLE_ARN_CONFIG),
        "arn:aws:iam::012345678901:role/my-restricted-role"
    );
    properties.put(
        configPrefix.concat(AwsAssumeRoleCredentialsProvider.ROLE_SESSION_NAME_CONFIG),
        "my-session-name"
    );
    properties.put(
        configPrefix.concat(AwsAssumeRoleCredentialsProvider.ROLE_EXTERNAL_ID_CONFIG),
        "my-external-id"
    );
    connectorConfig = new S3SinkConnectorConfig(properties);

    AwsAssumeRoleCredentialsProvider credentialsProvider =
        (AwsAssumeRoleCredentialsProvider) connectorConfig.getCredentialsProvider();

    assertThrows(
        "Missing required configuration",
        ConfigException.class,
        () -> credentialsProvider.configure(properties)
    );
  }

  @Test
  public void testConfigurableS3ObjectTaggingConfigs() {
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertEquals(false, connectorConfig.get(S3SinkConnectorConfig.S3_OBJECT_TAGGING_CONFIG));

    properties.put(S3SinkConnectorConfig.S3_OBJECT_TAGGING_CONFIG, "true");
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertEquals(true, connectorConfig.get(S3SinkConnectorConfig.S3_OBJECT_TAGGING_CONFIG));
    assertEquals(new ArrayList<String>(), connectorConfig.get(S3SinkConnectorConfig.S3_OBJECT_TAGGING_EXTRA_KV));

    properties.put(S3SinkConnectorConfig.S3_OBJECT_TAGGING_EXTRA_KV, "key1:value1,key2:value2");
    List<String> expectedConfigKeyValuePair = new ArrayList<String>() {{
      add("key1:value1");
      add("key2:value2");
    }};
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertEquals(expectedConfigKeyValuePair, connectorConfig.get(S3SinkConnectorConfig.S3_OBJECT_TAGGING_EXTRA_KV));

    properties.put(S3SinkConnectorConfig.S3_OBJECT_TAGGING_CONFIG, "false");
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertEquals(false, connectorConfig.get(S3SinkConnectorConfig.S3_OBJECT_TAGGING_CONFIG));

    properties.put(S3SinkConnectorConfig.S3_OBJECT_BEHAVIOR_ON_TAGGING_ERROR_CONFIG, "ignore");
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertEquals("ignore",
            connectorConfig.get(S3SinkConnectorConfig.S3_OBJECT_BEHAVIOR_ON_TAGGING_ERROR_CONFIG));

    properties.put(S3SinkConnectorConfig.S3_OBJECT_BEHAVIOR_ON_TAGGING_ERROR_CONFIG, "fail");
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertEquals("fail",
            connectorConfig.get(S3SinkConnectorConfig.S3_OBJECT_BEHAVIOR_ON_TAGGING_ERROR_CONFIG));
  }

  private void assertDefaultPartitionerVisibility(List<ConfigValue> values) {
    for (ConfigValue val : values) {
      switch (val.name()) {
        case PartitionerConfig.PARTITION_FIELD_NAME_CONFIG:
        case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
        case PartitionerConfig.PATH_FORMAT_CONFIG:
        case PartitionerConfig.LOCALE_CONFIG:
        case PartitionerConfig.TIMEZONE_CONFIG:
          assertFalse(val.visible());
          break;
      }
    }
  }

  private void assertFieldPartitionerVisibility() {
    List<ConfigValue> values;
    values = S3SinkConnectorConfig.getConfig().validate(properties);
    for (ConfigValue val : values) {
      switch (val.name()) {
        case PartitionerConfig.PARTITION_FIELD_NAME_CONFIG:
          assertTrue(val.visible());
          break;
        case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
        case PartitionerConfig.PATH_FORMAT_CONFIG:
        case PartitionerConfig.LOCALE_CONFIG:
        case PartitionerConfig.TIMEZONE_CONFIG:
          assertFalse(val.visible());
          break;
      }
    }
  }

  private void assertTimeBasedPartitionerVisibility(List<ConfigValue> values) {
    for (ConfigValue val : values) {
      switch (val.name()) {
        case PartitionerConfig.PARTITION_FIELD_NAME_CONFIG:
        case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
        case PartitionerConfig.PATH_FORMAT_CONFIG:
          assertFalse(val.visible());
          break;
        case PartitionerConfig.LOCALE_CONFIG:
        case PartitionerConfig.TIMEZONE_CONFIG:
          assertTrue(val.visible());
          break;
      }
    }
  }

  private void assertNullPartitionerVisibility(List<ConfigValue> values) {
    for (ConfigValue val : values) {
      switch (val.name()) {
        case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
        case PartitionerConfig.PATH_FORMAT_CONFIG:
        case PartitionerConfig.LOCALE_CONFIG:
        case PartitionerConfig.TIMEZONE_CONFIG:
          assertTrue(val.visible());
          break;
      }
    }
  }

  @Test(expected = ConfigException.class)
  public void testS3PartRetriesNegative() {
    properties.put(S3SinkConnectorConfig.S3_PART_RETRIES_CONFIG, "-1");
    connectorConfig = new S3SinkConnectorConfig(properties);
  }

  @Test(expected = ConfigException.class)
  public void testS3RetryBackoffNegative() {
    properties.put(S3SinkConnectorConfig.S3_RETRY_BACKOFF_CONFIG, "-1");
    connectorConfig = new S3SinkConnectorConfig(properties);
  }

  @Test(expected = ConfigException.class)
  public void testInvalidHighCompressionLevel() {
    properties.put(S3SinkConnectorConfig.COMPRESSION_LEVEL_CONFIG, "10");
    connectorConfig = new S3SinkConnectorConfig(properties);
  }

  @Test(expected = ConfigException.class)
  public void testInvalidLowCompressionLevel() {
    properties.put(S3SinkConnectorConfig.COMPRESSION_LEVEL_CONFIG, "-2");
    connectorConfig = new S3SinkConnectorConfig(properties);
  }

  @Test
  public void testJsonDecimalFormat() {
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertEquals(DecimalFormat.BASE64.name(), connectorConfig.getJsonDecimalFormat());

    properties.put(S3SinkConnectorConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name());
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertEquals(DecimalFormat.NUMERIC.name(), connectorConfig.getJsonDecimalFormat());
  }

  @Test
  public void testValidCompressionLevels() {
    IntStream.range(-1, 9).boxed().forEach(i -> {
          properties.put(S3SinkConnectorConfig.COMPRESSION_LEVEL_CONFIG, String.valueOf(i));
          connectorConfig = new S3SinkConnectorConfig(properties);
          assertEquals((int) i, connectorConfig.getCompressionLevel());
        }
    );
  }

  @Test
  public void testParquetCompressionTypeSupported() {
    properties.put(S3SinkConnectorConfig.PARQUET_CODEC_CONFIG, "none");
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertEquals(CompressionCodecName.UNCOMPRESSED, connectorConfig.parquetCompressionCodecName());

    properties.put(S3SinkConnectorConfig.PARQUET_CODEC_CONFIG, "gzip");
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertEquals(CompressionCodecName.GZIP, connectorConfig.parquetCompressionCodecName());

    properties.put(S3SinkConnectorConfig.PARQUET_CODEC_CONFIG, "snappy");
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertEquals(CompressionCodecName.SNAPPY, connectorConfig.parquetCompressionCodecName());

    properties.put(S3SinkConnectorConfig.PARQUET_CODEC_CONFIG, "lz4");
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertEquals(CompressionCodecName.LZ4, connectorConfig.parquetCompressionCodecName());

    properties.put(S3SinkConnectorConfig.PARQUET_CODEC_CONFIG, "zstd");
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertEquals(CompressionCodecName.ZSTD, connectorConfig.parquetCompressionCodecName());

    properties.put(S3SinkConnectorConfig.PARQUET_CODEC_CONFIG, "brotli");
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertEquals(CompressionCodecName.BROTLI, connectorConfig.parquetCompressionCodecName());

    properties.put(S3SinkConnectorConfig.PARQUET_CODEC_CONFIG, "lzo");
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertEquals(CompressionCodecName.LZO, connectorConfig.parquetCompressionCodecName());
  }

  @Test(expected = ConfigException.class)
  public void testUnsupportedParquetCompressionType() {
    properties.put(S3SinkConnectorConfig.PARQUET_CODEC_CONFIG, "uncompressed");
    connectorConfig = new S3SinkConnectorConfig(properties);
    connectorConfig.parquetCompressionCodecName();
  }

  @Test
  public void testValidTimezoneWithScheduleIntervalAccepted() {
    properties.put(PartitionerConfig.TIMEZONE_CONFIG, "CET");
    properties.put(S3SinkConnectorConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG, "30");
    new S3SinkConnectorConfig(properties);
  }

  @Test(expected = ConfigException.class)
  public void testEmptyTimezoneThrowsExceptionOnScheduleInterval() {
    properties.put(PartitionerConfig.TIMEZONE_CONFIG, PartitionerConfig.TIMEZONE_DEFAULT);
    properties.put(S3SinkConnectorConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG, "30");
    new S3SinkConnectorConfig(properties);
  }

  @Test
  public void testEmptyTimezoneExceptionMessage() {
    properties.put(PartitionerConfig.TIMEZONE_CONFIG, PartitionerConfig.TIMEZONE_DEFAULT);
    properties.put(S3SinkConnectorConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG, "30");
    String expectedError = String.format(
        "%s configuration must be set when using %s",
        PartitionerConfig.TIMEZONE_CONFIG,
        S3SinkConnectorConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG
    );
    try {
      new S3SinkConnectorConfig(properties);
    } catch (ConfigException e) {
      assertEquals(expectedError, e.getMessage());
    }
  }

  @Test
  public void testKeyStorageDefaultFalse() {
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertFalse(connectorConfig.getBoolean(S3SinkConnectorConfig.STORE_KAFKA_KEYS_CONFIG));
  }

  @Test
  public void testKeyStorageSupported() {
    properties.put(S3SinkConnectorConfig.STORE_KAFKA_KEYS_CONFIG, "true");
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertTrue(connectorConfig.getBoolean(S3SinkConnectorConfig.STORE_KAFKA_KEYS_CONFIG));
  }

  @Test
  public void testKeyFormatClassDefault() {
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertEquals(AvroFormat.class, connectorConfig.getClass(KEYS_FORMAT_CLASS_CONFIG));
  }

  @Test
  public void testKeyFormatClassSupported() {
    properties.put(KEYS_FORMAT_CLASS_CONFIG, AvroFormat.class.getCanonicalName());
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertEquals(AvroFormat.class, connectorConfig.getClass(KEYS_FORMAT_CLASS_CONFIG));

    properties.put(KEYS_FORMAT_CLASS_CONFIG, JsonFormat.class.getCanonicalName());
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertEquals(JsonFormat.class, connectorConfig.getClass(KEYS_FORMAT_CLASS_CONFIG));

    properties.put(KEYS_FORMAT_CLASS_CONFIG, ByteArrayFormat.class.getCanonicalName());
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertEquals(ByteArrayFormat.class, connectorConfig.getClass(KEYS_FORMAT_CLASS_CONFIG));

    properties.put(KEYS_FORMAT_CLASS_CONFIG, ParquetFormat.class.getCanonicalName());
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertEquals(ParquetFormat.class, connectorConfig.getClass(KEYS_FORMAT_CLASS_CONFIG));
  }

  @Test
  public void testHeaderStorageDefaultFalse() {
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertFalse(connectorConfig.getBoolean(S3SinkConnectorConfig.STORE_KAFKA_HEADERS_CONFIG));
  }

  @Test
  public void testHeaderStorageSupported() {
    properties.put(S3SinkConnectorConfig.STORE_KAFKA_HEADERS_CONFIG, "true");
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertTrue(connectorConfig.getBoolean(S3SinkConnectorConfig.STORE_KAFKA_HEADERS_CONFIG));
  }

  @Test
  public void testHeaderFormatClassDefault() {
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertEquals(AvroFormat.class, connectorConfig.getClass(HEADERS_FORMAT_CLASS_CONFIG));
  }

  @Test
  public void testHeaderFormatClassSupported() {
    properties.put(HEADERS_FORMAT_CLASS_CONFIG, AvroFormat.class.getCanonicalName());
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertEquals(AvroFormat.class, connectorConfig.getClass(HEADERS_FORMAT_CLASS_CONFIG));

    properties.put(HEADERS_FORMAT_CLASS_CONFIG, JsonFormat.class.getCanonicalName());
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertEquals(JsonFormat.class, connectorConfig.getClass(HEADERS_FORMAT_CLASS_CONFIG));

    properties.put(HEADERS_FORMAT_CLASS_CONFIG, ByteArrayFormat.class.getCanonicalName());
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertEquals(ByteArrayFormat.class, connectorConfig.getClass(HEADERS_FORMAT_CLASS_CONFIG));

    properties.put(HEADERS_FORMAT_CLASS_CONFIG, ParquetFormat.class.getCanonicalName());
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertEquals(ParquetFormat.class, connectorConfig.getClass(HEADERS_FORMAT_CLASS_CONFIG));
  }

  @Test
  public void testSchemaPartitionerAffixTypDefault() {
    properties.remove(SCHEMA_PARTITION_AFFIX_TYPE_CONFIG);
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertEquals(AffixType.NONE, connectorConfig.getSchemaPartitionAffixType());
  }

  @Test
  public void testSchemaPartitionerAffixType() {
    properties.put(SCHEMA_PARTITION_AFFIX_TYPE_CONFIG, AffixType.NONE.name());
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertEquals(AffixType.NONE, connectorConfig.getSchemaPartitionAffixType());

    properties.put(SCHEMA_PARTITION_AFFIX_TYPE_CONFIG, AffixType.PREFIX.name());
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertEquals(AffixType.PREFIX, connectorConfig.getSchemaPartitionAffixType());

    properties.put(SCHEMA_PARTITION_AFFIX_TYPE_CONFIG, AffixType.SUFFIX.name());
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertEquals(S3SinkConnectorConfig.AffixType.SUFFIX, connectorConfig.getSchemaPartitionAffixType());
  }

  @Test(expected = ConfigException.class)
  public void testSchemaPartitionerAffixTypeExceptionOnWrongValue() {
    properties.put(SCHEMA_PARTITION_AFFIX_TYPE_CONFIG, "Random");
    new S3SinkConnectorConfig(properties);
  }

  @Test
  public void testSendDigestConfigDefault() {
    properties.remove(SEND_DIGEST_CONFIG);
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertFalse(connectorConfig.isSendDigestEnabled());
  }

  @Test
  public void testSendDigestConfig() {
    properties.put(SEND_DIGEST_CONFIG, "true");
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertTrue(connectorConfig.isSendDigestEnabled());

    properties.put(SEND_DIGEST_CONFIG, "false");
    connectorConfig = new S3SinkConnectorConfig(properties);
    assertFalse(connectorConfig.isSendDigestEnabled());
  }

  @Test(expected = ConfigException.class)
  public void testSendDigestConfigOnWrongValue() {
    properties.put(SEND_DIGEST_CONFIG, "Random");
    new S3SinkConnectorConfig(properties);
  }
}

