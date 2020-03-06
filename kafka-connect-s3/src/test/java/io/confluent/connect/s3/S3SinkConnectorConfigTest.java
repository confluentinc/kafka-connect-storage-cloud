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
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class S3SinkConnectorConfigTest extends S3SinkConnectorTestBase {

  protected Map<String, String> localProps = new HashMap<>();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

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
        JsonFormat.class
    );
    List<Object> expectedPartitionerClasses = Arrays.<Object>asList(
        DefaultPartitioner.class,
        HourlyPartitioner.class,
        DailyPartitioner.class,
        TimeBasedPartitioner.class,
        FieldPartitioner.class
    );

    List<ConfigValue> values = S3SinkConnectorConfig.getConfig().validate(properties);
    for (ConfigValue val : values) {
      if (val.value() instanceof Class) {
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
        }
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
        "3"
    );
    connectorConfig = new S3SinkConnectorConfig(properties);

    AWSCredentialsProvider credentialsProvider = connectorConfig.getCredentialsProvider();

    assertEquals(ACCESS_KEY_VALUE, credentialsProvider.getCredentials().getAWSAccessKeyId());
    assertEquals(SECRET_KEY_VALUE, credentialsProvider.getCredentials().getAWSSecretKey());
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

    thrown.expect(ConfigException.class);
    thrown.expectMessage("are mandatory configuration properties");

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
    connectorConfig.getCredentialsProvider();
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
  public void testValidCompressionLevels() {
    IntStream.range(-1, 9).boxed().forEach(i -> {
          properties.put(S3SinkConnectorConfig.COMPRESSION_LEVEL_CONFIG, String.valueOf(i));
          connectorConfig = new S3SinkConnectorConfig(properties);
          assertEquals((int) i, connectorConfig.getCompressionLevel());
        }
    );
  }
}

