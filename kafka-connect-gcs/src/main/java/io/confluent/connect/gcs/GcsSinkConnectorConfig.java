/*
 * Copyright 2017 Confluent Inc.
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

package io.confluent.connect.gcs;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import io.confluent.connect.gcs.format.avro.AvroFormat;
import io.confluent.connect.gcs.format.json.JsonFormat;
import io.confluent.connect.gcs.storage.GcsStorage;
import io.confluent.connect.storage.StorageSinkConnectorConfig;
import io.confluent.connect.storage.common.ComposableConfig;
import io.confluent.connect.storage.common.GenericRecommender;
import io.confluent.connect.storage.common.ParentValueRecommender;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.hive.HiveConfig;
import io.confluent.connect.storage.partitioner.DailyPartitioner;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.FieldPartitioner;
import io.confluent.connect.storage.partitioner.HourlyPartitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;

public class GcsSinkConnectorConfig extends StorageSinkConnectorConfig {

  // GCS Group
  public static final String GCS_BUCKET_CONFIG = "gcs.bucket.name";

  public static final String PART_SIZE_CONFIG = "gcs.part.size";
  public static final int PART_SIZE_DEFAULT = 25 * 1024 * 1024;

  public static final String GCS_PART_RETRIES_CONFIG = "gcs.part.retries";
  public static final int GCS_PART_RETRIES_DEFAULT = 3;

  public static final String FORMAT_BYTEARRAY_EXTENSION_CONFIG = "format.bytearray.extension";
  public static final String FORMAT_BYTEARRAY_EXTENSION_DEFAULT = ".bin";

  public static final String FORMAT_BYTEARRAY_LINE_SEPARATOR_CONFIG = "format.bytearray.separator";
  public static final String FORMAT_BYTEARRAY_LINE_SEPARATOR_DEFAULT = System.lineSeparator();

  public static final String GCS_PROXY_URL_CONFIG = "gcs.proxy.url";
  public static final String GCS_PROXY_URL_DEFAULT = "";

  public static final String GCS_PROXY_USER_CONFIG = "gcs.proxy.user";
  public static final String GCS_PROXY_USER_DEFAULT = null;

  public static final String GCS_PROXY_PASS_CONFIG = "gcs.proxy.password";
  public static final Password GCS_PROXY_PASS_DEFAULT = new Password(null);

  private final String name;

  private final StorageCommonConfig commonConfig;
  private final HiveConfig hiveConfig;
  private final PartitionerConfig partitionerConfig;

  private final Map<String, ComposableConfig> propertyToConfig = new HashMap<>();
  private final Set<AbstractConfig> allConfigs = new HashSet<>();

  private static final GenericRecommender STORAGE_CLASS_RECOMMENDER = new GenericRecommender();
  private static final GenericRecommender FORMAT_CLASS_RECOMMENDER = new GenericRecommender();
  private static final GenericRecommender PARTITIONER_CLASS_RECOMMENDER = new GenericRecommender();
  private static final ParentValueRecommender AVRO_COMPRESSION_RECOMMENDER
      = new ParentValueRecommender(FORMAT_CLASS_CONFIG, AvroFormat.class, AVRO_SUPPORTED_CODECS);

  static {
    STORAGE_CLASS_RECOMMENDER.addValidValues(
        Arrays.<Object>asList(GcsStorage.class)
    );

    FORMAT_CLASS_RECOMMENDER.addValidValues(
        Arrays.<Object>asList(AvroFormat.class, JsonFormat.class)
    );

    PARTITIONER_CLASS_RECOMMENDER.addValidValues(
        Arrays.<Object>asList(
            DefaultPartitioner.class,
            HourlyPartitioner.class,
            DailyPartitioner.class,
            TimeBasedPartitioner.class,
            FieldPartitioner.class
        )
    );
  }

  public static ConfigDef newConfigDef() {
    ConfigDef configDef = StorageSinkConnectorConfig.newConfigDef(
        FORMAT_CLASS_RECOMMENDER,
        AVRO_COMPRESSION_RECOMMENDER
    );
    {
      final String group = "GCS";
      int orderInGroup = 0;

      configDef.define(
          GCS_BUCKET_CONFIG,
          Type.STRING,
          Importance.HIGH,
          "The GCS Bucket.",
          group,
          ++orderInGroup,
          Width.LONG,
          "GCS Bucket"
      );

      configDef.define(
          PART_SIZE_CONFIG,
          Type.INT,
          PART_SIZE_DEFAULT,
          new PartRange(),
          Importance.HIGH,
          "The Part Size in GCS Multi-part Uploads.",
          group,
          ++orderInGroup,
          Width.LONG,
          "GCS Part Size"
      );

      configDef.define(
          GCS_PART_RETRIES_CONFIG,
          Type.INT,
          GCS_PART_RETRIES_DEFAULT,
          Importance.MEDIUM,
          "Number of upload retries of a single GCS part. Zero means no retries.",
          group,
          ++orderInGroup,
          Width.LONG,
          "GCS Part Upload Retries"
      );

      configDef.define(
          FORMAT_BYTEARRAY_EXTENSION_CONFIG,
          Type.STRING,
          FORMAT_BYTEARRAY_EXTENSION_DEFAULT,
          Importance.LOW,
          String.format(
              "Output file extension for ByteArrayFormat. Defaults to '%s'",
              FORMAT_BYTEARRAY_EXTENSION_DEFAULT
          ),
          group,
          ++orderInGroup,
          Width.LONG,
          "Output file extension for ByteArrayFormat"
      );

      configDef.define(
          FORMAT_BYTEARRAY_LINE_SEPARATOR_CONFIG,
          Type.STRING,
          // Because ConfigKey automatically trims strings, we cannot set
          // the default here and instead inject null;
          // the default is applied in getFormatByteArrayLineSeparator().
          null,
          Importance.LOW,
          "String inserted between records for ByteArrayFormat. "
              + "Defaults to 'System.lineSeparator()' "
              + "and may contain escape sequences like '\\n'. "
              + "An input record that contains the line separator will look like "
              + "multiple records in the output GCS object.",
          group,
          ++orderInGroup,
          Width.LONG,
          "Line separator ByteArrayFormat"
      );

      configDef.define(
          GCS_PROXY_URL_CONFIG,
          Type.STRING,
          GCS_PROXY_URL_DEFAULT,
          Importance.LOW,
          "GCS Proxy settings encoded in URL syntax. This property is meant to be used only if you"
              + " need to access GCS through a proxy.",
          group,
          ++orderInGroup,
          Width.LONG,
          "GCS Proxy Settings"
      );

      configDef.define(
          GCS_PROXY_USER_CONFIG,
          Type.STRING,
          GCS_PROXY_USER_DEFAULT,
          Importance.LOW,
          "GCS Proxy User. This property is meant to be used only if you"
              + " need to access GCS through a proxy. Using ``"
              + GCS_PROXY_USER_CONFIG
              + "`` instead of embedding the username and password in ``"
              + GCS_PROXY_URL_CONFIG
              + "`` allows the password to be hidden in the logs.",
          group,
          ++orderInGroup,
          Width.LONG,
          "GCS Proxy User"
      );

      configDef.define(
          GCS_PROXY_PASS_CONFIG,
          Type.PASSWORD,
          GCS_PROXY_PASS_DEFAULT,
          Importance.LOW,
          "GCS Proxy Password. This property is meant to be used only if you"
              + " need to access GCS through a proxy. Using ``"
              + GCS_PROXY_PASS_CONFIG
              + "`` instead of embedding the username and password in ``"
              + GCS_PROXY_URL_CONFIG
              + "`` allows the password to be hidden in the logs.",
          group,
          ++orderInGroup,
          Width.LONG,
          "GCS Proxy Password"
      );
    }
    return configDef;
  }

  public GcsSinkConnectorConfig(Map<String, String> props) {
    this(newConfigDef(), props);
  }

  protected GcsSinkConnectorConfig(ConfigDef configDef, Map<String, String> props) {
    super(configDef, props);
    ConfigDef storageCommonConfigDef = StorageCommonConfig.newConfigDef(STORAGE_CLASS_RECOMMENDER);
    commonConfig = new StorageCommonConfig(storageCommonConfigDef, originalsStrings());
    hiveConfig = new HiveConfig(originalsStrings());
    ConfigDef partitionerConfigDef = PartitionerConfig.newConfigDef(PARTITIONER_CLASS_RECOMMENDER);
    partitionerConfig = new PartitionerConfig(partitionerConfigDef, originalsStrings());

    this.name = parseName(originalsStrings());
    addToGlobal(hiveConfig);
    addToGlobal(partitionerConfig);
    addToGlobal(commonConfig);
    addToGlobal(this);
  }

  private void addToGlobal(AbstractConfig config) {
    allConfigs.add(config);
    addConfig(config.values(), (ComposableConfig) config);
  }

  private void addConfig(Map<String, ?> parsedProps, ComposableConfig config) {
    for (String key : parsedProps.keySet()) {
      propertyToConfig.put(key, config);
    }
  }

  public String getBucketName() {
    return getString(GCS_BUCKET_CONFIG);
  }

  public int getPartSize() {
    return getInt(PART_SIZE_CONFIG);
  }

  public int getGcsPartRetries() {
    return getInt(GCS_PART_RETRIES_CONFIG);
  }

  public String getByteArrayExtension() {
    return getString(FORMAT_BYTEARRAY_EXTENSION_CONFIG);
  }

  public String getFormatByteArrayLineSeparator() {
    // White space is significant for line separators, but ConfigKey trims it out,
    // so we need to check the originals rather than using the normal machinery.
    if (originalsStrings().containsKey(FORMAT_BYTEARRAY_LINE_SEPARATOR_CONFIG)) {
      return originalsStrings().get(FORMAT_BYTEARRAY_LINE_SEPARATOR_CONFIG);
    }
    return FORMAT_BYTEARRAY_LINE_SEPARATOR_DEFAULT;
  }

  protected static String parseName(Map<String, String> props) {
    String nameProp = props.get("name");
    return nameProp != null ? nameProp : "GCS-sink";
  }

  public String getName() {
    return name;
  }

  @Override
  public Object get(String key) {
    ComposableConfig config = propertyToConfig.get(key);
    if (config == null) {
      throw new ConfigException(String.format("Unknown configuration '%s'", key));
    }
    return config == this ? super.get(key) : config.get(key);
  }

  public Map<String, ?> plainValues() {
    Map<String, Object> map = new HashMap<>();
    for (AbstractConfig config : allConfigs) {
      map.putAll(config.values());
    }
    return map;
  }

  private static class PartRange implements ConfigDef.Validator {
    // GCS specific limit
    final int min = 5 * 1024 * 1024;
    // Connector specific
    final int max = Integer.MAX_VALUE;

    @Override
    public void ensureValid(String name, Object value) {
      if (value == null) {
        throw new ConfigException(name, value, "Part size must be non-null");
      }
      Number number = (Number) value;
      if (number.longValue() < min) {
        throw new ConfigException(
            name,
            value,
            "Part size must be at least: " + min + " bytes (5MB)"
        );
      }
      if (number.longValue() > max) {
        throw new ConfigException(
            name,
            value,
            "Part size must be no more: " + Integer.MAX_VALUE + " bytes (~2GB)"
        );
      }
    }

    public String toString() {
      return "[" + min + ",...," + max + "]";
    }
  }

  public static ConfigDef getConfig() {
    // Define the names of the configurations we're going to override
    Set<String> skip = new HashSet<>();
    skip.add(StorageSinkConnectorConfig.SHUTDOWN_TIMEOUT_CONFIG);

    // Order added is important, so that group order is maintained
    ConfigDef visible = new ConfigDef();
    addAllConfigKeys(visible, newConfigDef(), skip);
    addAllConfigKeys(visible, StorageCommonConfig.newConfigDef(STORAGE_CLASS_RECOMMENDER), skip);
    addAllConfigKeys(visible, PartitionerConfig.newConfigDef(PARTITIONER_CLASS_RECOMMENDER), skip);

    return visible;
  }

  private static void addAllConfigKeys(ConfigDef container, ConfigDef other, Set<String> skip) {
    for (ConfigDef.ConfigKey key : other.configKeys().values()) {
      if (skip != null && !skip.contains(key.name)) {
        container.define(key);
      }
    }
  }

  public static void main(String[] args) {
    System.out.println(getConfig().toEnrichedRst());
  }

}
