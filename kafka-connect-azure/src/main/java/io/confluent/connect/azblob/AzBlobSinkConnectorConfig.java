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

package io.confluent.connect.azblob;

import io.confluent.connect.azblob.storage.AzBlobStorage;
import io.confluent.connect.s3.format.avro.AvroFormat;
import io.confluent.connect.storage.StorageSinkConnectorConfig;
import io.confluent.connect.storage.common.ComposableConfig;
import io.confluent.connect.storage.common.GenericRecommender;
import io.confluent.connect.storage.common.ParentValueRecommender;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.hive.HiveConfig;
import io.confluent.connect.storage.hive.schema.DefaultSchemaGenerator;
import io.confluent.connect.storage.hive.schema.TimeBasedSchemaGenerator;
import io.confluent.connect.storage.partitioner.DailyPartitioner;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.FieldPartitioner;
import io.confluent.connect.storage.partitioner.HourlyPartitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class AzBlobSinkConnectorConfig extends StorageSinkConnectorConfig {

  public static final String AZ_STORAGEACCOUNT_CONNECTION_STRING =
      "azblob.storageaccount.connectionstring";
  public static final String AZ_STORAGE_CONTAINER_NAME = "azblob.containername";

  public static final String AVRO_CODEC_CONFIG = "avro.codec";
  public static final String AVRO_CODEC_DEFAULT = "null";

  public static final String FORMAT_BYTEARRAY_EXTENSION_CONFIG = "format.bytearray.extension";
  public static final String FORMAT_BYTEARRAY_EXTENSION_DEFAULT = ".bin";

  public static final String FORMAT_BYTEARRAY_LINE_SEPARATOR_CONFIG = "format.bytearray.separator";
  public static final String FORMAT_BYTEARRAY_LINE_SEPARATOR_DEFAULT = System.lineSeparator();

  private final String name;

  private final StorageCommonConfig commonConfig;
  private final HiveConfig hiveConfig;
  private final PartitionerConfig partitionerConfig;

  private final Map<String, ComposableConfig> propertyToConfig = new HashMap<>();
  private final Set<AbstractConfig> allConfigs = new HashSet<>();

  private static final GenericRecommender STORAGE_CLASS_RECOMMENDER = new GenericRecommender();
  private static final GenericRecommender FORMAT_CLASS_RECOMMENDER = new GenericRecommender();
  private static final GenericRecommender PARTITIONER_CLASS_RECOMMENDER = new GenericRecommender();
  private static final GenericRecommender SCHEMA_GENERATOR_CLASS_RECOMMENDER =
      new GenericRecommender();
  private static final ParentValueRecommender AVRO_COMPRESSION_RECOMMENDER
      = new ParentValueRecommender(FORMAT_CLASS_CONFIG, AvroFormat.class, AVRO_SUPPORTED_CODECS);


  static {
    STORAGE_CLASS_RECOMMENDER.addValidValues(
        Arrays.<Object>asList(AzBlobStorage.class)
    );

    //    FORMAT_CLASS_RECOMMENDER.addValidValues(
    //        Arrays.<Object>asList(AvroFormat.class, JsonFormat.class)
    //    );

    PARTITIONER_CLASS_RECOMMENDER.addValidValues(
        Arrays.<Object>asList(
            DefaultPartitioner.class,
            HourlyPartitioner.class,
            DailyPartitioner.class,
            TimeBasedPartitioner.class,
            FieldPartitioner.class
        )
    );

    SCHEMA_GENERATOR_CLASS_RECOMMENDER.addValidValues(
        Arrays.<Object>asList(
            DefaultSchemaGenerator.class,
            TimeBasedSchemaGenerator.class
        )
    );

  }


  public static ConfigDef newConfigDef() {
    ConfigDef configDef = StorageSinkConnectorConfig.newConfigDef(
        FORMAT_CLASS_RECOMMENDER,
        AVRO_COMPRESSION_RECOMMENDER
    );
    {
      final String group = "AZ";
      int orderInGroup = 0;

      configDef.define(
          AZ_STORAGEACCOUNT_CONNECTION_STRING,
          Type.STRING,
          "default",
          Importance.MEDIUM,
          "The connection stirng.",
          group,
          ++orderInGroup,
          Width.LONG,
          "Connection String"
      );

      configDef.define(
          AZ_STORAGE_CONTAINER_NAME,
          Type.STRING,
          "default",
          Importance.MEDIUM,
          "The container name.",
          group,
          ++orderInGroup,
          Width.LONG,
          "Container name"
      );

      configDef.define(
          AVRO_CODEC_CONFIG,
          Type.STRING,
          AVRO_CODEC_DEFAULT,
          Importance.LOW,
          "The Avro compression codec to be used for output files. Available values: null, "
              + "deflate, snappy and bzip2 (codec source is org.apache.avro.file.CodecFactory)",
          group,
          ++orderInGroup,
          Width.LONG,
          "Avro compression codec"
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
              + "multiple records in the output S3 object.",
          group,
          ++orderInGroup,
          Width.LONG,
          "Line separator ByteArrayFormat"
      );
    }
    return configDef;
  }

  public AzBlobSinkConnectorConfig(Map<String, String> props) {
    this(newConfigDef(), props);
  }

  protected AzBlobSinkConnectorConfig(ConfigDef configDef, Map<String, String> props) {
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

  public String getAvroCodec() {
    return getString(AVRO_CODEC_CONFIG);
  }

  protected static String parseName(Map<String, String> props) {
    String nameProp = props.get("name");
    return nameProp != null ? nameProp : "azblob-sink";
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

  public String getStorageConnectionString() {
    return getString(AZ_STORAGEACCOUNT_CONNECTION_STRING);
  }

  public String getContainerName() {
    return getString(AZ_STORAGE_CONTAINER_NAME);
  }

  private static class PartRange implements ConfigDef.Validator {
    // AZ specific limit // TODO check this value
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
        throw new ConfigException(name, value,
            "Part size must be at least: " + min + " bytes (5MB)");
      }
      if (number.longValue() > max) {
        throw new ConfigException(name, value,
            "Part size must be no more: " + Integer.MAX_VALUE + " bytes (~2GB)");
      }
    }

    public String toString() {
      return "[" + min + ",...," + max + "]";
    }
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
