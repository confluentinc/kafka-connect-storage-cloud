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

package io.confluent.connect.s3;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.confluent.connect.s3.format.avro.AvroFormat;
import io.confluent.connect.s3.format.json.JsonFormat;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.StorageSinkConnectorConfig;
import io.confluent.connect.storage.common.ComposableConfig;
import io.confluent.connect.storage.common.GenericRecommender;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.hive.HiveConfig;
import io.confluent.connect.storage.partitioner.DailyPartitioner;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.FieldPartitioner;
import io.confluent.connect.storage.partitioner.HourlyPartitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;

public class S3SinkConnectorConfig extends StorageSinkConnectorConfig {

  // S3 Group
  public static final String S3_BUCKET_CONFIG = "s3.bucket.name";

  public static final String SSEA_CONFIG = "s3.ssea.name";
  public static final String SSEA_DEFAULT = "";

  public static final String PART_SIZE_CONFIG = "s3.part.size";
  public static final int PART_SIZE_DEFAULT = 25 * 1024 * 1024;

  public static final String WAN_MODE_CONFIG = "s3.wan.mode";
  private static final boolean WAN_MODE_DEFAULT = false;

  public static final String CREDENTIALS_PROVIDER_CLASS_CONFIG = "s3.credentials.provider.class";
  public static final Class<? extends AWSCredentialsProvider> CREDENTIALS_PROVIDER_CLASS_DEFAULT =
      DefaultAWSCredentialsProviderChain.class;

  public static final String REGION_CONFIG = "s3.region";
  public static final String REGION_DEFAULT = Regions.DEFAULT_REGION.getName();

  public static final String ACL_CANNED_CONFIG = "s3.acl.canned";
  public static final String ACL_CANNED_DEFAULT = null;

  public static final String AVRO_CODEC_CONFIG = "avro.codec";
  public static final String AVRO_CODEC_DEFAULT = "null";

  public static final String S3_PART_RETRIES_CONFIG = "s3.part.retries";
  public static final int S3_PART_RETRIES_DEFAULT = 3;

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

  static {
    STORAGE_CLASS_RECOMMENDER.addValidValues(
        Arrays.<Object>asList(S3Storage.class)
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
    ConfigDef configDef = StorageSinkConnectorConfig.newConfigDef(FORMAT_CLASS_RECOMMENDER);
    {
      final String group = "S3";
      int orderInGroup = 0;

      configDef.define(
          S3_BUCKET_CONFIG,
          Type.STRING,
          Importance.HIGH,
          "The S3 Bucket.",
          group,
          ++orderInGroup,
          Width.LONG,
          "S3 Bucket"
      );

      configDef.define(
          REGION_CONFIG,
          Type.STRING,
          REGION_DEFAULT,
          new RegionValidator(),
          Importance.MEDIUM,
          "The AWS region to be used the connector.",
          group,
          ++orderInGroup,
          Width.LONG,
          "AWS region",
          new RegionRecommender()
      );

      configDef.define(
          PART_SIZE_CONFIG,
          Type.INT,
          PART_SIZE_DEFAULT,
          new PartRange(),
          Importance.HIGH,
          "The Part Size in S3 Multi-part Uploads.",
          group,
          ++orderInGroup,
          Width.LONG,
          "S3 Part Size"
      );

      configDef.define(
          CREDENTIALS_PROVIDER_CLASS_CONFIG,
          Type.CLASS,
          CREDENTIALS_PROVIDER_CLASS_DEFAULT,
          new CredentialsProviderValidator(),
          Importance.LOW,
          "Credentials provider or provider chain to use for authentication to AWS. By default "
              + "the connector uses 'DefaultAWSCredentialsProviderChain'.",
          group,
          ++orderInGroup,
          Width.LONG,
          "AWS Credentials Provider Class"
      );

      configDef.define(
          SSEA_CONFIG,
          Type.STRING,
          SSEA_DEFAULT,
          Importance.LOW,
          "The S3 Server Side Encryption Algorithm.",
          group,
          ++orderInGroup,
          Width.LONG,
          "S3 Server Side Encryption Algorithm"
      );

      configDef.define(
          ACL_CANNED_CONFIG,
          Type.STRING,
          ACL_CANNED_DEFAULT,
          new CannedAclValidator(),
          Importance.LOW,
          "An S3 canned ACL header value to apply when writing objects.",
          group,
          ++orderInGroup,
          Width.LONG,
          "S3 Canned ACL"
      );

      configDef.define(
          WAN_MODE_CONFIG,
          Type.BOOLEAN,
          WAN_MODE_DEFAULT,
          Importance.MEDIUM,
          "Use S3 accelerated endpoint.",
          group,
          ++orderInGroup,
          Width.LONG,
          "S3 accelerated endpoint enabled"
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
          S3_PART_RETRIES_CONFIG,
          Type.INT,
          S3_PART_RETRIES_DEFAULT,
          Importance.MEDIUM,
          "Number of upload retries of a single S3 part. Zero means no retries.",
          group,
          ++orderInGroup,
          Width.LONG,
          "S3 Part Upload Retries"
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

  public S3SinkConnectorConfig(Map<String, String> props) {
    this(newConfigDef(), props);
  }

  protected S3SinkConnectorConfig(ConfigDef configDef, Map<String, String> props) {
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
    return getString(S3_BUCKET_CONFIG);
  }

  public String getSsea() {
    return getString(SSEA_CONFIG);
  }

  public CannedAccessControlList getCannedAcl() {
    return CannedAclValidator.ACLS_BY_HEADER_VALUE.get(getString(ACL_CANNED_CONFIG));
  }

  public int getPartSize() {
    return getInt(PART_SIZE_CONFIG);
  }

  @SuppressWarnings("unchecked")
  public AWSCredentialsProvider getCredentialsProvider() {
    try {
      return ((Class<? extends AWSCredentialsProvider>)
                  getClass(S3SinkConnectorConfig.CREDENTIALS_PROVIDER_CLASS_CONFIG)).newInstance();
    } catch (IllegalAccessException | InstantiationException e) {
      throw new ConnectException(
          "Invalid class for: " + S3SinkConnectorConfig.CREDENTIALS_PROVIDER_CLASS_CONFIG,
          e
      );
    }
  }

  public String getAvroCodec() {
    return getString(AVRO_CODEC_CONFIG);
  }

  public int getS3PartRetries() {
    return getInt(S3_PART_RETRIES_CONFIG);
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
    return nameProp != null ? nameProp : "S3-sink";
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
    // S3 specific limit
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

  private static class RegionRecommender implements ConfigDef.Recommender {
    @Override
    public List<Object> validValues(String name, Map<String, Object> connectorConfigs) {
      return Arrays.<Object>asList(RegionUtils.getRegions());
    }

    @Override
    public boolean visible(String name, Map<String, Object> connectorConfigs) {
      return true;
    }
  }

  private static class RegionValidator implements ConfigDef.Validator {
    @Override
    public void ensureValid(String name, Object region) {
      String regionStr = ((String) region).toLowerCase().trim();
      if (RegionUtils.getRegion(regionStr) == null) {
        throw new ConfigException(
            name,
            region,
            "Value must be one of: " + Utils.join(RegionUtils.getRegions(), ", ")
        );
      }
    }

    @Override
    public String toString() {
      return "[" + Utils.join(RegionUtils.getRegions(), ", ") + "]";
    }
  }

  private static class CannedAclValidator implements ConfigDef.Validator {
    public static final Map<String, CannedAccessControlList> ACLS_BY_HEADER_VALUE = new HashMap<>();
    public static final String ALLOWED_VALUES;

    static {
      List<String> aclHeaderValues = new ArrayList<>();
      for (CannedAccessControlList acl : CannedAccessControlList.values()) {
        ACLS_BY_HEADER_VALUE.put(acl.toString(), acl);
        aclHeaderValues.add(acl.toString());
      }
      ALLOWED_VALUES = Utils.join(aclHeaderValues, ", ");
    }

    @Override
    public void ensureValid(String name, Object cannedAcl) {
      if (cannedAcl == null) {
        return;
      }
      String aclStr = ((String) cannedAcl).trim();
      if (!ACLS_BY_HEADER_VALUE.containsKey(aclStr)) {
        throw new ConfigException(name, cannedAcl, "Value must be one of: " + ALLOWED_VALUES);
      }
    }

    @Override
    public String toString() {
      return "[" + ALLOWED_VALUES + "]";
    }
  }

  private static class CredentialsProviderValidator implements ConfigDef.Validator {
    @Override
    public void ensureValid(String name, Object provider) {
      if (provider != null && provider instanceof Class
              && AWSCredentialsProvider.class.isAssignableFrom((Class<?>) provider)) {
        return;
      }
      throw new ConfigException(
          name,
          provider,
          "Class must extend: " + AWSCredentialsProvider.class
      );
    }

    @Override
    public String toString() {
      return "Any class implementing: " + AWSCredentialsProvider.class;
    }
  }

  public static ConfigDef getConfig() {
    Map<String, ConfigDef.ConfigKey> everything = new HashMap<>(newConfigDef().configKeys());
    everything.putAll(StorageCommonConfig.newConfigDef(STORAGE_CLASS_RECOMMENDER).configKeys());
    everything.putAll(PartitionerConfig.newConfigDef(PARTITIONER_CLASS_RECOMMENDER).configKeys());

    Set<String> blacklist = new HashSet<>();
    blacklist.add(StorageSinkConnectorConfig.SHUTDOWN_TIMEOUT_CONFIG);

    ConfigDef visible = new ConfigDef();
    for (ConfigDef.ConfigKey key : everything.values()) {
      if (!blacklist.contains(key.name)) {
        visible.define(key);
      }
    }
    return visible;
  }

  public static void main(String[] args) {
    System.out.println(getConfig().toEnrichedRst());
  }

}
