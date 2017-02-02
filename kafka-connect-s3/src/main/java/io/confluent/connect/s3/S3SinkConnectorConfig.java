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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.regions.Regions;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.confluent.connect.storage.StorageSinkConnectorConfig;
import io.confluent.connect.storage.common.ComposableConfig;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.hive.HiveConfig;
import io.confluent.connect.storage.partitioner.PartitionerConfig;

public class S3SinkConnectorConfig extends StorageSinkConnectorConfig {

  // S3 Group
  public static final String S3_BUCKET_CONFIG = "s3.bucket.name";

  public static final String SSEA_CONFIG = "s3.ssea.name";
  public static final String SSEA_DEFAULT = "";

  public static final String PART_SIZE_CONFIG = "s3.part.size";
  public static final int PART_SIZE_DEFAULT = 100 * 1024 * 1024;

  public static final String WAN_MODE_CONFIG = "s3.wan.mode";
  private static final boolean WAN_MODE_DEFAULT = false;

  public static final String CREDENTIALS_PROVIDER_CLASS_CONFIG = "s3.credentials.provider.class";
  public static final Class<? extends AWSCredentialsProvider> CREDENTIALS_PROVIDER_CLASS_DEFAULT =
      DefaultAWSCredentialsProviderChain.class;

  public static final String REGION_CONFIG = "s3.region";
  public static final String REGION_DEFAULT = Regions.DEFAULT_REGION.getName();

  private final String name;

  private final StorageCommonConfig commonConfig;
  private final HiveConfig hiveConfig;
  private final PartitionerConfig partitionerConfig;

  private final Map<String, ComposableConfig> propertyToConfig = new HashMap<>();
  private final Set<AbstractConfig> allConfigs = new HashSet<>();

  static {
    {
      final String group = "S3";
      int orderInGroup = 0;
      CONFIG_DEF.define(S3_BUCKET_CONFIG,
                        Type.STRING,
                        Importance.HIGH,
                        "The S3 Bucket.",
                        group,
                        ++orderInGroup,
                        Width.MEDIUM,
                        "S3 Bucket");

      CONFIG_DEF.define(PART_SIZE_CONFIG,
                        Type.INT,
                        PART_SIZE_DEFAULT,
                        ConfigDef.Range.between(1, Integer.MAX_VALUE),
                        Importance.HIGH,
                        "The Part Size in S3 Multi-part Uploads.",
                        group,
                        ++orderInGroup,
                        Width.MEDIUM,
                        "S3 Part Size");

      CONFIG_DEF.define(SSEA_CONFIG,
                        Type.STRING,
                        SSEA_DEFAULT,
                        Importance.LOW,
                        "The S3 Server Side Encryption Algorithm.",
                        group,
                        ++orderInGroup,
                        Width.MEDIUM,
                        "S3 Server Side Encryption Algorithm");

      CONFIG_DEF.define(WAN_MODE_CONFIG,
                        Type.BOOLEAN,
                        WAN_MODE_DEFAULT,
                        Importance.MEDIUM,
                        "Use S3 accelerated endpoint.",
                        group,
                        ++orderInGroup,
                        Width.LONG,
                        "S3 accelerated endpoint enabled.");

      CONFIG_DEF.define(REGION_CONFIG,
                        Type.STRING,
                        REGION_DEFAULT,
                        new RegionValidator(),
                        Importance.MEDIUM,
                        "The AWS region to be used the connector.",
                        group,
                        ++orderInGroup,
                        Width.LONG,
                        "AWS region",
                        new RegionRecommender());

      CONFIG_DEF.define(CREDENTIALS_PROVIDER_CLASS_CONFIG,
                        Type.CLASS,
                        CREDENTIALS_PROVIDER_CLASS_DEFAULT,
                        Importance.LOW,
                        "Credentials provider or provider chain to use for authentication to AWS. By default the "
                        + " connector uses 'DefaultAWSCredentialsProviderChain'.",
                        group,
                        ++orderInGroup,
                        Width.LONG,
                        "AWS Credentials Provider Class");
    }
  }

  public S3SinkConnectorConfig(Map<String, String> props) {
    this(CONFIG_DEF, props);
  }

  protected S3SinkConnectorConfig(ConfigDef configDef, Map<String, String> props) {
    super(configDef, props);
    commonConfig = new StorageCommonConfig(originalsStrings());
    hiveConfig = new HiveConfig(originalsStrings());
    partitionerConfig = new PartitionerConfig(originalsStrings());
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

  public String getSSEA() {
    return getString(SSEA_CONFIG);
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
      throw new ConnectException("Invalid class for: " + S3SinkConnectorConfig.CREDENTIALS_PROVIDER_CLASS_CONFIG, e);
    }
  }

  public ClientConfiguration getClientConfiguration() {
    // Currently return just the default.
    return new ClientConfiguration();
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
        throw new ConfigException(name, region, "Value must be one of: " + Utils.join(RegionUtils.getRegions(), ", "));
      }
    }

    @Override
    public String toString() {
      return "[" + Utils.join(RegionUtils.getRegions(), ", ") + "]";
    }
  }

  private static class CredentialsProviderValidator implements ConfigDef.Validator {
    @Override
    public void ensureValid(String name, Object provider) {
      if (provider != null && provider instanceof Class
              && AWSCredentialsProvider.class.isAssignableFrom((Class<?>) provider)) {
        return;
      }
      throw new ConfigException(name, provider, "Class must extend: " + AWSCredentialsProvider.class);
    }

    @Override
    public String toString() {
      return "Any class implementing: " + AWSCredentialsProvider.class;
    }
  }
}
