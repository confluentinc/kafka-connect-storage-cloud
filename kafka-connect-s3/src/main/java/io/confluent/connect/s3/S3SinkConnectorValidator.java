/*
 * Copyright 2022 Confluent Inc.
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

import io.confluent.connect.s3.format.bytearray.ByteArrayFormat;
import io.confluent.connect.s3.format.json.JsonFormat;
import io.confluent.connect.s3.storage.CompressionType;
import io.confluent.connect.storage.format.Format;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static io.confluent.connect.s3.S3SinkConnectorConfig.COMPRESSION_TYPE_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.HEADERS_FORMAT_CLASS_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.KEYS_FORMAT_CLASS_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.S3_PATH_STYLE_ACCESS_ENABLED_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.STORE_KAFKA_HEADERS_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.STORE_KAFKA_KEYS_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.WAN_MODE_CONFIG;
import static io.confluent.connect.storage.StorageSinkConnectorConfig.FORMAT_CLASS_CONFIG;

public class S3SinkConnectorValidator {

  private static final Logger log = LoggerFactory.getLogger(S3SinkConnectorValidator.class);

  public static final Map<CompressionType, Set<Class<? extends Format>>>
      COMPRESSION_SUPPORTED_FORMATS = Collections.unmodifiableMap(
      new HashMap<CompressionType, Set<Class<? extends Format>>>() {
          {
            put(CompressionType.GZIP, new HashSet<>(Arrays.asList(
                    JsonFormat.class,
                    ByteArrayFormat.class)));
          }
      }
    );

  public static final String FORMAT_CONFIG_ERROR_MESSAGE = "Compression Type %s "
      + "not valid for %s format class: ( %s ).";

  private static final String ACCESS_MODE_ERROR_MESSAGE = "Both accelerate mode and path style "
      + "access are being enabled. These options are mutually exclusive and cannot be enabled "
      + "together. Please disable one of them";

  private final Map<String, String> connectorConfigs;
  private final ConfigDef config;

  protected final Map<String, ConfigValue> valuesByKey;

  public S3SinkConnectorValidator(
        ConfigDef config, Map<String, String> connectorConfigs, List<ConfigValue> configValues) {
    this.config = config;
    this.connectorConfigs = connectorConfigs;
    valuesByKey = new HashMap<>();
    for (ConfigValue configValue: configValues) {
      valuesByKey.put(configValue.name(), configValue);
    }
  }

  public Config validate() {
    log.info("Validating s3 Configs");
    S3SinkConnectorConfig s3SinkConnectorConfig = null;
    try {
      s3SinkConnectorConfig = new S3SinkConnectorConfig(config, connectorConfigs);
    } catch (ConfigException exception) {
      log.trace("Configuration not ready for cross validation.");
    }
    if (s3SinkConnectorConfig != null) {
      validateCompression(
          s3SinkConnectorConfig.getCompressionType(), s3SinkConnectorConfig.formatClass(),
          s3SinkConnectorConfig.storeKafkaKeys(), s3SinkConnectorConfig.keysFormatClass(),
          s3SinkConnectorConfig.storeKafkaHeaders(), s3SinkConnectorConfig.headersFormatClass()
      );

      validateWanModeAndPathStyleCompatibility(s3SinkConnectorConfig);
    }

    return new Config(new ArrayList<>(this.valuesByKey.values()));
  }

  public void validateCompression(CompressionType compressionType, Class formatClass,
        boolean storeKafkaKeys, Class keysFormatClass,
        boolean storeKafkaHeaders, Class headersFormatClass) {
    if (!compressionType.equals(CompressionType.NONE)) {
      Set<Class<? extends Format>> validFormats = COMPRESSION_SUPPORTED_FORMATS.get(
          compressionType);
      if (!validFormats.contains(formatClass)) {
        recordErrors(
            String.format(FORMAT_CONFIG_ERROR_MESSAGE,
                    compressionType.name, "data", formatClass.getName()),
            FORMAT_CLASS_CONFIG, COMPRESSION_TYPE_CONFIG);
      }

      if (storeKafkaKeys) {
        if (!validFormats.contains(keysFormatClass)) {
          recordErrors(
              String.format(FORMAT_CONFIG_ERROR_MESSAGE,
                  compressionType.name, "keys", keysFormatClass.getName()),
              STORE_KAFKA_KEYS_CONFIG, KEYS_FORMAT_CLASS_CONFIG, COMPRESSION_TYPE_CONFIG);
        }
      }

      if (storeKafkaHeaders) {
        if (!validFormats.contains(headersFormatClass)) {
          recordErrors(
              String.format(FORMAT_CONFIG_ERROR_MESSAGE,
                  compressionType.name, "headers", headersFormatClass.getName()),
              STORE_KAFKA_HEADERS_CONFIG, HEADERS_FORMAT_CLASS_CONFIG, COMPRESSION_TYPE_CONFIG);
        }
      }
    }
  }
  private void validateWanModeAndPathStyleCompatibility(S3SinkConnectorConfig s3SinkConnectorConfig) {
    boolean s3WanModeEnabled = s3SinkConnectorConfig.getBoolean(WAN_MODE_CONFIG);
    boolean pathStyleAccessEnabled = s3SinkConnectorConfig.getBoolean(S3_PATH_STYLE_ACCESS_ENABLED_CONFIG);
    if (s3WanModeEnabled && pathStyleAccessEnabled) {
      recordErrors(ACCESS_MODE_ERROR_MESSAGE, WAN_MODE_CONFIG, S3_PATH_STYLE_ACCESS_ENABLED_CONFIG);
    }
  }

  private void recordErrors(String message, String... keys) {
    log.error("Validation Failed with error: " + message);
    for (String key: keys) {
      recordError(message, key);
    }
  }

  private void recordError(String message, String key) {
    Objects.requireNonNull(key);
    if (!key.equals("")) {
      ConfigValue value = valuesByKey.get(key);
      if (!message.equals("")) {
        value.addErrorMessage(message);
      }
    }
  }
}
