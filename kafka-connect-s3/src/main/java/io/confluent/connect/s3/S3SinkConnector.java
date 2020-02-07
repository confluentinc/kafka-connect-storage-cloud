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

import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.StorageFactory;
import io.confluent.connect.storage.common.StorageCommonConfig;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.connect.s3.util.Version;

/**
 * Connector class for Amazon Simple Storage Service (S3).
 */
public class S3SinkConnector extends SinkConnector {
  private static final Logger log = LoggerFactory.getLogger(S3SinkConnector.class);
  private Map<String, String> configProps;
  private S3SinkConnectorConfig config;

  /**
   * No-arg constructor. It is instantiated by Connect framework.
   */
  public S3SinkConnector() {
    // no-arg constructor required by Connect framework.
  }

  // Visible for testing.
  S3SinkConnector(S3SinkConnectorConfig config) {
    this.config = config;
  }

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    configProps = new HashMap<>(props);
    config = new S3SinkConnectorConfig(props);
    log.info("Starting S3 connector {}", config.getName());
  }

  @Override
  public Class<? extends Task> taskClass() {
    return S3SinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    Map<String, String> taskProps = new HashMap<>(configProps);
    List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);
    for (int i = 0; i < maxTasks; ++i) {
      taskConfigs.add(taskProps);
    }
    return taskConfigs;
  }

  @Override
  public void stop() {
    log.info("Shutting down S3 connector {}", config.getName());
  }

  @Override
  public ConfigDef config() {
    return S3SinkConnectorConfig.getConfig();
  }

  private boolean checkBucketExists(S3SinkConnectorConfig config) {
    @SuppressWarnings("unchecked")
    Class<? extends S3Storage> storageClass =
            (Class<? extends S3Storage>)
                    config.getClass(StorageCommonConfig.STORAGE_CLASS_CONFIG);

    final S3Storage bucketCheckHelper = StorageFactory.createStorage(storageClass,
            S3SinkConnectorConfig.class, config,
            config.getString(StorageCommonConfig.STORE_URL_CONFIG));

    return bucketCheckHelper.bucketExists();
  }

  @Override
  public Config validate(final Map<String, String> connectorConfigs) {
    ConfigDef configDef = this.config();
    if (null == configDef) {
      throw new ConnectException(String.format("%s.config() must return a ConfigDef that is not"
              + " null.", this.getClass().getName()));
    } else {

      // Checking whether the bucket exists
      if (!checkBucketExists(new S3SinkConnectorConfig(connectorConfigs))) {
        throw new ConnectException("Bucket does not exist or invalid credentials "
                + "provided");
      }

      List<ConfigValue> configValues = configDef.validate(connectorConfigs);
      return new Config(configValues);
    }
  }

}
