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

package io.confluent.connect.azure;

import io.confluent.connect.azure.util.Version;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Connector class for Amazon Simple Storage Service (S3).
 */
public class AzBlobSinkConnector extends SinkConnector {
  private static final Logger log = LoggerFactory.getLogger(AzBlobSinkConnector.class);
  private Map<String, String> configProps;
  private AzBlobSinkConnectorConfig config;

  /**
   * No-arg constructor. It is instantiated by Connect framework.
   */
  public AzBlobSinkConnector() {
    // no-arg constructor required by Connect framework.
  }

  // Visible for testing.
  AzBlobSinkConnector(AzBlobSinkConnectorConfig config) {
    this.config = config;
  }

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    configProps = new HashMap<>(props);
    config = new AzBlobSinkConnectorConfig(props);
    log.info("Starting Azure blob connector {}", config.getName());
  }

  @Override
  public Class<? extends Task> taskClass() {
    return AzBlobSinkTask.class;
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
    return AzBlobSinkConnectorConfig.getConfig();
  }

}
