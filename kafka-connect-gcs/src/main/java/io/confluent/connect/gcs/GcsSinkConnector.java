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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.connect.gcs.util.Version;

/**
 * Connector class for Google Cloud Storage (GCS).
 */
public class GcsSinkConnector extends SinkConnector {
  private static final Logger log = LoggerFactory.getLogger(GcsSinkConnector.class);
  private Map<String, String> configProps;
  private GcsSinkConnectorConfig config;

  /**
   * No-arg constructor. It is instantiated by Connect framework.
   */
  public GcsSinkConnector() {
    // no-arg constructor required by Connect framework.
  }

  // Visible for testing.
  GcsSinkConnector(GcsSinkConnectorConfig config) {
    this.config = config;
  }

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    configProps = new HashMap<>(props);
    config = new GcsSinkConnectorConfig(props);
    log.info("Starting GCS connector {}", config.getName());
  }

  @Override
  public Class<? extends Task> taskClass() {
    return GcsSinkTask.class;
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
    log.info("Shutting down GCS connector {}", config.getName());
  }

  @Override
  public ConfigDef config() {
    return GcsSinkConnectorConfig.getConfig();
  }

}
