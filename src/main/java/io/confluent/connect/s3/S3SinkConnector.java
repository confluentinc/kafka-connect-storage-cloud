/*
 * Copyright 2016 Confluent Inc.
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

import io.confluent.connect.s3.util.Version;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.Task;

import java.util.List;
import java.util.Map;

public class S3SinkConnector extends Connector {

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {}

  @Override
  public Class<? extends Task> taskClass() {
    return S3SinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    return null;
  }

  @Override
  public void stop() {}

  @Override
  public ConfigDef config() {
    return null;
  }

}
