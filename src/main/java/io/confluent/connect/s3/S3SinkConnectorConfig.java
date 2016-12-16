/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/

package io.confluent.connect.s3;

import io.confluent.connect.storage.StorageSinkConnectorConfig;

import java.util.Map;

public class S3SinkConnectorConfig extends StorageSinkConnectorConfig {

  public S3SinkConnectorConfig(Map<String, String> props) {
    super(props);
  }
}
