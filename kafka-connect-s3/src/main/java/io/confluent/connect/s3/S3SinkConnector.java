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

package io.confluent.connect.s3;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.internal.BucketNameUtils;
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

  public boolean checkBucketExists(String bucketName) {
    try {
      final AmazonS3 bucketCheckHelper = AmazonS3ClientBuilder.defaultClient();
      return bucketCheckHelper.doesBucketExist(bucketName);
    } catch (SdkClientException e) {
      return false;
    }
  }

  @Override
  public Config validate(final Map<String, String> connectorConfigs) {
    ConfigDef configDef = this.config();
    if (null == configDef) {
      throw new ConnectException(String.format("%s.config() must return a ConfigDef that is not"
              + " null.", this.getClass().getName()));
    } else {

      // Checking whether bucket name has a valid format.
      String bucketName = connectorConfigs.get(S3SinkConnectorConfig.S3_BUCKET_CONFIG);
      try {
        BucketNameUtils.validateBucketName(bucketName);
      } catch (IllegalArgumentException e) {
        throw new ConnectException(bucketName
                + " is an invalid bucket name. Does not follow AWS guidelines.");
      }

      // Check whether the bucket exists.
      if (!checkBucketExists(bucketName)) {
        throw new ConnectException("Bucket does not exist.");
      }
    }

    List<ConfigValue> configValues = configDef.validate(connectorConfigs);
    return new Config(configValues);
  }
}
