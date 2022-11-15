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


package io.confluent.connect.s3.metrics;

import io.confluent.connect.s3.TopicPartitionWriter;
import java.util.Map;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import org.apache.kafka.common.TopicPartition;

public interface S3SinkTaskMetricsMBean {

  void start(Map<TopicPartition, TopicPartitionWriter> topicPartitionWriters,
      String connectorName,
      Map<String, String> connectorConfig);

  void stop() throws MBeanRegistrationException, InstanceNotFoundException;

  public int getNumberOfAssignedTopicPartitions();

  public int getNumberOfOutputPartitions();

  public int getNumberOfFileRotations();

  public int getNumberOfPartFiles();

}
