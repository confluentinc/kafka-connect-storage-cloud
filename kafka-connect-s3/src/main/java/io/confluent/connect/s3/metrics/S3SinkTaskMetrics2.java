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
import io.confluent.connect.s3.util.OutputPartitionInfo;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3SinkTaskMetrics2 implements S3SinkTaskMetricsMXBean {

  private final String taskId;
  private final String connectorName;
  private final Map<TopicPartition, TopicPartitionWriter> topicPartitionWriters;

  private static final Logger log = LoggerFactory.getLogger(S3SinkTaskMetrics2.class);

  public S3SinkTaskMetrics2(Map<TopicPartition, TopicPartitionWriter> topicPartitionWriters,
      String connectorName, Map<String, String> connectorConfig) {
    this.topicPartitionWriters = topicPartitionWriters;
    this.connectorName = connectorName;
    this.taskId = connectorConfig.get("task.id");

    try {
      ObjectName objectName = new ObjectName(
          String.format("io.confluent.connect.s3.S3Sink:name=%s,task=%s", connectorName, taskId));
      MBeanServer server = ManagementFactory.getPlatformMBeanServer();
      server.registerMBean(this, objectName);
    } catch (MalformedObjectNameException | InstanceAlreadyExistsException
        | MBeanRegistrationException | NotCompliantMBeanException exception) {
      log.error("error registering JMX Bean", exception);
    }
    log.info("successfully registered JMX Bean");
  }

  @Override
  public void stop() throws MBeanRegistrationException, InstanceNotFoundException {
    try {
      ObjectName objectName = new ObjectName(
          String.format("io.confluent.connect.s3.S3Sink:name=%s,task=%s", connectorName, taskId));
      MBeanServer server = ManagementFactory.getPlatformMBeanServer();
      server.unregisterMBean(objectName);
    } catch (MalformedObjectNameException | MBeanRegistrationException exception) {
      log.error("error un-registering JMX Bean", exception);
    }
  }

  @Override
  public SortedMap<String, OutputPartitionInfo> getOutputPartitionInfo() {
    SortedMap<String, OutputPartitionInfo> outputPartitionInfoMap = new TreeMap<>();
    for (Map.Entry<TopicPartition, TopicPartitionWriter> entry: topicPartitionWriters.entrySet()) {
      final TopicPartitionWriter topicPartitionWriter = entry.getValue();
      outputPartitionInfoMap.putAll(topicPartitionWriter.getOutputPartitionInfo());
    }
    return outputPartitionInfoMap;
  }

  @Override
  public Map<String, Integer> getTotalRotationCount() {
    SortedMap<String, Integer> rotationCountMap = new TreeMap<>();
    for (Map.Entry<TopicPartition, TopicPartitionWriter> entry: topicPartitionWriters.entrySet()) {
      final TopicPartition topicPartition = entry.getKey();
      final TopicPartitionWriter topicPartitionWriter = entry.getValue();
      rotationCountMap.put(topicPartition.toString(), topicPartitionWriter.getTotalRotationCount());
    }
    return rotationCountMap;
  }

}
