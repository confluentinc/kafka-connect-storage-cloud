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

package io.confluent.connect.s3.file;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.joda.time.DateTime;

import java.util.Properties;

public class KafkaFileEventProvider extends FileEventProvider {
  private final KafkaFileEventConfig kafkaConfig;
  private final KafkaFileEventSecurity kafkaSecurity;

  public KafkaFileEventProvider(String configJson, String securityJson, boolean skipError) {
    super(configJson, securityJson, skipError);
    this.kafkaConfig =
        AbstractFileEventConfig.fromJsonString(configJson, KafkaFileEventConfig.class);
    this.kafkaSecurity = AbstractFileEventConfig.fromJsonString(securityJson, KafkaFileEventSecurity.class);
  }

  @Override
  public void callImpl(
      String topicName,
      String s3Partition,
      String filePath,
      int partition,
      DateTime baseRecordTimestamp,
      DateTime currentTimestamp,
      int recordCount,
      DateTime eventDatetime) {
    String key = topicName;
    FileEvent value =
        new FileEvent(
            topicName,
            s3Partition,
            filePath,
            partition,
            formatDateRFC3339(baseRecordTimestamp),
            formatDateRFC3339(currentTimestamp),
            recordCount,
            formatDateRFC3339(eventDatetime));
    Properties combinedProperties = new Properties();
    combinedProperties.putAll(kafkaConfig.toProps());
    combinedProperties.putAll(kafkaSecurity.toProps());
    try (final Producer<String, SpecificRecord> producer =
        new KafkaProducer<>(combinedProperties)) {
      producer.send(
          new ProducerRecord<>(kafkaConfig.getTopicName(), key, value),
          (event, ex) -> {
            if (ex != null) {
              throw new RuntimeException(ex);
            }
          });
    }
  }
}
