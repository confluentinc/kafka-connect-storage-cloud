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

package io.confluent.connect.s3.callback;

import io.confluent.connect.s3.KafkaFileCallbackConfig;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;

public class KafkaFileCallbackProvider implements FileCallbackProvider {
  private final String configJson;
  private final KafkaFileCallbackConfig kafkaConfig;

  public KafkaFileCallbackProvider(String configJson) {
    this.configJson = configJson;
    this.kafkaConfig = KafkaFileCallbackConfig.fromJsonString(configJson,
            KafkaFileCallbackConfig.class);
  }

  @Override
  public void call(String topicName,String s3Partition, String filePath, int partition,
                   Long baseRecordTimestamp, Long currentTimestamp, int recordCount) {
    String value = String.join("|", Arrays.asList(topicName, s3Partition, filePath,
            String.valueOf(partition), String.valueOf(baseRecordTimestamp),
            String.valueOf(currentTimestamp), String.valueOf(recordCount)));
    Callback callback = new Callback(topicName, s3Partition, filePath, partition,
            baseRecordTimestamp, currentTimestamp, recordCount);
    try (final Producer<String, SpecificRecord> producer =
                 new KafkaProducer<>(kafkaConfig.toProps())) {
      producer.send(new ProducerRecord<>(kafkaConfig.getTopicName(), topicName, callback),
              (event, ex) -> {
                if (ex != null) {
                  ex.printStackTrace();
                }
              });
    } catch (Exception e) {
      System.out.println("foo");
    }
  }

}
