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

public class KafkaFileEventProvider extends FileEventProvider {
    private final KafkaFileEventConfig kafkaConfig;

    public KafkaFileEventProvider(String configJson, boolean skipError) {
        super(configJson, skipError);
        this.kafkaConfig =
                KafkaFileEventConfig.fromJsonString(configJson, KafkaFileEventConfig.class);
    }

    @Override
    public void callImpl(
            String clusterName,
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
                        clusterName,
                        topicName,
                        s3Partition,
                        filePath,
                        partition,
                        formatDateRFC3339(baseRecordTimestamp),
                        formatDateRFC3339(currentTimestamp),
                        recordCount,
                        formatDateRFC3339(eventDatetime));
        try (final Producer<String, SpecificRecord> producer =
                     new KafkaProducer<>(kafkaConfig.toProps())) {
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
