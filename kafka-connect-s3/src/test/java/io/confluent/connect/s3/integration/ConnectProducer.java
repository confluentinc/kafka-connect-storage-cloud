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

package io.confluent.connect.s3.integration;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;

/**
 * A class that enables sending SinkRecords to the EmbeddedConnectCluster via a custom Producer.
 */
public class ConnectProducer implements AutoCloseable {

  private JsonConverter keyConverter;
  private JsonConverter valueConverter;
  private Producer<byte[], byte[]> producer;

  public ConnectProducer(EmbeddedConnectCluster connect) {
    Map<String, Object> jsonConverterProps = Collections.singletonMap("schemas.enable", "true");

    keyConverter = new JsonConverter();
    keyConverter.configure(jsonConverterProps, true);

    valueConverter = new JsonConverter();
    valueConverter.configure(jsonConverterProps, false);

    Map<String, Object> producerProps = new HashMap<>();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers());
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        org.apache.kafka.common.serialization.ByteArraySerializer.class.getName());
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        org.apache.kafka.common.serialization.ByteArraySerializer.class.getName());

    producer = new KafkaProducer<>(producerProps);
  }

  public void produce(SourceRecord record) throws ExecutionException, InterruptedException {
    // Serialize the key to a byte[] so we can send it directly to the producer
    byte[] key = keyConverter.fromConnectData(record.topic(), record.keySchema(), record.key());
    // Serialize the value to a byte[] so we can send it directly to the producer
    byte[] value = valueConverter
        .fromConnectData(record.topic(), record.valueSchema(), record.value());

    producer.send(new ProducerRecord<>(record.topic(), key, value)).get();
  }

  public void produce(String topic, Schema keySchema, Object key, Schema valueSchema,
      Object value) throws ExecutionException, InterruptedException {

    SourceRecord record = new SourceRecord(Collections.emptyMap(), Collections.emptyMap(), topic,
        keySchema, key, valueSchema, value);
    produce(record);
  }

  @Override
  public void close() {
    if (producer != null) {
      producer.close();
    }
  }

}
