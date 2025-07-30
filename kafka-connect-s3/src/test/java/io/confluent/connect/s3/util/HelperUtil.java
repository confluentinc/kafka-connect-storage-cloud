package io.confluent.connect.s3.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;

import java.util.HashMap;
import java.util.Map;

public class HelperUtil {
  public static JsonConverter initializeJsonConverter() {
    Map<String, Object> jsonConverterProps = new HashMap<>();
    jsonConverterProps.put("schemas.enable", "true");
    jsonConverterProps.put("converter.type", "value");
    JsonConverter jsonConverter = new JsonConverter();
    jsonConverter.configure(jsonConverterProps);
    return jsonConverter;
  }

  public static Producer<byte[], byte[]> initializeCustomProducer(EmbeddedConnectCluster connect) {
    Map<String, Object> producerProps = new HashMap<>();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers());
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        org.apache.kafka.common.serialization.ByteArraySerializer.class.getName());
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        org.apache.kafka.common.serialization.ByteArraySerializer.class.getName());
    return new KafkaProducer<>(producerProps);
  }
}
