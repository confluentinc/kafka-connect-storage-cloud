/*
 * Add here
 */

package io.confluent.connect.s3.hooks;

import io.confluent.connect.s3.S3SinkConnectorConfig;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.connect.errors.RetriableException;

import java.util.Properties;
import java.util.Set;

public class BlockingKafkaPostCommitHook implements PostCommitHook {

  private static final Logger log = LoggerFactory.getLogger(BlockingKafkaPostCommitHook.class);
  private String kafkaTopic;
  private KafkaProducer<String, String> kafkaProducer;

  @Override
  public void init(S3SinkConnectorConfig config) {
    kafkaTopic = config.getPostCommitKafkaTopic();
    kafkaProducer = newKafkaPostCommitProducer(config);
    log.info("BlockingKafkaPostCommitHook initialized successfully");
  }

  @Override
  public void put(Set<String> s3ObjectPaths) {
    try {
      kafkaProducer.beginTransaction();
      log.info("Transaction began");

      for (String s3ObjectPath : s3ObjectPaths) {
        kafkaProducer.send(new ProducerRecord<>(kafkaTopic, s3ObjectPath));
      }

      kafkaProducer.commitTransaction();
      log.info("Transaction committed");
    } catch (ProducerFencedException | AuthorizationException | UnsupportedVersionException
             | IllegalStateException | OutOfOrderSequenceException e) {
      log.error("Failed to begin transaction with unrecoverable exception, closing producer", e);
      throw new ConnectException(e);
    } catch (KafkaException e) {
      log.error("Failed to produce to kafka, aborting transaction and will try again later", e);
      kafkaProducer.abortTransaction();
      throw new RetriableException(e);
    }
  }

  @Override
  public void close() {
    try {
      kafkaProducer.close();
    } catch (Exception e) {
      log.error("Failed to close kafka producer", e);
    }
  }

  private KafkaProducer<String, String> newKafkaPostCommitProducer(S3SinkConnectorConfig config) {
    Properties props = new Properties();
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            config.getPostCommitKafkaBootstrapBrokers());
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class.getName());
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    String id = "blocking-kafka-producer-" + RandomStringUtils.randomAlphabetic(6);
    props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, id);
    props.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, id);
    props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "10");

    KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
    try {
      log.info("stating to initialize transactions");
      kafkaProducer.initTransactions();
      log.info("Transactions initialized");
    } catch (Exception e) {
      log.error("Failed to initiate transaction context", e);
      throw new ConnectException(e);
    }
    return kafkaProducer;
  }

}
