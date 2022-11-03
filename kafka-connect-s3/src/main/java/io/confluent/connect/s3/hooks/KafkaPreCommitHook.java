package io.confluent.connect.s3.hooks;

import io.confluent.connect.s3.S3SinkConnectorConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class KafkaPreCommitHook implements PreCommitHook {

    private static final Logger log = LoggerFactory.getLogger(KafkaPreCommitHook.class);
    private String kafkaTopicForPrecommit;
    private KafkaProducer<String, String> kafkaProducer;

    public KafkaPreCommitHook() {

    }

    public void init(S3SinkConnectorConfig config) {
        kafkaTopicForPrecommit = config.getPrecommitKafkaTopic();
        kafkaProducer = newKafkaPrecommitProducer(config);
    }

    public Map<TopicPartition, Set<String>> execute(Map<TopicPartition, Set<String>> filesToCommit) {
        Map<TopicPartition, Set<String>> sentFiles = new ConcurrentHashMap<>();

        int totalNumOfFiles = filesToCommit.values().stream().mapToInt(Set::size).sum();

        CountDownLatch latch = new CountDownLatch(totalNumOfFiles);
        filesToCommit.forEach((tp, tpFileSet) -> {
            if (!tpFileSet.isEmpty()) {
                sentFiles.put(tp, ConcurrentHashMap.newKeySet());
            }
            tpFileSet.forEach(file -> {
                try {
                    kafkaProducer.send(new ProducerRecord<>(kafkaTopicForPrecommit, file), (metadata, e) -> {
                        if (e != null) {
                            log.warn("Failed to send file: {}. Error: {}", file, e);
                        }
                        log.debug("file {} produced successfully, with metadata: {}", file, metadata);
                        sentFiles.get(tp).add(file);
                        latch.countDown();
                    });
                } catch (Exception e) {
                    // handle exception
                    latch.countDown();
                }
            });
        });

        try {
            if(!latch.await(15, TimeUnit.SECONDS)) {
                log.warn("latch elapsed before we sent all files. Already sent: {}", sentFiles);
            }
        } catch (InterruptedException e) {
            log.error("got interrupted while waiting for latch", e);
        }

        return sentFiles;
    }

    public void close() {
        kafkaProducer.close();
    }

    private KafkaProducer<String, String> newKafkaPrecommitProducer(S3SinkConnectorConfig config) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getPrecommitKafkaBootstrapServers());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "1");
        props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "internal-archiver-producer");

        return new KafkaProducer<>(props);
    }

}
