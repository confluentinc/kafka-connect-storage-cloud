/*
 * Add here
 */

package io.confluent.connect.s3.hooks;

import com.amazonaws.util.Md5Utils;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.connect.errors.RetriableException;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BlockingKafkaPostCommitHook implements PostCommitHook {

  private static final Logger log = LoggerFactory.getLogger(BlockingKafkaPostCommitHook.class);
  private static final DateTimeFormatter timeFormatter =
          DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:00");
  private Pattern pattern;

  private long partitionDurationMinutes;
  private String kafkaTopic;
  private KafkaProducer<String, String> kafkaProducer;

  @Override
  public void init(S3SinkConnectorConfig config) {
    @SuppressWarnings("unchecked")
    Class<? extends Partitioner<?>> partitionerClass =
            (Class<? extends Partitioner<?>>) config.getClass(
                    PartitionerConfig.PARTITIONER_CLASS_CONFIG);

    if (partitionerClass == null || !partitionerClass.getName().equals(
            "io.logz.kafka.connect.FieldAndTimeBasedPartitioner")) {
      throw new IllegalArgumentException("This post commit hook can be used ony with"
              + " io.logz.kafka.connect.FieldAndTimeBasedPartitioner partitioner");
    }

    String topicsDir = config.getString(StorageCommonConfig.TOPICS_DIR_CONFIG);
    pattern = Pattern.compile(topicsDir + "/(\\d+)/");

    long rotateScheduleMS = config.getLong(
            S3SinkConnectorConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG);
    partitionDurationMinutes = TimeUnit.MILLISECONDS.toMinutes(rotateScheduleMS);
    if (partitionDurationMinutes < 1) {
      throw new IllegalArgumentException("rotate.schedule.interval.ms must be bigger than 1"
              + " minute to use this hook");
    }
    kafkaTopic = config.getPostCommitKafkaTopic();
    kafkaProducer = newKafkaPostCommitProducer(config);
    log.info("BlockingKafkaPostCommitHook initialized successfully");
  }

  @Override
  public void put(Set<String> s3ObjectPaths, Long baseRecordTimestamp) {
    try {
      kafkaProducer.beginTransaction();
      log.info("Transaction began");

      for (String s3ObjectPath : s3ObjectPaths) {
        List<Header> headers = new ArrayList<>();
        headers.add(new RecordHeader("accountId", getAccountId(s3ObjectPath).getBytes()));
        headers.add(new RecordHeader("fileTimestamp",
                roundTimeToPartitonTime(baseRecordTimestamp).getBytes()));
        headers.add(new RecordHeader("pathHash",
                getPathHash(s3ObjectPath).getBytes()));
        kafkaProducer.send(new ProducerRecord<>(kafkaTopic,
                null, null, null, s3ObjectPath, headers));
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

  private String roundTimeToPartitonTime(Long baseRecordTimestamp) {
    LocalDateTime localDateTime = LocalDateTime.ofInstant(
            Instant.ofEpochMilli(baseRecordTimestamp), ZoneOffset.UTC);
    LocalDateTime roundedDateTime = localDateTime.truncatedTo(ChronoUnit.HOURS).plusMinutes(
            (localDateTime.getMinute() / partitionDurationMinutes) * partitionDurationMinutes);
    return roundedDateTime.format(timeFormatter);
  }

  private String getPathHash(String s3ObjectPath) {
    return Md5Utils.md5AsBase64(s3ObjectPath.getBytes()).substring(0, 16)
            // Escape the base64 + and / to safe URL characters
            .replace("+", "A")
            .replace("/", "B");
  }

  private String getAccountId(String s3ObjectPath) {
    Matcher matcher = pattern.matcher(s3ObjectPath);
    if (matcher.find()) {
      return matcher.group(1);
    } else {
      throw new ConnectException("Couldn't create header for accountId");
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
