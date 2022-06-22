FROM confluentinc/cp-kafka-connect-base:7.1.1 as base

COPY jmx/jmx_prometheus_httpserver-0.17.0.jar /
COPY jmx/config.yaml /
ENV KAFKA_JMX_OPTS="-javaagent:/jmx_prometheus_httpserver-0.17.0.jar=127.0.0.1:10902:/config.yaml"
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.0.0
COPY kafka-connect-s3/target/kafka-connect-s3-10.1.0-SNAPSHOT-development/share/java/kafka-connect-s3/kafka-connect-s3-10.1.0-SNAPSHOT.jar /usr/share/confluent-hub-components/confluentinc-kafka-connect-s3/lib/kafka-connect-s3-10.0.0.jar
COPY connect-avro-distributed.properties /etc/schema-registry/
COPY lr-model-pipeline.properties /etc/schema-registry/
COPY first.properties /etc/schema-registry/
COPY crawler.properties /etc/schema-registry/
COPY pub_i18n.properties /etc/schema-registry/
COPY pub_euro_i18n.properties /etc/schema-registry/
COPY pub_asia_i18n.properties /etc/schema-registry/
COPY push2.properties /etc/schema-registry/
