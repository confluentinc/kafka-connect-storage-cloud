FROM confluentinc/cp-kafka-connect-base:5.5.1 as base
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.0.0
COPY target/kafka-connect-s3-10.1.0-SNAPSHOT-development/share/java/kafka-connect-s3/kafka-connect-s3-10.1.0-SNAPSHOT.jar /usr/share/confluent-hub-components/confluentinc-kafka-connect-s3/lib/kafka-connect-s3-10.0.0.jar
COPY connect-avro-distributed.properties /etc/schema-registry/
COPY lr-model-pipeline.properties /etc/schema-registry/
COPY first.properties /etc/schema-registry/
COPY crawler.properties /etc/schema-registry/
COPY pub_i18n.properties /etc/schema-registry/
COPY pub_euro_i18n.properties /etc/schema-registry/
COPY pub_asia_i18n.properties /etc/schema-registry/
COPY push2.properties /etc/schema-registry/


