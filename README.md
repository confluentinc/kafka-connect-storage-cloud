# Kafka Connect Connector for S3
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bhttps%3A%2F%2Fgithub.com%2Fconfluentinc%2Fkafka-connect-storage-cloud.svg?type=shield)](https://app.fossa.io/projects/git%2Bhttps%3A%2F%2Fgithub.com%2Fconfluentinc%2Fkafka-connect-storage-cloud?ref=badge_shield)


*kafka-connect-storage-cloud* is the repository for Confluent's [Kafka Connectors](http://kafka.apache.org/documentation.html#connect)
designed to be used to copy data from Kafka into Amazon S3. 

## Kafka Connect Sink Connector for Amazon Simple Storage Service (S3)

Documentation for this connector can be found [here](http://docs.confluent.io/current/connect/connect-storage-cloud/kafka-connect-s3/docs/index.html).

Blogpost for this connector can be found [here](https://www.confluent.io/blog/apache-kafka-to-amazon-s3-exactly-once).

# Development

To build a development version you'll need a recent version of Kafka 
as well as a set of upstream Confluent projects, which you'll have to build from their appropriate snapshot branch.
See [the kafka-connect-storage-common FAQ](https://github.com/confluentinc/kafka-connect-storage-common/wiki/FAQ)
for guidance on this process.

You can build *kafka-connect-storage-cloud* with Maven using the standard lifecycle phases.


# Contribute

- Source Code: https://github.com/confluentinc/kafka-connect-storage-cloud
- Issue Tracker: https://github.com/confluentinc/kafka-connect-storage-cloud/issues


# License

This project is licensed under the [Confluent Community License](LICENSE).


[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bhttps%3A%2F%2Fgithub.com%2Fconfluentinc%2Fkafka-connect-storage-cloud.svg?type=large)](https://app.fossa.io/projects/git%2Bhttps%3A%2F%2Fgithub.com%2Fconfluentinc%2Fkafka-connect-storage-cloud?ref=badge_large)

# Sovrn-specific build

This connector does have a Maven package target that attempts to add the necessary support jars to run the connector 
properly inside Kafka Connect. It has been our observation that this group of jars is insufficient. Once the connector
has been packaged by Maven, the following jars need to be added to the group (which is located in 
`kafka-connect-s3/target/kafka-connect-s3-<version>-SNAPSHOT-package/share/java/kafka-connect-s3`).

avro-1.9.2.jar
kafka-avro-serializer-6.0.1.jar
kafka-connect-avro-data-6.0.1.jar
kafka-connect-storage-common-10.0.5.jar
kafka-connect-storage-core-10.0.5.jar
kafka-connect-storage-format-10.0.5.jar
kafka-connect-storage-partitioner-10.0.5.jar
kafka-schema-registry-client-6.0.1.jar
kafka-schema-serializer-6.0.1.jar
parquet-avro-1.11.1.jar
parquet-column-1.11.1.jar
parquet-common-1.11.1.jar
parquet-encoding-1.11.1.jar
parquet-format-structures-1.11.1.jar
parquet-hadoop-1.11.1.jar

Once the extra jars (which you can get from the S3 sink connector at 
[Confluent Hub](https://www.confluent.io/hub/confluentinc/kafka-connect-s3)) have been added to the directory,
go up one directory (`java`), and create the tarball using `tar cvfz patched-kafka-connect-s3.tgz kafka-connect-s3`.

This tarball is then uploaded to Artifactory using 
`jfrog rt u patched-kafka-connect-s3.tgz raw-sovrn/exchange/patched-kafka-connect-s3.tgz`. Once this upload is complete,
the tarball is ready to be used by `ansible-kafkaconnect`.
