S3 Connector
==============

The S3 connector allows you to export data from Kafka topics to S3 objects in a variety of formats
and integrates with Hive to make data immediately available for querying with HiveQL.

The connector periodically polls data from Kafka and uploads them to S3. The data from each Kafka
topic is partitioned by the provided partitioner and divided into chunks. Each chunk of data is
represented as an S3 object with topic, kafka partition and start offset of this data chunk encoded
in the object key. If no partitioner is specified in the configuration, the default partitioner which
preserves the Kafka partitioning is used. The size of each data chunk is determined by the number of
records written to S3, the time written to HDFS and schema compatibility.

The S3 connector integrates with Hive and when it is enabled, the connector automatically creates
an external Hive partitioned table for each Kafka topic and updates the table according to the
available data in S3.

Quickstart
----------
In this Quickstart, we use the S3 connector to export data produced by the Avro console producer
to S3.

Start Zookeeper, Kafka and SchemaRegistry if you haven't done so. The instructions on how to start
these services are available at the Confluent Platform QuickStart. You also need to have created 
a destination bucket in S3 beforehand. For Hive integration, you need to have Hive installed and 
to know the metastore thrift uri.

Features
--------
The S3 connector offers a variety of features:

Configuration
-------------
This section first gives example configurations that cover common scenarios, then provides an exhaustive
description of the available configuration options.

Example
~~~~~~~
Here is the content of ``etc/kafka-connect-s3/quickstart-s3.properties``::

Format and Partitioner
~~~~~~~~~~~~~~~~~~~~~~

Hive Integration
~~~~~~~~~~~~~~~~

Schema Evolution
----------------
