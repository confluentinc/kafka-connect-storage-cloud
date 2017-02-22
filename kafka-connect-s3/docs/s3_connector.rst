S3 Connector
==============

The S3 connector, currently available as sink, allows you to export data from Kafka topics to S3 objects in a
variety of formats. In addition, S3 connector exports data by guaranteeing exactly-once delivery
semantics to the consumers of the S3 objects it produces.

Being a Sink connector, the S3 connector periodically polls data from Kafka and in turn uploads them
to S3. A partitioner is used to split the data of every Kafka partition into chunks. Each chunk of data is
represented as an S3 object whose key name encodes the topic, the kafka partition and the start offset of
this data chunk. If no partitioner is specified in the configuration, the default partitioner which
preserves Kafka partitioning is used. The size of each data chunk is determined by the number of
records written to S3 and by schema compatibility.

Features
--------
The S3 connector offers a variety of features:

* **Extensible Data Format with or without Schema**: Out of the box, the connector supports writing data to S3 in Avro
  and Json format. Besides records with schema, the connector supports exporting plain Json records without schema in
  text files, one record per-line. In general, the connector may accept any format that provides an implementation of
  the ``Format`` interface.

* **Schema Evolution**: When schemas are used, the connector supports schema evolution based on schema compatibility
  modes. The available modes currently are: ``NONE``, ``BACKWARD``, ``FORWARD`` and ``FULL`` and a selection can be made
  by setting the property ``schema.compatibility``. When the connector observes a schema change, it decides whether to
  roll a file or project to the proper schema according to the ``schema.compatibility`` configuration in use.

* **Pluggable Partitioner**: The connector comes out of the box with partitioners that support default partitioning,
  field partitioning, and time-based partitioning in days or hours. You may implement your own partitioners by
  extending the ``Partitioner`` class. Additionally, you can customize time based partitioning by extending the
  ``TimeBasedPartitioner`` class.


Exactly-once delivery on top of an eventually consistent store
--------------------------------------------------------------
Given the same set of Kafka records, the S3 connector is able to provide exactly-once semantics to the consumers of the
objects it exports under the condition that it is supplied with a deterministic partitioner.

TODO: complete description of EOS implementation.

In the current version, time-based partitioners still depend on wall-clock time to partition data as the time-based
partitioners available with HDFS connector do.


Quickstart
----------
In this Quickstart, we use the S3 connector to export data produced by the Avro console producer
to S3.

To begin, make sure that Zookeeper, Kafka and SchemaRegistry services are running. The instructions on how to start
these services are available at the Confluent Platform QuickStart. You also need to have created a destination bucket
in S3 with appropriate permissions in advance.


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
