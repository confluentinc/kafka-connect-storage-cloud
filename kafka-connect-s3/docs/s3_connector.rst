S3 Connector
==============

The S3 connector, currently available as sink, allows you to export data from Kafka topics to S3 objects in a
variety of formats. In addition, given deterministic partitioners, the S3 connector exports data by guaranteeing
exactly-once delivery semantics to the consumers of the S3 objects it produces.

Being a sink, the S3 connector periodically polls data from Kafka and in turn uploads them
to S3. A partitioner is used to split the data of every Kafka partition into chunks. Each chunk of data is
represented as an S3 object, whose key name encodes the topic, the kafka partition and the start offset of
this data chunk. If no partitioner is specified in the configuration, the default partitioner which
preserves Kafka partitioning is used. The size of each data chunk is determined by the number of
records written to S3 and by schema compatibility.

Features
--------
The S3 connector offers a variety of features:

* **Exactly Once Delivery**: Records that are exported using a deterministic partitioner are delivered with exactly-once
  semantics as far as the user of the S3 bucket is concerned.

* **Pluggable Data Format with or without Schema**: Out of the box, the connector supports writing data to S3 in Avro
  and Json format. Besides records with schema, the connector supports exporting plain Json records without schema in
  text files, one record per-line. In general, the connector may accept any format that provides an implementation of
  the ``Format`` interface.

* **Schema Evolution**: When schemas are used, the connector supports schema evolution based on schema compatibility
  modes. The available modes are: ``NONE``, ``BACKWARD``, ``FORWARD`` and ``FULL`` and a selection can be made
  by setting the property ``schema.compatibility`` in the connector's configuration. When the connector observes a
  schema change, it decides whether to roll the file or project the record to the proper schema according to
  the ``schema.compatibility`` configuration in use.

* **Pluggable Partitioner**: The connector comes out of the box with partitioners that support default partitioning,
  field partitioning, and time-based partitioning in days or hours. You may implement your own partitioners by
  extending the ``Partitioner`` class. Additionally, you can customize time based partitioning by extending the
  ``TimeBasedPartitioner`` class.


Exactly-once delivery on top of eventual consistency
----------------------------------------------------
Given the same set of Kafka records, the S3 connector is able to provide exactly-once semantics to consumers of the
objects it exports to S3 under the condition that the connector is supplied with a deterministic partitioner.

Currently, out of the available partitioners, deterministic are the default and field partitioners. These partitioners
take into account ``flush.size`` and ``schema.compatibility`` to decide when to roll and commit a new file to S3. When
any of these partitioners is used, splitting of files always happens at the same offsets for a given set of Kafka records.
The connector, by using multipart uploads in S3 and encoding the start offset in the files it commits, always delivers
files in S3 that contain the same records, even under the presence of failures. If a connector task fails before a multipart upload
completes, the file does not become visible to S3. If, on the other hand, a failure occurs after the upload has completed
but before the corresponding offset is committed to Kafka by the connector, then a re-upload will take place. However,
such a re-upload is transparent to the user of the S3 bucket, who at any time will have access to the same
records available by the successfully uploaded files.

In the current version, as opposed to the default and field partitioners, the time-based partitioners still depend on
wall-clock time to partition data, similar to the way time-based partitioners available with the HDFS connector do.
A version of time-based partitioners based only on record timestamps that will guarantee exactly-once delivery to S3
will become soon available.


Quickstart
----------
In this Quickstart, we use the S3 connector to export data produced by the Avro console producer to S3.

Before you begin, make sure that Zookeeper, Kafka and SchemaRegistry services are running. Instructions on how to
start these services are available at the :ref:`Confluent Platform Quickstart<quickstart>`. You also need to create a
destination bucket in S3 with appropriate permissions in advance.

This Quickstart assumes that you started the required services with the default configurations and
you should make necessary changes according to the actual configurations used.

.. note:: You need to make sure the connector user has write access to the S3 bucket
   specified in ``s3.bucket.name`` and has deployed credentials in a way that makes them accessible through the
   ``DefaultAWSCredentialsProviderChain```. Alternatively, you may supply the connector with your custom credentials
   provider by setting the property ``s3.credentials.provider.class``.

First, start the Avro console producer::

  $ ./bin/kafka-avro-console-producer --broker-list localhost:9092 --topic s3_topic \
  --property value.schema='{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"}]}'

Then, in the console producer, type in::

  {"f1": "value1"}
  {"f1": "value2"}
  {"f1": "value3"}
  {"f1": "value4"}
  {"f1": "value5"}
  {"f1": "value6"}
  {"f1": "value7"}
  {"f1": "value8"}
  {"f1": "value9"}

The nine records entered are published to the Kafka topic ``s3_topic`` in Avro format.

Before starting the connector, please make sure that the configurations in
``etc/kafka-connect-s3/quickstart-s3.properties`` are properly set to your configurations of S3, e.g. ``s3.bucket.name``
points to your bucket, ``s3.region`` directs to your S3 region and ``flush.size=3`` for this example. Next, run the
following command to start Kafka connect with the S3 connector::

  $ ./bin/connect-standalone etc/schema-registry/connect-avro-standalone.properties \
  etc/kafka-connect-s3/quickstart-s3.properties

You should see that the process starts up, logs a few messages and then uploads data from Kafka
to S3. Once the connector finishes ingesting records to S3, check that the data is available
in S3, for instance using the AWS CLI::

  $ aws s3api list-objects --bucket "your-bucket-name"

You should see three objects with keys::

``topics/s3_topic/partition=0/s3_topic+0+0000000000.avro``
``topics/s3_topic/partition=0/s3_topic+0+0000000003.avro``
``topics/s3_topic/partition=0/s3_topic+0+0000000006.avro``

Each file is encoded as ``topic+kafkaPartition+startOffset.format``.

To verify the contents, first copy each file from S3 to your local filesystem, for instance by running::

  $ aws s3 cp s3://<your-bucket>/topics/s3_topic/partition=0/s3_topic+0+0000000000.avro

and use ``avro-tools-1.7.7.jar``
(available in `Apache mirrors <http://mirror.metrocast.net/apache/avro/avro-1.7.7/java/avro-tools-1.7.7.jar>`_) to
print the records::

  $ java -jar avro-tools-1.7.7.jar tojson s3_topic+0+0000000000.avro

For the file above, you should see the following output::

  {"f1":"value1"}
  {"f1":"value2"}
  {"f1":"value3"}

with the rest of the records contained in the other two files.


Configuration
-------------
This section first gives example configurations that cover common scenarios, then provides an exhaustive
description of the available configuration options.

Example
~~~~~~~
Here is the content of ``etc/kafka-connect-s3/quickstart-s3.properties``::

Format and Partitioner
~~~~~~~~~~~~~~~~~~~~~~

Schema Evolution
----------------
