S3 Connector
==============

The S3 connector, currently available as a sink, allows you to export data from Kafka topics to S3 objects in
either Avro or JSON formats. In addition, for certain data layouts, S3 connector exports data by guaranteeing
exactly-once delivery semantics to consumers of the S3 objects it produces.

Being a sink, the S3 connector periodically polls data from Kafka and in turn uploads it
to S3. A partitioner is used to split the data of every Kafka partition into chunks. Each chunk of data is
represented as an S3 object, whose key name encodes the topic, the Kafka partition and the start offset of
this data chunk. If no partitioner is specified in the configuration, the default partitioner which
preserves Kafka partitioning is used. The size of each data chunk is determined by the number of
records written to S3 and by schema compatibility.

Features
--------
The S3 connector offers a variety of features:

* **Exactly Once Delivery**: Records that are exported using a deterministic partitioner are delivered with exactly-once
  semantics regardless of the eventual consistency of S3.

* **Pluggable Data Format with or without Schema**: Out of the box, the connector supports writing data to S3 in Avro
  and JSON format. Besides records with schema, the connector supports exporting plain JSON records without schema in
  text files, one record per-line. In general, the connector may accept any format that provides an implementation of
  the ``Format`` interface.

* **Schema Evolution**: When schemas are used, the connector supports schema evolution based on schema compatibility
  modes. The available modes are: ``NONE``, ``BACKWARD``, ``FORWARD`` and ``FULL`` and a selection can be made
  by setting the property ``schema.compatibility`` in the connector's configuration. When the connector observes a
  schema change, it decides whether to roll the file or project the record to the proper schema according to
  the ``schema.compatibility`` configuration in use.

* **Pluggable Partitioner**: The connector comes out of the box with partitioners that support default partitioning
  based on Kafka partitions, field partitioning, and time-based partitioning in days or hours. You may implement your
  own partitioners by extending the ``Partitioner`` class. Additionally, you can customize time based partitioning by
  extending the ``TimeBasedPartitioner`` class.


Exactly-once delivery on top of eventual consistency
----------------------------------------------------
The S3 connector is able to provide exactly-once semantics to consumers of the objects it exports to S3, under the
condition that the connector is supplied with a deterministic partitioner.

Currently, out of the available partitioners, the default and field partitioners are deterministic. This implies that,
when any of these partitioners is used, splitting of files always happens at the same offsets for a given set of Kafka
records. These partitioners take into account ``flush.size`` and ``schema.compatibility`` to decide when to roll and
save a new file to S3. The connector always delivers files in S3 that contain the same records, even under the
presence of failures. If a connector task fails before an upload completes, the file does not become visible to S3. If,
on the other hand, a failure occurs after the upload has completed but before the corresponding offset is committed to
Kafka by the connector, then a re-upload will take place. However, such a re-upload is transparent to the user of the S3
bucket, who at any time will have access to the same records made eventually available by successful uploads to S3.

In the current version, time-based partitioners, as opposed to default and field partitioners, depend on wall-clock time
to partition data. A version of time-based partitioners based only on record timestamps that will guarantee exactly-once
delivery to S3 will become soon available.

Schema Evolution
----------------
The S3 connector supports schema evolution and reacts to schema changes of data according to the
``schema.compatibility`` configuration. In this section, we will explain how the
connector reacts to schema evolution under different values of ``schema.compatibility``. The
``schema.compatibility`` can be set to ``NONE``, ``BACKWARD``, ``FORWARD`` and ``FULL``, which means
NO compatibility, BACKWARD compatibility, FORWARD compatibility and FULL compatibility respectively.

* **NO Compatibility**: By default, the ``schema.compatibility`` is set to ``NONE``. In this case,
  the connector ensures that each file written to S3 has the proper schema. When the connector
  observes a schema change in data, it commits the current set of files for the affected topic
  partitions and writes the data with new schema in new files.

* **BACKWARD Compatibility**: If a schema is evolved in a backward compatible way, we can always
  use the latest schema to query all the data uniformly. For example, removing fields is backward
  compatible change to a schema, since when we encounter records written with the old schema that
  contain these fields we can just ignore them. Adding a field with a default value is also backward
  compatible.

  If ``BACKWARD`` is specified in the ``schema.compatibility``, the connector keeps track
  of the latest schema used in writing data to S3, and if a data record with a schema version
  larger than current latest schema arrives, the connector commits the current set of files
  and writes the data record with new schema to new files. For data records arriving at a later time
  with schema of an earlier version, the connector projects the data record to the latest schema
  before writing to the same set of files in S3.

* **FORWARD Compatibility**: If a schema is evolved in a forward compatible way, we can always
  use the oldest schema to query all the data uniformly. Removing a field that had a default value
  is forward compatible, since the old schema will use the default value when the field is missing.

  If ``FORWARD`` is specified in the ``schema.compatibility``, the connector projects the data to
  the oldest schema before writing to the same set of files in S3.

* **Full Compatibility**: Full compatibility means that old data can be read with the new schema
  and new data can also be read with the old schema.

  If ``FULL`` is specified in the ``schema.compatibility``, the connector performs the same action
  as ``BACKWARD``.

Schema evolution in the S3 connector works in the same way as in the `HDFS connector <../../../connect-hdfs/docs/hdfs_connector.html#schema-evolution>`_.
  
Automatic Retries
-----------------
The S3 connector may experience problems writing to the S3 bucket, due to network partitions,
interruptions, or even AWS throttling limits. In many cases,
the connector will retry the request a number of times before failing.
To prevent from further overloading the network or S3 service,
the connector uses an exponential backoff technique to give the network and/or service time to recover.
The technique adds randomness, called jitter, to the calculated backoff times to prevent a thundering herd,
where large numbers of requests from many tasks are submitted concurrently and overwhelm the service.
Randomness spreads out the retries from many tasks and should reduce the overall time required
to complete all outstanding requests compared to simple exponential backoff.
The goal is to spread out the requests to S3 as much as possible.

The maximum number of retry attempts is dictated by the ``s3.part.retries`` S3 connector
configuration property,
which defaults to 3 attempts. The delay for retries is dependent upon the connector's ``s3.retry.backoff.ms``
configuration property, which defaults to 200 milliseconds. The actual delay is randomized,
but the maximum delay can be calculated as a function of the number of retry attempts with ``${s3.retry.backoff.ms} * 2 ^ (retry-1)``,
where ``retry`` is the number of attempts taken so far in the current iteration.
In order to keep the maximum delay within a reasonable duration, it is capped at 24 hours.
For example, the following table shows the possible wait times
before submitting each of the 3 retry attempts:

.. table:: Range of backoff times for each retry using the default configuration
   :widths: auto

   =====  =====================  =====================  ==============================================
   Retry  Minimum Backoff (sec)  Maximum Backoff (sec)  Total Potential Delay from First Attempt (sec)
   =====  =====================  =====================  ==============================================
     1         0.0                      0.2                              0.2
     2         0.0                      0.4                              0.6
     3         0.0                      0.8                              1.4
   =====  =====================  =====================  ==============================================

Increasing the maximum number of retries adds more backoff:

.. table:: Range of backoff times for additional retries
   :widths: auto

   =====  =====================  =====================  ==============================================
   Retry  Minimum Backoff (sec)  Maximum Backoff (sec)  Total Potential Delay from First Attempt (sec)
   =====  =====================  =====================  ==============================================
     4         0.0                      1.6                              3.0
     5         0.0                      3.2                              6.2
     6         0.0                      6.4                             12.6
     7         0.0                     12.8                             25.4
     8         0.0                     25.6                             51.0
     9         0.0                     51.2                            102.2
    10         0.0                    102.4                            204.6
   =====  =====================  =====================  ==============================================

At some point, maximum backoff time will reach saturation and will be capped at 24 hours.
From the example below, all attempts starting with 20 will have maximum backoff time as 24 hours:

.. table:: Range of backoff times when reaching the cap of 24 hours
   :widths: auto

   =====  =====================  =====================  ==============================================
   Retry  Minimum Backoff (sec)  Maximum Backoff (sec)  Total Potential Delay from First Attempt (sec)
   =====  =====================  =====================  ==============================================
    15         0.0                   3276.8                             6553.4
    16         0.0                   6553.6                            13107.0
    17         0.0                  13107.2                            26214.2
    18         0.0                  26214.4                            52428.6
    19         0.0                  52428.8                           104857.4
    20         0.0                  86400.0                           191257.4
    21         0.0                  86400.0                           277657.4
   =====  =====================  =====================  ==============================================

It's not advised to set ``s3.part.retries`` too high since making more attempts after reaching a cap of 24 hours isn't practical.
You can adjust both the ``s3.part.retries`` and ``s3.retry.backoff.ms`` connector configuration
properties to achieve
the desired retry and backoff characteristics.

Quickstart
----------
In this Quickstart, we use the S3 connector to export data produced by the Avro console producer to S3.

Before you begin, you will need to create an
S3 destination bucket in advance and grant the user or IAM role running the connector
`write access <http://docs.aws.amazon.com/AmazonS3/latest/UG/EditingBucketPermissions.html>`_ to it.

Next, start the services with one command using Confluent CLI:

.. tip::

   If not already in your PATH, add Confluent's ``bin`` directory by running: ``export PATH=<path-to-confluent>/bin:$PATH``

.. sourcecode:: bash

   $ confluent start

Every service will start in order, printing a message with its status:

.. include:: ../../../../includes/cli.rst
    :start-line: 19
    :end-line: 36

.. note:: You need to make sure the connector user has write access to the S3 bucket
   specified in ``s3.bucket.name`` and has deployed credentials
   `appropriately <http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html>`_.
   You can also pass additional properties to credentials provider, please refer to the
   `Configurable credentials provider`_ section.

To import a few records with a simple schema in Kafka, start the Avro console producer as follows:

.. sourcecode:: bash

  $ ./bin/kafka-avro-console-producer --broker-list localhost:9092 --topic s3_topic \
  --property value.schema='{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"}]}'

Then, in the console producer, type in:

.. sourcecode:: bash

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
points to your bucket, ``s3.region`` directs to your S3 region and ``flush.size=3`` for this example.
Then start the S3 connector by loading its configuration with the following command:

.. sourcecode:: bash

   $ confluent load s3-sink
   {
     "name": "s3-sink",
     "config": {
       "connector.class": "io.confluent.connect.s3.S3SinkConnector",
       "tasks.max": "1",
       "topics": "s3_topic",
       "s3.region": "us-west-2",
       "s3.bucket.name": "confluent-kafka-connect-s3-testing",
       "s3.part.size": "5242880",
       "flush.size": "3",
       "storage.class": "io.confluent.connect.s3.storage.S3Storage",
       "format.class": "io.confluent.connect.s3.format.avro.AvroFormat",
       "schema.generator.class": "io.confluent.connect.storage.hive.schema.DefaultSchemaGenerator",
       "partitioner.class": "io.confluent.connect.storage.partitioner.DefaultPartitioner",
       "schema.compatibility": "NONE",
       "name": "s3-sink"
     },
     "tasks": []
   }

To check that the connector started successfully view the Connect worker's log by running:

.. sourcecode:: bash

  $ confluent log connect

Towards the end of the log you should see that the connector starts, logs a few messages, and then uploads
data from Kafka to S3.
Once the connector has ingested some records check that the data is available in S3, for instance by using AWS CLI:

.. sourcecode:: bash

  $ aws s3api list-objects --bucket "your-bucket-name"

You should see three objects with keys:

.. sourcecode:: bash

  topics/s3_topic/partition=0/s3_topic+0+0000000000.avro
  topics/s3_topic/partition=0/s3_topic+0+0000000003.avro
  topics/s3_topic/partition=0/s3_topic+0+0000000006.avro

Each file is encoded as ``<topic>+<kafkaPartition>+<startOffset>.<format>``.

To verify the contents, first copy each file from S3 to your local filesystem, for instance by running:

.. sourcecode:: bash

  $ aws s3 cp s3://<your-bucket>/topics/s3_topic/partition=0/s3_topic+0+0000000000.avro

and use ``avro-tools-1.8.2.jar``
(available in `Apache mirrors <http://mirror.metrocast.net/apache/avro/avro-1.8.2/java/avro-tools-1.8.2.jar>`_) to
print the records:

.. sourcecode:: bash

  $ java -jar avro-tools-1.8.2.jar tojson s3_topic+0+0000000000.avro

For the file above, you should see the following output:

.. sourcecode:: bash

  {"f1":"value1"}
  {"f1":"value2"}
  {"f1":"value3"}

with the rest of the records contained in the other two files.

Finally, stop the Connect worker as well as all the rest of the Confluent services by running:

.. sourcecode:: bash

      $ confluent stop

Your output should resemble:

.. include:: ../../../../includes/cli.rst
    :start-line: 55
    :end-line: 71

or stop all the services and additionally wipe out any data generated during this quickstart by running:

.. sourcecode:: bash

      $ confluent destroy

Your output should resemble:

.. include:: ../../../../includes/cli.rst
    :start-line: 55
    :end-line: 72

Configuration
-------------
This section gives example configurations that cover common scenarios. For detailed description of all the
available configuration options of the S3 connector go to :ref:`Configuration Options<s3_configuration_options>`

Configurable credentials provider
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Some use cases require more fine-grained credentials configuration for AWS so that each connector
could have its own credentials.

To use a configurable credentials provider, set the ``s3.credentials.provider.class``
to the name of a class that implements both the ``com.amazonaws.auth.AWSCredentialsProvider``
and ``org.apache.kafka.common.Configurable`` interfaces.
Then as needed, supply additional properties required by that provider by prefixing them
with ``s3.credentials.provider.``. These will all be passed to the credentials provider during configuration.

Basic Example
~~~~~~~~~~~~~
The example settings are contained in ``etc/kafka-connect-s3/quickstart-s3.properties`` as follows:

.. sourcecode:: bash

  name=s3-sink
  connector.class=io.confluent.connect.s3.S3SinkConnector
  tasks.max=1
  topics=s3_topic
  flush.size=3

The first few settings are common to most connectors. ``topics`` specifies the topics we want to export data from, in
this case ``s3_topic``. The property ``flush.size`` specifies the number of records per partition the connector needs
to write before completing a multipart upload to S3.

.. sourcecode:: bash

  s3.bucket.name=confluent-kafka-connect-s3-testing
  s3.part.size=5242880

The next settings are specific to Amazon S3. A mandatory setting is the name of your S3 bucket to host the exported
Kafka records. Other useful settings are ``s3.region``, which you should set if you use a region other than the
default, and ``s3.part.size`` to control the size of each part in the multipart uploads that will be used to upload a
single chunk of Kafka records.

.. sourcecode:: bash

  storage.class=io.confluent.connect.s3.storage.S3Storage
  format.class=io.confluent.connect.s3.format.avro.AvroFormat
  schema.generator.class=io.confluent.connect.storage.hive.schema.DefaultSchemaGenerator
  partitioner.class=io.confluent.connect.storage.partitioner.DefaultPartitioner

These class settings are required to specify the storage interface (here S3), the output file format, currently
``io.confluent.connect.s3.format.avro.AvroFormat`` or ``io.confluent.connect.s3.format.json.JsonFormat`` and the partitioner
class along with its schema generator class. When using a format with no schema definition, it is sufficient to set the
schema generator class to its default value.

.. sourcecode:: bash

  schema.compatibility=NONE

Finally, schema evolution is disabled in this example by setting ``schema.compatibility`` to ``NONE``, as explained above.


Write raw message values into S3
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
It is possible to use the S3 connector to write out the unmodified original message values into
newline-separated files in S3. We accomplish this by telling Connect to not deserialize any of the
messages, and by configuring the S3 connector to store the message values in a binary format in S3.

The first part of our S3 connector is similar to other examples:

.. sourcecode:: bash

  name=s3-raw-sink
  connector.class=io.confluent.connect.s3.S3SinkConnector
  tasks.max=1
  topics=s3_topic
  flush.size=3

The ``topics`` setting specifies the topics we want to export data from, in this case ``s3_topic``.
The property ``flush.size`` specifies the number of records per partition the connector needs
to write before completing a multipart upload to S3.

Next we need to configure the particulars of Amazon S3:

.. sourcecode:: bash

  s3.bucket.name=confluent-kafka-connect-s3-testing
  s3.region=us-west-2
  s3.part.size=5242880
  s3.compression.type=gzip

The ``s3.bucket.name`` is mandatory and names your S3 bucket where the exported Kafka records
should be written. Another useful setting is ``s3.region`` that you should set if you use a
region other than the default. And since the S3 connector uses
`multi-part uploads <https://docs.aws.amazon.com/AmazonS3/latest/dev/mpuoverview.html>`_,
you can use the ``s3.part.size`` to control the size of each of these continuous parts used to
upload Kafka records into a single S3 object. The part size affects throughput and
latency, as an S3 object is visible/available only after all parts are uploaded.
The ``s3.compression.type`` specifies that we want the S3 connector to compress our S3 objects
using GZIP compression, adding the ``.gz`` extension to any files (see below).

So far this example configuration is relatively typical of most S3 connectors.
Now lets define that we should read the raw message values and write them in
binary format:

.. sourcecode:: bash

  value.converter=org.apache.kafka.connect.converters.ByteArrayConverter
  format.class=io.confluent.connect.s3.format.bytearray.ByteArrayFormat
  storage.class=io.confluent.connect.s3.storage.S3Storage
  schema.compatibility=NONE

The ``value.converter`` setting overrides for our connector the default that is in the Connect
worker configuration, and we use the ``ByteArrayConverter`` to instruct Connect to skip
deserializing the message values and instead give the connector the message values in their raw
binary form. We use the ``format.class`` setting to instruct the S3 connector to write these
binary message values as-is into S3 objects. By default the message values written to the same S3
object will be separated by a newline character sequence, but you can control this with the
``format.bytearray.separator`` setting, and you may want to consider this if your messages might
contain newlines. Also, by default the files written to S3 will have an
extension of ``.bin`` (before compression, if enabled), or you can use the
``format.bytearray.extension`` setting to change the pre-compression filename extension.

Next we need to decide how we want to partition the consumed messages in S3 objects. We have a few
options, including the default partitioner that preserves the same partitions as in Kafka:

.. sourcecode:: bash

  partitioner.class=io.confluent.connect.storage.partitioner.DefaultPartitioner

Or, we could instead partition by the timestamp of the Kafka messages:

.. sourcecode:: bash

  partitioner.class=io.confluent.connect.storage.partitioner.TimeBasedPartitioner
  timestamp.extract=Record

or the timestamp that the S3 connector processes each message:

.. sourcecode:: bash

  partitioner.class=io.confluent.connect.storage.partitioner.TimeBasedPartitioner
  timestamp.extract=Wallclock

Custom partitioners are always an option, too. Just be aware that since the record value is
an opaque binary value, we cannot extract timestamps from fields using the ``RecordField``
option.

The S3 connector configuration outlined above results in newline-delimited gzipped objects in S3
with ``.bin.gz``.
