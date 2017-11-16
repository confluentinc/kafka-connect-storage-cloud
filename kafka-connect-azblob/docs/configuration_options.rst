.. _s3_configuration_options:

Configuration Options
---------------------

Connector
^^^^^^^^^

``format.class``
  The format class to use when writing data to the store.

  * Type: class
  * Importance: high

``flush.size``
  Number of records written to store before invoking file commits.

  * Type: int
  * Importance: high

``rotate.interval.ms``
  The time interval in milliseconds to invoke file commits. This configuration ensures that file commits are invoked every configured interval. This configuration is useful when data ingestion rate is low and the connector didn't write enough messages to commit files. The default value -1 means that this feature is disabled.

  * Type: long
  * Default: -1
  * Importance: high

``rotate.schedule.interval.ms``
  The time interval in milliseconds to periodically invoke file commits. This configuration ensures that file commits are invoked every configured interval. Time of commit will be adjusted to 00:00 of selected timezone. Commit will be performed at scheduled time regardless previous commit time or number of messages. This configuration is useful when you have to commit your data based on current server time, like at the beginning of every hour. The default value -1 means that this feature is disabled.

  * Type: long
  * Default: -1
  * Importance: medium

``schema.cache.size``
  The size of the schema cache used in the Avro converter.

  * Type: int
  * Default: 1000
  * Importance: low

``retry.backoff.ms``
  The retry backoff in milliseconds. This config is used to notify Kafka connect to retry delivering a message batch or performing recovery in case of transient exceptions.

  * Type: long
  * Default: 5000
  * Importance: low

``filename.offset.zero.pad.width``
  Width to zero pad offsets in store's filenames if offsets are too short in order to provide fixed width filenames that can be ordered by simple lexicographic sorting.

  * Type: int
  * Default: 10
  * Valid Values: [0,...]
  * Importance: low

S3
^^

``s3.bucket.name``
  The S3 Bucket.

  * Type: string
  * Importance: high

``s3.region``
  The AWS region to be used the connector.

  * Type: string
  * Default: us-west-2
  * Valid Values: [us-gov-west-1, ap-northeast-1, ap-northeast-2, ap-south-1, ap-southeast-1, ap-southeast-2, ca-central-1, eu-central-1, eu-west-1, eu-west-2, sa-east-1, us-east-1, us-east-2, us-west-1, us-west-2, cn-north-1]
  * Importance: medium

``s3.part.size``
  The Part Size in S3 Multi-part Uploads.

  * Type: int
  * Default: 26214400
  * Valid Values: [5242880,...,2147483647]
  * Importance: high

``s3.credentials.provider.class``
  Credentials provider or provider chain to use for authentication to AWS. By default the  connector uses 'DefaultAWSCredentialsProviderChain'.

  * Type: class
  * Default: com.amazonaws.auth.DefaultAWSCredentialsProviderChain
  * Valid Values: Any class implementing: interface com.amazonaws.auth.AWSCredentialsProvider
  * Importance: low

``s3.ssea.name``
  The S3 Server Side Encryption Algorithm.

  * Type: string
  * Default: ""
  * Importance: low

``s3.wan.mode``
  Use S3 accelerated endpoint.

  * Type: boolean
  * Default: false
  * Importance: medium

``avro.codec``
  The Avro compression codec to be used for output files. Available values: null, deflate, snappy and bzip2 (codec source is org.apache.avro.file.CodecFactory)

  * Type: string
  * Default: null
  * Importance: low

Storage
^^^^^^^

``storage.class``
  The underlying storage layer.

  * Type: class
  * Importance: high

``topics.dir``
  Top level directory to store the data ingested from Kafka.

  * Type: string
  * Default: topics
  * Importance: high

``store.url``
  Store's connection URL, if applicable.

  * Type: string
  * Default: null
  * Importance: high

``directory.delim``
  Directory delimiter pattern

  * Type: string
  * Default: /
  * Importance: medium

``file.delim``
  File delimiter pattern

  * Type: string
  * Default: +
  * Importance: medium

Partitioner
^^^^^^^^^^^

``partitioner.class``
  The partitioner to use when writing data to the store. You can use ``DefaultPartitioner``, which preserves the Kafka partitions; ``FieldPartitioner``, which partitions the data to different directories according to the value of the partitioning field specified in ``partition.field.name``; ``TimeBasedPartitioner``, which partitions data according to ingestion time.

  * Type: class
  * Default: io.confluent.connect.storage.partitioner.DefaultPartitioner
  * Importance: high
  * Dependents: ``partition.field.name``, ``partition.duration.ms``, ``path.format``, ``locale``, ``timezone``, ``schema.generator.class``

``schema.generator.class``
  The schema generator to use with partitioners.

  * Type: class
  * Importance: high

``partition.field.name``
  The name of the partitioning field when FieldPartitioner is used.

  * Type: string
  * Default: ""
  * Importance: medium

``partition.duration.ms``
  The duration of a partition milliseconds used by ``TimeBasedPartitioner``. The default value -1 means that we are not using ``TimeBasedPartitioner``.

  * Type: long
  * Default: -1
  * Importance: medium

``path.format``
  This configuration is used to set the format of the data directories when partitioning with ``TimeBasedPartitioner``. The format set in this configuration converts the Unix timestamp to proper directories strings. For example, if you set ``path.format='year'=YYYY/'month'=MM/'day'=dd/'hour'=HH``, the data directories will have the format ``/year=2015/month=12/day=07/hour=15/``.

  * Type: string
  * Default: ""
  * Importance: medium

``locale``
  The locale to use when partitioning with ``TimeBasedPartitioner``.

  * Type: string
  * Default: ""
  * Importance: medium

``timezone``
  The timezone to use when partitioning with ``TimeBasedPartitioner``.

  * Type: string
  * Default: ""
  * Importance: medium

``timestamp.extractor``
  The extractor that gets the timestamp for records when partitioning with ``TimeBasedPartitioner``. It can be set to ``Wallclock``, ``Record`` or ``RecordField`` in order to use one of the built-in timestamp extractors or be given the fully-qualified class name of a user-defined class that extends the ``TimestampExtractor`` interface.

  * Type: string
  * Default: Wallclock
  * Importance: medium

``timestamp.field``
  The record field to be used as timestamp by the timestamp extractor.

  * Type: string
  * Default: timestamp
  * Importance: medium

