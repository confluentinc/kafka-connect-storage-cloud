.. _s3_configuration_options:

S3 Connector Configuration Options
----------------------------------

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

``enhanced.avro.schema.support``
  Enable enhanced avro schema support in AvroConverter: Enum symbol preservation and Package Name awareness

  * Type: boolean
  * Default: false
  * Importance: low

``connect.meta.data``
  Allow connect converter to add its meta data to the output schema

  * Type: boolean
  * Default: true
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

``avro.codec``
  The Avro compression codec to be used for output  files. Available values: null, deflate, snappy and bzip2 (CodecSource is org.apache.avro.file.CodecFactory)

  * Type: string
  * Default: null
  * Valid Values: [null, deflate, snappy, bzip2]
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
  Credentials provider or provider chain to use for authentication to AWS. By default the connector uses 'DefaultAWSCredentialsProviderChain'.

  * Type: class
  * Default: com.amazonaws.auth.DefaultAWSCredentialsProviderChain
  * Valid Values: Any class implementing: interface com.amazonaws.auth.AWSCredentialsProvider
  * Importance: low

``s3.ssea.name``
  The S3 Server Side Encryption Algorithm.

  * Type: string
  * Default: ""
  * Valid Values: [, AES256, aws:kms]
  * Importance: low

``s3.sse.customer.key``
  The S3 Server Side Encryption Customer-Provided Key (SSE-C).

  * Type: password
  * Default: [hidden]
  * Importance: low

``s3.sse.kms.key.id``
  The name of the AWS Key Management Service (AWS-KMS) key to be used for server side encryption of the S3 objects. No encryption is used when no key is provided, but it is enabled when 'aws:kms' is specified as encryption algorithm with a valid key name.

  * Type: string
  * Default: ""
  * Importance: low

``s3.acl.canned``
  An S3 canned ACL header value to apply when writing objects.

  * Type: string
  * Default: null
  * Valid Values: [private, public-read, public-read-write, authenticated-read, log-delivery-write, bucket-owner-read, bucket-owner-full-control, aws-exec-read]
  * Importance: low

``s3.wan.mode``
  Use S3 accelerated endpoint.

  * Type: boolean
  * Default: false
  * Importance: medium

``s3.compression.type``
  Compression type for file written to S3. Applied when using JsonFormat or ByteArrayFormat. Available values: none, gzip.

  * Type: string
  * Default: none
  * Valid Values: [none, gzip]
  * Importance: low

``s3.part.retries``
  Maximum number of retry attempts for failed requests. Zero means no retries. The actual number
  of attempts is determined by the S3 client based on multiple factors, including, but not
  limited to - the value of this parameter, type of exception occurred,
  throttling settings of the underlying S3 client, etc.

  * Type: int
  * Default: 3
  * Valid Values: [0,...]
  * Importance: medium

``s3.retry.backoff.ms``
  How long to wait in milliseconds before attempting the first retry of a failed S3 request. Upon a failure, this connector may wait up to twice as long as the previous wait, up to the maximum number of retries. This avoids retrying in a tight loop under failure scenarios.

  * Type: long
  * Default: 200
  * Valid Values: [0,...]
  * Importance: low

``format.bytearray.extension``
  Output file extension for ByteArrayFormat. Defaults to '.bin'

  * Type: string
  * Default: .bin
  * Importance: low

``format.bytearray.separator``
  String inserted between records for ByteArrayFormat. Defaults to 'System.lineSeparator()' and may contain escape sequences like '\n'. An input record that contains the line separator will look like multiple records in the output S3 object.

  * Type: string
  * Default: null
  * Importance: low

``s3.proxy.url``
  S3 Proxy settings encoded in URL syntax. This property is meant to be used only if you need to access S3 through a proxy.

  * Type: string
  * Default: ""
  * Importance: low

``s3.proxy.user``
  S3 Proxy User. This property is meant to be used only if you need to access S3 through a proxy. Using ``s3.proxy.user`` instead of embedding the username and password in ``s3.proxy.url`` allows the password to be hidden in the logs.

  * Type: string
  * Default: null
  * Importance: low

``s3.proxy.password``
  S3 Proxy Password. This property is meant to be used only if you need to access S3 through a proxy. Using ``s3.proxy.password`` instead of embedding the username and password in ``s3.proxy.url`` allows the password to be hidden in the logs.

  * Type: password
  * Default: [hidden]
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
  * Dependents: ``partition.field.name``, ``partition.duration.ms``, ``path.format``, ``locale``, ``timezone``

``partition.field.name``
  The name of the partitioning field when FieldPartitioner is used.

  * Type: list
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

