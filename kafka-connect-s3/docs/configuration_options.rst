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
  The time interval in milliseconds to invoke file commits. This configuration ensures that file commits are invoked every configured interval. This configuration is useful when data ingestion rate is low and the connector didn't write enough messages to commit files.The default value -1 means that this feature is disabled.

  * Type: long
  * Default: -1
  * Importance: high

``rotate.schedule.interval.ms``
  The time interval in milliseconds to periodically invoke file commits. This configuration ensures that file commits are invoked every configured interval. Time of commit will be adjusted to 00:00 of selected timezone. Commit will be performed at scheduled time regardless previous commit time or number of messages. This configuration is useful when you have to commit your data based on current server time, like at the beginning of every hour. The default value -1 means that this feature is disabled.

  * Type: long
  * Default: -1
  * Importance: medium

``retry.backoff.ms``
  The retry backoff in milliseconds. This config is used to notify Kafka connect to retry delivering a message batch or performing recovery in case of transient exceptions.

  * Type: long
  * Default: 5000
  * Importance: low

``shutdown.timeout.ms``
  Clean shutdown timeout. This makes sure that asynchronous Hive metastore updates are completed during connector shutdown.

  * Type: long
  * Default: 3000
  * Importance: medium

``filename.offset.zero.pad.width``
  Width to zero pad offsets in store's filenames if offsets are too short in order to provide fixed width filenames that can be ordered by simple lexicographic sorting.

  * Type: int
  * Default: 10
  * Valid Values: [0,...]
  * Importance: low

``schema.cache.size``
  The size of the schema cache used in the Avro converter.

  * Type: int
  * Default: 1000
  * Importance: low

S3
^^

``s3.bucket.name``
  The S3 Bucket.

  * Type: string
  * Importance: high

``s3.part.size``
  The Part Size in S3 Multi-part Uploads.

  * Type: int
  * Default: 104857600
  * Valid Values: [5242880,...,2147483647]
  * Importance: high

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

``s3.region``
  The AWS region to be used the connector.

  * Type: string
  * Default: us-west-2
  * Valid Values: [us-gov-west-1, ap-northeast-1, ap-northeast-2, ap-south-1, ap-southeast-1, ap-southeast-2, ca-central-1, eu-central-1, eu-west-1, eu-west-2, sa-east-1, us-east-1, us-east-2, us-west-1, us-west-2, cn-north-1]
  * Importance: medium

``s3.credentials.provider.class``
  Credentials provider or provider chain to use for authentication to AWS. By default the  connector uses 'DefaultAWSCredentialsProviderChain'.

  * Type: class
  * Default: com.amazonaws.auth.DefaultAWSCredentialsProviderChain
  * Valid Values: Any class implementing: interface com.amazonaws.auth.AWSCredentialsProvider
  * Importance: low
