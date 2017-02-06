Configuration Options
---------------------

S3
^^^^

``hdfs.url``

  The HDFS connection URL. This configuration has the format of hdfs:://hostname:port and specifies the HDFS to export data to.

``s3.region``;

  * Type: string
  * Default: Regions.DEFAULT_REGION
  * Importance: high

``s3.bucket.name``;

  S3 bucket under which the connector will store the exported records from Kafka.

  * Type: string
  * Importance: high

``s3.ssea.name``;

  Option to use a server-side encryption algorithm for data stored in S3.

  * Type: string
  * Default: ""
  * Importance: medium

``s3.part.size``;

  The size, in bytes, of the first *n-1* parts in a *n-way* multi-part upload of a large object to S3.

  * Type: int
  * Default: 104857600 (100MB)
  * Importance: medium

``s3.wan.mode``;

  * Type: boolean
  * Default: false
  * Importance: medium

``s3.credentials.provider.class``;

  * Type: string
  * Importance: high

``topics.dir``
  Top level HDFS directory to store the data ingested from Kafka.

  * Type: string
  * Default: topics
  * Importance: high

``logs.dir``
  Top level HDFS directory to store the write ahead logs.

  * Type: string
  * Default: logs
  * Importance: high

``format.class``
  The format class to use when writing data to HDFS.

  * Type: string
  * Default: io.confluent.connect.hdfs.avro.AvroFormat
  * Importance: high

   TOPICS_DIR_CONFIG = ``topics.dir``;
   TOPICS_DIR_DOC = ``Top level directory to store the data ingested from Kafka.``;
   TOPICS_DIR_DEFAULT = ``topics``;
   TOPICS_DIR_DISPLAY = ``Topics directory``;

   STORE_URL_CONFIG = ``store.url``;
   STORE_URL_DOC = ``Store's connection URL, if applicable.``;
   STORE_URL_DEFAULT = null;
   STORE_URL_DISPLAY = ``Store URL``;

   DIRECTORY_DELIM_CONFIG = ``directory.delim``;
   DIRECTORY_DELIM_DOC = ``Directory delimiter pattern``;
   DIRECTORY_DELIM_DEFAULT = ``/``;
   DIRECTORY_DELIM_DISPLAY = ``Directory Delimiter``;

   FILE_DELIM_CONFIG = ``file.delim``;
   FILE_DELIM_DOC = ``File delimiter pattern``;
   FILE_DELIM_DEFAULT = ``+``;
   FILE_DELIM_DISPLAY = ``File Delimiter``;

   STORAGE_CLASS_CONFIG = ``storage.class``;
   STORAGE_CLASS_DOC = ``The underlying storage layer.``;
   STORAGE_CLASS_DISPLAY = ``Storage Class``;

Storage Common
^^^^

Hive
^^^^

Security
^^^^^^^^

Schema
^^^^^^

Connector
^^^^^^^^^

Internal
^^^^^^^^

