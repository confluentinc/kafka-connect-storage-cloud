.. _s3_connector_changelog:

Changelog
=========


Version 4.1.0
-------------

S3 Connector
~~~~~~~~~~~~~~

* `PR-150 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/150>`_ - Make confluent CLI command output shared
* `PR-148 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/148>`_ - Rely on jackson.version from common pom instead of defining it ourselves
* `PR-149 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/149>`_ - Bump Jackson version
* `PR-140 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/140>`_ - Fix for BufferOverflowException in S3OutputStream
* `PR-106 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/106>`_ - Add s3.sse.kms.key.id to specify a particular key in AWS KMS
* `PR-98 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/98>`_ - Add gzip compression support for json and byte array formats
* `PR-143 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/143>`_ - Support for multiple fields in field partitioner
* `PR-138 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/138>`_ - CC-1490: NPE when rotate.interval.ms is set but partitioner is not instance of TimeBasedPartitioner
* `PR-132 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/132>`_ - CC-1214: Import all the AvroData configs to storage sink connectors
* `PR-123 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/123>`_ - CC-1299: Refactor this code to use the AVRO_CODEC config key from storage-common
* `PR-114 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/114>`_ - Update to PowerMock 2.0.x and Mockito 2
* `PR-48 <https://github.com/confluentinc/kafka-connect-storage-common/pull/48>`_ - RecordField nested timestamp extraction
* `PR-57 <https://github.com/confluentinc/kafka-connect-storage-common/pull/57>`_ - Issue-53. Multiple field support for partitioning.
* `PR-55 <https://github.com/confluentinc/kafka-connect-storage-common/pull/55>`_ - CC-1489: NPE on records with null timestamp.
* `PR-45 <https://github.com/confluentinc/kafka-connect-storage-common/pull/45>`_ - HOTFIX: Consider all the properties in AvroDataConfig.
* `PR-52 <https://github.com/confluentinc/kafka-connect-storage-common/pull/52>`_ - CC-1333: Remove dependency on hive-exec and instead use hive-exec:core
* `PR-51 <https://github.com/confluentinc/kafka-connect-storage-common/pull/51>`_ - CC-1333: Remove dependency on hive-exec and instead use hive-exec:core
* `PR-50 <https://github.com/confluentinc/kafka-connect-storage-common/pull/50>`_ - CC-1333: Remove dependency on hive-exec and instead use hive-exec:core
* `PR-46 <https://github.com/confluentinc/kafka-connect-storage-common/pull/46>`_ - CC-1299: Create AVRO_CODEC ConfigKey

Version 4.0.1

S3 Connector
~~~~~~~~~~~~~~

* `PR-148 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/148>`_ - Rely on jackson.version from common pom instead of defining it ourselves
* `PR-149 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/149>`_ - Bump Jackson version
* `PR-140 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/140>`_ - Fix for BufferOverflowException in S3OutputStream
* `PR-138 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/138>`_ - CC-1490: NPE when rotate.interval.ms is set but partitioner is not instance of TimeBasedPartitioner
* `PR-132 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/132>`_ - CC-1214: Import all the AvroData configs to storage sink connectors
* `PR-55 <https://github.com/confluentinc/kafka-connect-storage-common/pull/55>`_ - CC-1489: NPE on records with null timestamp.
* `PR-45 <https://github.com/confluentinc/kafka-connect-storage-common/pull/45>`_ - HOTFIX: Consider all the properties in AvroDataConfig.

Version 4.0.0
-------------

S3 Connector
~~~~~~~~~~~~~~

* `PR-102 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/102>`_ - Allow to write records at the root of the bucket
* `PR-105 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/105>`_ - Add instructions about upstream builds
* `PR-97 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/97>`_ - MINOR: S3 Proxy Settings revisited
* `PR-103 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/103>`_ - Remove unused imports
* `PR-101 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/101>`_ - Remove unused imports
* `PR-85 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/85>`_ - Add ByteArrayFormat
* `PR-87 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/87>`_ - Add s3.canned.acl config
* `PR-84 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/84>`_ - Create CONTRIBUTING.md
* `PR-92 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/92>`_ - CC-1114: Switch to common pom and fix checkstyle issues.
* `PR-78 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/78>`_ - Retry s3 part upload
* `PR-80 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/80>`_ - HOTFIX: Remove refs to schema-generator config.
* `PR-60 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/60>`_ - Add logging when creating a new RecordWriter
* `PR-44 <https://github.com/confluentinc/kafka-connect-storage-common/pull/44>`_ - HOTFIX: Remove unused parameter in newConfigDef for PartitionerConfig.
* `PR-41 <https://github.com/confluentinc/kafka-connect-storage-common/pull/41>`_ - HOTFIX: Update dependencies
* `PR-40 <https://github.com/confluentinc/kafka-connect-storage-common/pull/40>`_ - HOTFIX: Make specific dependencies explicit.
* `PR-37 <https://github.com/confluentinc/kafka-connect-storage-common/pull/37>`_ - Remove unused imports
* `PR-35 <https://github.com/confluentinc/kafka-connect-storage-common/pull/35>`_ - Add missing modules to the dependencyManagement pom section so downstream projects will inherit the right version automatically.
* `PR-31 <https://github.com/confluentinc/kafka-connect-storage-common/pull/31>`_ - Remove schema.generator.class config and have Formats specify their own SchemaGenerator internally

Version 3.3.1
-------------

S3 Connector
~~~~~~~~~~~~~~

* `PR-116 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/116>`_ - HOTFIX: Add recommender for schema generator classes only on 3.3.x.
* `PR-112 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/112>`_ - CC-1244: 3.3.1 release notes
* `PR-108 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/108>`_ - HOTFIX: Handle primitive types in AvroFormat
* `PR-104 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/104>`_ - Fix new client creation to cater for URLs and region together
* `PR-100 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/100>`_ - CC-1167: Add recommenders for class type properties
* `PR-90 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/90>`_ - Add upstream project so build are triggered automatically
* `PR-82 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/82>`_ - CC-872: Update default part.size number in S3 docs and more.
* `PR-77 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/77>`_ - Update quickstart to use Confluent CLI.
* `PR-43 <https://github.com/confluentinc/kafka-connect-storage-common/pull/43>`_ - HOTFIX: Add recommender for schema generator classes only on 3.3.x.
* `PR-39 <https://github.com/confluentinc/kafka-connect-storage-common/pull/39>`_ - HOTFIX: Check simple class names in recommender.
* `PR-38 <https://github.com/confluentinc/kafka-connect-storage-common/pull/38>`_ - CC-1153: Isolate config instances and recommenders.
* `PR-36 <https://github.com/confluentinc/kafka-connect-storage-common/pull/36>`_ - CC-1153: Storage connectors can add recommendations for Class types
* `PR-33 <https://github.com/confluentinc/kafka-connect-storage-common/pull/33>`_ - Add upstream project so build are triggered automatically

Version 3.3.0
-------------

S3 Connector
~~~~~~~~~~~~~~

* `PR-28 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/28>`_ - HOTFIX: Explicitly deactivate licensing profiles by default.
* `PR-34 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/34>`_ - CC-524: Reduce the default s3.part.size to 25MB to avoid OOM with current default jvm heap size.
* `PR-33 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/33>`_ - CC-513: Newline is not appended between records when using JsonFormat.
* `PR-37 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/37>`_ - Enable compression of output Avro files.
* `PR-38 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/38>`_ - CC-530: Exclude storage-common jars when packaging S3 connector.
* `PR-19 <https://github.com/confluentinc/kafka-connect-storage-common/pull/19>`_ - Move to using io.confluent:common for deps.
* `PR-22 <https://github.com/confluentinc/kafka-connect-storage-common/pull/22>`_ - HOTFIX: Import recent changes from the hdfs connector.
* `PR-24 <https://github.com/confluentinc/kafka-connect-storage-common/pull/24>`_ - Convert - to _ in Hive table names.
* `PR-25 <https://github.com/confluentinc/kafka-connect-storage-common/pull/25>`_ - Include causes with ConfigExceptions caused by catching other exceptions.
* `PR-26 <https://github.com/confluentinc/kafka-connect-storage-common/pull/26>`_ - Add constructors to SchemaGenerators to support no parameters and a Map config.
* `PR-29 <https://github.com/confluentinc/kafka-connect-storage-common/pull/29>`_ - Timestamp information should be copied to projected SinkRecord.

Version 3.2.2
-------------

S3 Connector
~~~~~~~~~~~~~~

* `PR-19 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/19>`_ - CC-500: Provide exactly-once time-based partitioning in S3
* `PR-45 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/45>`_ - HOTFIX: S3SinkConnector should extend SinkConnector
* `PR-51 <https://github.com/confluentinc/kafka-connect-storage-cloud/pull/51>`_ - Allow custom partitioners to have their own configs
* `PR-27 <https://github.com/confluentinc/kafka-connect-storage-common/pull/27>`_ - HOTFIX: Include a trailing delimiter when verifying data format for Hive
* `PR-23 <https://github.com/confluentinc/kafka-connect-storage-common/pull/23>`_ - HOTFIX: Add test schema and record builder for records with timestamp field
* `PR-18 <https://github.com/confluentinc/kafka-connect-storage-common/pull/18>`_ - CC-497: Add timestamp based partitioners.

Version 3.2.1
-------------

S3 Connector
~~~~~~~~~~~~~~

* `PR-33 <https://github.com/confluentinc/kafka-connect-s3/pull/33>`_ - Separate JSON records using line separator instead of single white space.
* `PR-34 <https://github.com/confluentinc/kafka-connect-s3/pull/34>`_ - Reduce the default s3.part.size to 25MB to avoid OOM exceptions with the current default java heap size settings for Connect.
* `PR-32 <https://github.com/confluentinc/kafka-connect-s3/pull/32>`_ - Add s3.region property to quickstart config and docs.
* `PR-25 <https://github.com/confluentinc/kafka-connect-s3/pull/25>`_ - flush.size doc fixes.

Version 3.2.0
-------------

S3 Connector
~~~~~~~~~~~~~~

Initial Version
