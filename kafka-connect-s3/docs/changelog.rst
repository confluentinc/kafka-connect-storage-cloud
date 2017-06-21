.. _s3_connector_changelog:

Changelog
=========

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
