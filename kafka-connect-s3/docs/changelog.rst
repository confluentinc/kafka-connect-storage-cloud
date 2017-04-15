.. _s3_connector_changelog:

Changelog
=========

Version 3.2.1
-------------

S3 Connector
~~~~~~~~~~~~~~

* `PR-33 <https://github.com/confluentinc/kafka-connect-s3/pull/33>`_ - Separate JSON records using line separator
instead of single white space.
* `PR-34 <https://github.com/confluentinc/kafka-connect-s3/pull/34>`_ - Reduce the default s3.part.size to 25MB to avoid
OOM exceptions with the current default java heap size settings for Connect.
* `PR-32 <https://github.com/confluentinc/kafka-connect-s3/pull/32>`_ - Add s3.region property to quickstart config and
docs.
* `PR-25 <https://github.com/confluentinc/kafka-connect-s3/pull/25>`_ - flush.size doc fixes.

Version 3.2.0
-------------

S3 Connector
~~~~~~~~~~~~~~

Initial Version
