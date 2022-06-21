# Kafka Connect Connector for S3
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bhttps%3A%2F%2Fgithub.com%2Fconfluentinc%2Fkafka-connect-storage-cloud.svg?type=shield)](https://app.fossa.io/projects/git%2Bhttps%3A%2F%2Fgithub.com%2Fconfluentinc%2Fkafka-connect-storage-cloud?ref=badge_shield)


*kafka-connect-storage-cloud* is the repository for Confluent's [Kafka Connectors](http://kafka.apache.org/documentation.html#connect)
designed to be used to copy data from Kafka into Amazon S3. 

## Kafka Connect Sink Connector for Amazon Simple Storage Service (S3)

Documentation for this connector can be found [here](http://docs.confluent.io/current/connect/connect-storage-cloud/kafka-connect-s3/docs/index.html).

Blogpost for this connector can be found [here](https://www.confluent.io/blog/apache-kafka-to-amazon-s3-exactly-once).

# Development

NOTE: this is an exact fork of v10.0.8:

```shell
 git checkout tags/v10.0.8 -b v10.0.8
```

TODO: env: IntelliJ Community edition (I'm sure VSCode would work fine too, but I couldn't bear dealing with random Java extensions)
NBNBNB: if using IntelliJ in combination with sdkman, remember to set the IntelliJ Maven path to the sdkman maven path!
Otherwise the various `mvn installs`s on the dependent packages won't be linked properly and there will be PAIN

## Installing Java 11 and maven with sdkman

Why Java 11? [These Confluent docs](https://docs.confluent.io/platform/current/installation/versions-interoperability.html#java) suggest that Java 11
is used in the Confluent platform - I've inferred that Java 11 is probably used to write the Confluent connectors. 

```sh
$ curl -s "https://get.sdkman.io" | bash
$ sdk install java 11.0.2-open
$ sdk install maven
```

## Building a .zip snapshot of this forked connector

scratch, command to install kafka into local maven repo:

```shell
# https://stackoverflow.com/a/4955695
mvn install:install-file \
   -Dfile="/Users/jasonbrewer/workshop/kafka-connectors/kafka/build/libs/kafka-7.3.0-22-ccs.jar" \
   -DgroupId="org.apache.kafka" \
   -DartifactId="org.apache.kafka" \
   -Dversion="7.3.0-22-ccs" \
   -Dpackaging="jar" \
   -DgeneratePom=true
```

1. Build snapshots of the upstream Confluent projects below
   1. confluentinc/kafka (NOT Apache Kafka!) - `git checkout tags/v7.3.0-22-ccs -b v7.3.0-22-ccs && ./gradlew srcJar && ./gradlew jar` (maybe)
      1. Then, to add it to the local maven registry:
      2. `./gradlewAll publishToMavenLocal`
   2. confluentinc/rest-utils - exact commit = 49f3b66f67f58b4e1c0ddd0a0d642baccec8a122 - `mvn install`
   3. onfluentinc/schema-registry - exact commit = 49f3b66f67f58b4e1c0ddd0a0d642baccec8a122 - `mvn install`
   4. confluentinc/common - `git checkout tags/v7.3.0-469 -b 7.3.0-469 && mvn install`
   5. confluentinc/kafka-connect-storage-common - `git checkout tags/v11.0.4 -b 11.0.4 && mvn -U clean install -pl \!:kafka-connect-storage-hive`
      1. the `-pl \!:kafka-connect-storage-hive` instructs maven NOT to build that module - the install currently fails because a dependency mirror is not available. A build of kafka-connect-s3 does not seem to depend on it...
      2. NOTE, in the top level pom.xml, change the version of io.confluent.common to '7.3.0-469' to match the version in step 4. i.e.
      ```
        <parent>
            <groupId>io.confluent</groupId>
            <artifactId>common</artifactId>
            <version>7.3.0-469</version>
        </parent>
      ```
2. Note that all of these `mvn install`s take a while, I think mainly due to the test runs. They may be suppressed with: `mvn install -DskipTests`
3. Run the tests, just to make sure the connector code executes: `cd kafka-connect-s3 && mvn test`
   1. Or without the 'checkstyle' / linting step `mvn test -Dcheckstyle.skip`
4. TODO: use maven to build (mvn compile?), then compress with ... (a tool that produces zip archives)

## Updating the connector ZIP in S3

- See udx-infra, essentially add the new .zip archive to the right directory and run terragrunt apply to overwrite the extension file
- Then (somehow) restart the msk_connect instance and see your new connector in action! (somehow)

## Original docs

To build a development version you'll need a recent version of Kafka 
as well as a set of upstream Confluent projects, which you'll have to build from their appropriate snapshot branch.
See [the kafka-connect-storage-common FAQ](https://github.com/confluentinc/kafka-connect-storage-common/wiki/FAQ)
for guidance on this process.

You can build *kafka-connect-storage-cloud* with Maven using the standard lifecycle phases.


# Contribute

- Source Code: https://github.com/confluentinc/kafka-connect-storage-cloud
- Issue Tracker: https://github.com/confluentinc/kafka-connect-storage-cloud/issues


# License

This project is licensed under the [Confluent Community License](LICENSE).


[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bhttps%3A%2F%2Fgithub.com%2Fconfluentinc%2Fkafka-connect-storage-cloud.svg?type=large)](https://app.fossa.io/projects/git%2Bhttps%3A%2F%2Fgithub.com%2Fconfluentinc%2Fkafka-connect-storage-cloud?ref=badge_large)
