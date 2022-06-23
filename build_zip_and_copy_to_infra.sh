#! /bin/bash

# e.g. ~/workshop/udx-infra/terraform/modules/data-lake
PATH_TO_INFRA=$1

mvn clean install -DskipTests && \
echo "Removing existing build in infra, just to be safe..." && \
rm -f "$1/confluentinc-kafka-connect-s3-udc-edit-10.0.8.zip" || true && \
echo "Copying ZIP archived build to $1" && \
cp kafka-connect-s3/target/components/packages/confluentinc-kafka-connect-s3-10.0.8.zip "$1/confluentinc-kafka-connect-s3-udc-edit-10.0.8.zip"