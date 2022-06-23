#! /bin/bash

# e.g. ~/workshop/udx-infra/terraform/modules/data-lake
PATH_TO_INFRA=$1
OUTPUT_ZIP_NAME="confluentinc-kafka-connect-s3-10.0.8-UdxStreamPartitioner.zip"

mvn clean install -DskipTests && \
echo "Removing existing build in infra, just to be safe..." && \
rm -f "$PATH_TO_INFRA/$OUTPUT_ZIP_NAME" || true && \
echo "Copying ZIP archived build to $PATH_TO_INFRA" && \
cp kafka-connect-s3/target/components/packages/confluentinc-kafka-connect-s3-10.0.8.zip "$PATH_TO_INFRA/$OUTPUT_ZIP_NAME"