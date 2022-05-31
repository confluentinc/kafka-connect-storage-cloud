#!/usr/bin/env groovy

def getVaultSecretsList() {
  return [["connect/s3sink_it", "creds", "/tmp/s3_sink_aws_credentials.json", "AWS_CREDENTIALS_PATH"]]
}

common {
  slackChannel = '#connect-warn'
  upstreamProjects = 'confluentinc/kafka-connect-storage-common'
  nodeLabel = 'docker-debian-jdk8'
  pintMerge = true
  twistlockCveScan = true
  secret_file_list = getVaultSecretsList()
  downStreamValidate = false
  disableConcurrentBuilds = true
}
