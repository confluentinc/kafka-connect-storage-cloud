#!/usr/bin/env groovy

def getVaultSecretsList() {
  return [["connect/s3sink_it", "creds", "aws_credentials", "AWS_CREDENTIAL_PROFILES_FILE"]]
}

common {
  slackChannel = '#connect-warn'
  upstreamProjects = 'confluentinc/kafka-connect-storage-common'
  nodeLabel = 'docker-oraclejdk8'
  pintMerge = true
  twistlockCveScan = true
  secret_file_list = getVaultSecretsList()
  downStreamValidate = false
}

