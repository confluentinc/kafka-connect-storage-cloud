#!/usr/bin/env groovy

def getVaultSecretsList() {
  return [["connect/s3sink_it", "creds", "/tmp/s3_sink_aws_credentials.json", "AWS_CREDENTIALS_PATH"]]
}

common {
  slackChannel = '#connect-warn'
  upstreamProjects = ['confluentinc/common','confluentinc/schema-registry']
  nodeLabel = 'docker-oraclejdk8'
  pintMerge = true
  twistlockCveScan = true
  secret_file_list = getVaultSecretsList()
  downStreamValidate = false
}