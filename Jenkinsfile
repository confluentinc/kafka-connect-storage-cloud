#!/usr/bin/env groovy

#!/usr/bin/env groovy

/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

def getVaultSecretsList() {
  return [["aws/connect_s3sink_it", "creds", "${env.WORKSPACE}/aws_creds.json", "AWS_CREDENTIAL_PROFILES_FILE"]]
}

common {
  slackChannel = '#connect-warn'
  nodeLabel = 'docker-oraclejdk8'
  upstreamProjects = 'confluentinc/kafka-connect-storage-common'
  pintMerge = true
  twistlockCveScan = true
  secret_file_list = getVaultSecretsList()
  downStreamValidate = false
}

