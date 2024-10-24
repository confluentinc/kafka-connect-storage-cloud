/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.s3.auth;

import io.confluent.connect.storage.common.util.StringUtils;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.util.Map;

import static io.confluent.connect.s3.S3SinkConnectorConfig.AWS_ACCESS_KEY_ID_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.AWS_SECRET_ACCESS_KEY_CONFIG;

/**
 * AWS credentials provider that uses the AWS Security Token Service to assume a Role and create a
 * temporary, short-lived session to use for authentication.  This credentials provider does not
 * support refreshing the credentials in a background thread.
 */
public class AwsAssumeRoleCredentialsProvider implements AwsCredentialsProvider, Configurable {

  public static final String ROLE_EXTERNAL_ID_CONFIG = "sts.role.external.id";
  public static final String ROLE_ARN_CONFIG = "sts.role.arn";
  public static final String ROLE_SESSION_NAME_CONFIG = "sts.role.session.name";

  private static final ConfigDef STS_CONFIG_DEF = new ConfigDef()
      .define(
          ROLE_EXTERNAL_ID_CONFIG,
          ConfigDef.Type.STRING,
          ConfigDef.Importance.MEDIUM,
          "The role external ID used when retrieving session credentials under an assumed role."
      ).define(
          ROLE_ARN_CONFIG,
          ConfigDef.Type.STRING,
          ConfigDef.Importance.HIGH,
          "Role ARN to use when starting a session."
      ).define(
          ROLE_SESSION_NAME_CONFIG,
          ConfigDef.Type.STRING,
          ConfigDef.Importance.HIGH,
          "Role session name to use when starting a session"
      );

  private String roleArn;
  private String roleExternalId;
  private String roleSessionName;

  private AwsBasicCredentials basicCredentials;

  // STSAssumeRoleSessionCredentialsProvider takes care of refreshing short-lived
  // credentials 60 seconds before it's expiry
  private StsAssumeRoleCredentialsProvider stsCredentialProvider;

  @Override
  public void configure(Map<String, ?> configs) {
    AbstractConfig config = new AbstractConfig(STS_CONFIG_DEF, configs);
    roleArn = config.getString(ROLE_ARN_CONFIG);
    roleExternalId = config.getString(ROLE_EXTERNAL_ID_CONFIG);
    roleSessionName = config.getString(ROLE_SESSION_NAME_CONFIG);
    final String accessKeyId = (String) configs.get(AWS_ACCESS_KEY_ID_CONFIG);
    final String secretKey = (String) configs.get(AWS_SECRET_ACCESS_KEY_CONFIG);
    if (StringUtils.isNotBlank(accessKeyId) && StringUtils.isNotBlank(secretKey)) {
      basicCredentials = AwsBasicCredentials.create(accessKeyId, secretKey);
      //TODO: Set defaults if any to StsClient.builder() equiaveltn to
      // AWSSecurityTokenServiceClientBuilder.standard()
      stsCredentialProvider = StsAssumeRoleCredentialsProvider.builder()
          .stsClient(StsClient.builder()
              .credentialsProvider(StaticCredentialsProvider.create(basicCredentials)).build())
          .refreshRequest(
              AssumeRoleRequest.builder()
                  .roleArn(roleArn)
                  .roleSessionName(roleSessionName)
                  .externalId(roleExternalId)
                  .build())
          .build();
    } else {
      basicCredentials = null;
      stsCredentialProvider = StsAssumeRoleCredentialsProvider.builder()
          // TODO: Add equiaveltn of AWSSecurityTokenServiceClientBuilder.defaultClient()
          .stsClient(StsClient.create())
          .refreshRequest(AssumeRoleRequest.builder()
              .roleArn(roleArn)
              .roleSessionName(roleSessionName)
              .externalId(roleExternalId)
              .build())
          // default sts client will internally use default credentials chain provider
          // https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default
          .build();
    }
  }

  @Override
  public AwsCredentials resolveCredentials() {
    return stsCredentialProvider.resolveCredentials();
  }
}
