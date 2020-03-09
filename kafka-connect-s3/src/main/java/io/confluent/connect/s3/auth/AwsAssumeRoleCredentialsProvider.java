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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

/**
 * AWS credentials provider that uses the AWS Security Token Service to assume a Role and create a
 * temporary, short-lived session to use for authentication.  This credentials provider does not
 * support refreshing the credentials in a background thread.
 */
public class AwsAssumeRoleCredentialsProvider implements AWSCredentialsProvider, Configurable {

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

  @Override
  public void configure(Map<String, ?> configs) {
    AbstractConfig config = new AbstractConfig(STS_CONFIG_DEF, configs);
    roleArn = config.getString(ROLE_ARN_CONFIG);
    roleExternalId = config.getString(ROLE_EXTERNAL_ID_CONFIG);
    roleSessionName = config.getString(ROLE_SESSION_NAME_CONFIG);
  }

  @Override
  public AWSCredentials getCredentials() {
    return new STSAssumeRoleSessionCredentialsProvider.Builder(roleArn, roleSessionName)
        .withStsClient(AWSSecurityTokenServiceClientBuilder.defaultClient())
        .withExternalId(roleExternalId)
        .build()
        .getCredentials();
  }

  @Override
  public void refresh() {
    // Nothing to do really, since we acquire a new session every getCredentials() call.
  }

}
