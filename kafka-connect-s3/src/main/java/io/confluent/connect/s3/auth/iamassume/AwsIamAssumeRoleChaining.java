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

package io.confluent.connect.s3.auth.iamassume;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import static io.confluent.connect.s3.S3SinkConnectorConfig.ASSUME_AWS_ACCESS_KEY_ID_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.ASSUME_AWS_SECRET_ACCESS_KEY_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.CREDENTIALS_PROVIDER_CLASS_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.CUSTOMER_ROLE_ARN_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.CUSTOMER_ROLE_EXTERNAL_ID_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.MIDDLEWARE_ROLE_ARN_CONFIG;
import java.util.Map;

public class AwsIamAssumeRoleChaining implements AWSCredentialsProvider {

  private static final ConfigDef STS_CONFIG_DEF = new ConfigDef()
      .define(
        CUSTOMER_ROLE_EXTERNAL_ID_CONFIG,
        ConfigDef.Type.STRING,
        ConfigDef.Importance.HIGH,
        "The role external ID used when retrieving session credentials under an assumed role."
      ).define(
        CUSTOMER_ROLE_ARN_CONFIG,
        ConfigDef.Type.STRING,
        ConfigDef.Importance.HIGH,
        "Role ARN to use when starting a session."
      ).define(
        MIDDLEWARE_ROLE_ARN_CONFIG,
        ConfigDef.Type.STRING,
        ConfigDef.Importance.HIGH,
        "Role ARN to use when starting a session."
      ).define(
        ASSUME_AWS_ACCESS_KEY_ID_CONFIG,
        ConfigDef.Type.STRING,
        ConfigDef.Importance.HIGH,
        "The secret access key used to authenticate personal AWS credentials such as IAM "
            + "credentials. Use only if you do not wish to authenticate by using a credentials "
            + "provider class via ``"
            + CREDENTIALS_PROVIDER_CLASS_CONFIG
            + "``"
      ).define(
        ASSUME_AWS_SECRET_ACCESS_KEY_CONFIG,
        ConfigDef.Type.PASSWORD,
        ConfigDef.Importance.HIGH,
        "The secret access key used to authenticate personal AWS credentials such as IAM "
            + "credentials. Use only if you do not wish to authenticate by using a credentials "
            + "provider class via ``"
            + CREDENTIALS_PROVIDER_CLASS_CONFIG
            + "``"
      );

  private String customerRoleArn;
  private String customerRoleExternalId;
  private String middlewareRoleArn;
  private STSAssumeRoleSessionCredentialsProvider stsCredentialProvider;
  private String accessSecret = "";
  private String accessKeyId = "";

  // Method to initiate role chaining
  public void configure(Map<String, ?> configs) {
    // Assume the initial role
    AbstractConfig config = new AbstractConfig(STS_CONFIG_DEF, configs);
    customerRoleArn = config.getString(CUSTOMER_ROLE_ARN_CONFIG);
    customerRoleExternalId = config.getString(CUSTOMER_ROLE_EXTERNAL_ID_CONFIG);
    middlewareRoleArn = config.getString(MIDDLEWARE_ROLE_ARN_CONFIG);

    STSAssumeRoleSessionCredentialsProvider initialProvider = buildProvider(
        middlewareRoleArn, 
        "middlewareSession", 
        "", 
        null
    );

    // Use the credentials from the initial role to assume the subsequent role
    stsCredentialProvider = buildProvider(
        customerRoleArn, 
        "customerSession", 
        customerRoleExternalId, 
        initialProvider
    );
  }

  // Updated buildProvider to optionally accept an existing AwsCredentialsProvider
  private STSAssumeRoleSessionCredentialsProvider buildProvider(
      final String roleArn, 
      final String roleSessionName, 
      final String roleExternalId, 
      STSAssumeRoleSessionCredentialsProvider existingProvider) {

    STSAssumeRoleSessionCredentialsProvider credentialsProvider;
    // If an existing credentials provider is provided, use it for creating the STS client
    if (existingProvider != null) {
      AWSCredentials basicCredentials = existingProvider.getCredentials();
      credentialsProvider = new STSAssumeRoleSessionCredentialsProvider
          .Builder(roleArn, roleSessionName)
          .withStsClient(AWSSecurityTokenServiceClientBuilder
              .standard()
              .withCredentials(new AWSStaticCredentialsProvider(basicCredentials)).build()
          )
          .withExternalId(roleExternalId)
          .build();
    } else {
      if (accessKeyId == "" || accessSecret == "") {
        credentialsProvider = new STSAssumeRoleSessionCredentialsProvider
          .Builder(roleArn, roleSessionName)
          .withStsClient(AWSSecurityTokenServiceClientBuilder.defaultClient())
          .withExternalId(roleExternalId)
          .build();
      } else {
        BasicAWSCredentials basicCredentials = new BasicAWSCredentials(accessKeyId, accessSecret);
        credentialsProvider = new STSAssumeRoleSessionCredentialsProvider
          .Builder(roleArn, roleSessionName)
          .withStsClient(AWSSecurityTokenServiceClientBuilder
              .standard()
              .withCredentials(new AWSStaticCredentialsProvider(basicCredentials)).build()
          )
          .withExternalId(roleExternalId)
          .build();
      }
    }
    return credentialsProvider;
  }

  @Override
  public AWSCredentials getCredentials() {
    return stsCredentialProvider.getCredentials();
  }

  @Override
  public void refresh() {
    stsCredentialProvider.refresh();
  }
}
