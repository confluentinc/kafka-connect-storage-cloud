/*
 * Copyright 2024 Confluent Inc.
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
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

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
      );

  private String customerRoleArn;
  private String customerRoleExternalId;
  private String middlewareRoleArn;
  private STSAssumeRoleSessionCredentialsProvider stsCredentialProvider;
  private STSAssumeRoleSessionCredentialsProvider initialProvider;

  // Method to initiate role chaining
  public void configure(Map<String, ?> configs) {
    // Assume the initial role
    AbstractConfig config = new AbstractConfig(STS_CONFIG_DEF, configs);
    customerRoleArn = config.getString(CUSTOMER_ROLE_ARN_CONFIG);
    customerRoleExternalId = config.getString(CUSTOMER_ROLE_EXTERNAL_ID_CONFIG);
    middlewareRoleArn = config.getString(MIDDLEWARE_ROLE_ARN_CONFIG);

    initialProvider = buildProvider(
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
      credentialsProvider = new STSAssumeRoleSessionCredentialsProvider
        .Builder(roleArn, roleSessionName)
        .withStsClient(AWSSecurityTokenServiceClientBuilder.defaultClient())
        .build();
    }
    return credentialsProvider;
  }

  @Override
  public AWSCredentials getCredentials() {
    return stsCredentialProvider.getCredentials();
  }

  @Override
  public void refresh() {
    if (initialProvider != null) {
      initialProvider.refresh();
      stsCredentialProvider = buildProvider(
          customerRoleArn, 
          "customerSession", 
          customerRoleExternalId, 
          initialProvider
      );
    }
  }

  // Visible for test
  public STSAssumeRoleSessionCredentialsProvider getProvider() {
    return stsCredentialProvider;
  }
}
