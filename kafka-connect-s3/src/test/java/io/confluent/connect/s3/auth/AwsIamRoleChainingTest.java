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

package io.confluent.connect.s3.auth;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import io.confluent.connect.s3.S3SinkConnectorTestBase;
import io.confluent.connect.s3.auth.iamassume.AwsIamAssumeRoleChaining;
import io.confluent.connect.s3.S3SinkConnectorConfig;

import org.junit.After;
import org.junit.Before;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.Map;

class AwsIamAssumeRoleChainingTest extends S3SinkConnectorTestBase {

  protected Map<String, String> configs = new HashMap<>();

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    final String CUSTOMER_ROLE_ARN = "arn:aws:iam::012345678901:role/my-restricted-role";
    final String CUSTOMER_EXTERNAL_ID = "exampleExternaID";
    final String MIDDLEWARE_ROLE_ARN = "arn:aws:iam::109876543210:role/my-example-role";

    configs.put(
      S3SinkConnectorConfig.AUTH_METHOD,
      "IAM Assume Role"
    );
    configs.put(
        S3SinkConnectorConfig.CUSTOMER_ROLE_ARN_CONFIG,
        CUSTOMER_ROLE_ARN
    );
    configs.put(
        S3SinkConnectorConfig.CUSTOMER_ROLE_EXTERNAL_ID_CONFIG,
        CUSTOMER_EXTERNAL_ID
    );
    configs.put(
        S3SinkConnectorConfig.MIDDLEWARE_ROLE_ARN_CONFIG,
        MIDDLEWARE_ROLE_ARN
    );
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    configs.clear();
  }

  @Test
  public void testConfigure() {
    AwsIamAssumeRoleChaining roleChaining = mock(AwsIamAssumeRoleChaining.class);
    roleChaining.configure(configs);

    STSAssumeRoleSessionCredentialsProvider mockFinalProvider = Mockito.mock(STSAssumeRoleSessionCredentialsProvider.class);
    Mockito.doReturn(mockFinalProvider).when(roleChaining).getProvider();

    AWSCredentials expectedCredentials = new BasicAWSCredentials("accessKey", "secretKey");
    Mockito.when(mockFinalProvider.getCredentials()).thenReturn((AWSSessionCredentials) expectedCredentials);

    AWSCredentials actualCredentials = (AWSCredentials) roleChaining.getCredentials();

    assertEquals(expectedCredentials, actualCredentials);
  }
}
