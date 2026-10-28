/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.s3;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigException;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public final class DummyAssertiveCredentialsProvider implements AwsCredentialsProvider,
    Configurable {

  public static final String ACCESS_KEY_NAME = "access.key";
  public static final String SECRET_KEY_NAME = "secret.key";
  public static final String CONFIGS_NUM_KEY_NAME = "configs.num";

  private AwsCredentials credentials;

  @Override
  public void configure(final Map<String, ?> configs) {

    final String accessKeyId = (String) configs.get(ACCESS_KEY_NAME);
    final String secretKey = (String) configs.get(SECRET_KEY_NAME);
    final Integer configsNum = Integer.valueOf((String) configs.get(CONFIGS_NUM_KEY_NAME));

    validateConfigs(configs);

    assertEquals(configsNum.intValue(), configs.size());

    credentials = AwsBasicCredentials.create(accessKeyId, secretKey);
  }

  private void validateConfigs(Map<String, ?> configs) {

    if (!configs.containsKey(ACCESS_KEY_NAME) ||
        !configs.containsKey(SECRET_KEY_NAME)) {
      throw new ConfigException(String.format("%s and %s are mandatory configuration properties",
          ACCESS_KEY_NAME, SECRET_KEY_NAME
      ));
    }
  }

  @Override
  public AwsCredentials resolveCredentials() {
    return credentials;
  }

  public static DummyAssertiveCredentialsProvider create() {
    return new DummyAssertiveCredentialsProvider();
  }
}
