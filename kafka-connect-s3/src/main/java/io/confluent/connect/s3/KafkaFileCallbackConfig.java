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

import java.util.Properties;

public class KafkaFileCallbackConfig extends AbstractFileCallbackConfig {

  private String topicName;
  private String topicUser;
  private String topicPassword;
  private String bootstrapServers;
  private String securityProtocols;

  public KafkaFileCallbackConfig() {
  }

  public KafkaFileCallbackConfig(String topicName, String topicUser, String topicPassword,
                                 String bootstrapServers, String securityProtocols) {
    this.topicName = topicName;
    this.topicUser = topicUser;
    this.topicPassword = topicPassword;
    this.bootstrapServers = bootstrapServers;
    this.securityProtocols = securityProtocols;
  }


  @Override
  public Properties toProps() {
    Properties prop = new Properties();
    return prop;
  }

  public String getTopicName() {
    return topicName;
  }
}
