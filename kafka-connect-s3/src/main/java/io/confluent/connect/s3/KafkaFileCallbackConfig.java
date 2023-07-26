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


  private String schemaRegistryUrl;
  private String keySerializer = "org.apache.kafka.common.serialization.StringSerializer";
  private String valueSerializer = "io.confluent.kafka.serializers.KafkaAvroSerializer";

  public KafkaFileCallbackConfig() {
  }

  public KafkaFileCallbackConfig(String topicName, String topicUser, String topicPassword,
                                 String bootstrapServers, String securityProtocols, String schemaRegistryUrl) {
    this.topicName = topicName;
    this.topicUser = topicUser;
    this.topicPassword = topicPassword;
    this.bootstrapServers = bootstrapServers;
    this.securityProtocols = securityProtocols;
    this.schemaRegistryUrl = schemaRegistryUrl;
  }



  public String getTopicName() {
    return topicName;
  }

  public String getSchemaRegistryUrl() {
    return schemaRegistryUrl;
  }
  public String getTopicUser() {
    return topicUser;
  }

  public String getTopicPassword() {
    return topicPassword;
  }

  public String getBootstrapServers() {
    return bootstrapServers;
  }

  public String getSecurityProtocols() {
    return securityProtocols;
  }

  public String getKeySerializer() {
    return keySerializer;
  }

  public String toJson() {
    final StringBuffer sb = new StringBuffer("{");
    sb.append("\"topic_name\": \"").append(topicName).append('"');
    sb.append(", \"topic_user\": \"").append(topicUser).append('"');
    sb.append(", \"topic_password\": \"").append(topicPassword).append('"');
    sb.append(", \"bootstrap_servers\": \"").append(bootstrapServers).append('"');
    sb.append(", \"security_protocols\": \"").append(securityProtocols).append('"');
    sb.append(", \"schema_registry_url\": \"").append(schemaRegistryUrl).append('"');
    sb.append('}');
    return sb.toString();
  }

  @Override
  public Properties toProps() {
    Properties prop = new Properties();
    prop.setProperty("bootstrap.servers", bootstrapServers);
    prop.setProperty("topic.name", topicName);
    prop.setProperty("key.serializer", keySerializer);
    prop.setProperty("value.serializer", valueSerializer);
    prop.setProperty("schema.registry.url", schemaRegistryUrl);
    return prop;
  }
}
