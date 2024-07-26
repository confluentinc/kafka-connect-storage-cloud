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

package io.confluent.connect.s3.file;

import java.util.Map;
import java.util.Properties;

public class KafkaFileEventConfig extends AbstractFileEventConfig {

  Map<String, Object> custom;
  private static final String KEY_SERIALIZER =
      "org.apache.kafka.common.serialization.StringSerializer";
  private static final String VALUE_SERIALIZER =
      "io.confluent.kafka.serializers.KafkaAvroSerializer";

  private String topicName;
  private String databaseName;
  private String tableName;
  private String bootstrapServers;
  private String schemaRegistryUrl;

  /** empty constructor for jackson */
  public KafkaFileEventConfig() {}

  public KafkaFileEventConfig(
      String topicName,
      String databaseName,
      String tableName,
      String bootstrapServers,
      String schemaRegistryUrl,
      Map<String, Object> custom) {
    this.topicName = topicName;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.bootstrapServers = bootstrapServers;
    this.schemaRegistryUrl = schemaRegistryUrl;
    this.custom = custom;
  }

  @Override
  protected void validateFields() {
    if (topicName == null || bootstrapServers == null || schemaRegistryUrl == null) {
      throw new RuntimeException(
          "topic_name, boostrap_servers and schema_registry_url shall be defined");
    }
  }

  @Override
  public String toJson() {
    final StringBuffer sb = new StringBuffer("{");
    sb.append("\"topic_name\": \"").append(topicName).append('"');
    if(databaseName != null)
      sb.append("\"database_name\": \"").append(databaseName).append('"');
    if(tableName != null)
      sb.append("\"table_name\": \"").append(tableName).append('"');
    sb.append(", \"bootstrap_servers\": \"").append(bootstrapServers).append('"');
    sb.append(", \"schema_registry_url\": \"").append(schemaRegistryUrl).append('"');
    sb.append(", \"custom\": {");
    String customIncrement = "";
    for (Map.Entry<String, Object> custom : custom.entrySet()) {
      sb.append(
          String.format(
              "%s \"%s\": \"%s\"", customIncrement, custom.getKey(), custom.getValue().toString()));
      customIncrement = ",";
    }
    sb.append("}}");
    return sb.toString();
  }

  @Override
  public Properties toProps() {
    Properties prop = new Properties();
    prop.setProperty("key.serializer", KEY_SERIALIZER);
    prop.setProperty("value.serializer", VALUE_SERIALIZER);
    // mandatory
    prop.setProperty("bootstrap.servers", bootstrapServers);
    prop.setProperty("topic.name", topicName);
    prop.setProperty("schema.registry.url", schemaRegistryUrl);
    // custom
    for (Map.Entry<String, Object> custom : custom.entrySet()) {
      prop.setProperty(custom.getKey(), custom.getValue().toString());
    }
    return prop;
  }

  public String getTopicName() {
    return topicName;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getSchemaRegistryUrl() {
    return schemaRegistryUrl;
  }

  public String getBootstrapServers() {
    return bootstrapServers;
  }

  public Map<String, Object> getCustom() {
    return custom;
  }
}
