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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.util.Properties;

public abstract class AbstractFileEventConfig {
  public static <T extends AbstractFileEventConfig> T fromJsonString(
      String jsonContent, Class<T> clazz) {
    try {
      if (jsonContent == null) {
        return clazz.newInstance();
      }
      ObjectMapper instanceMapper = JsonMapper.builder()
          .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)
          .propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
          .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
          .build();
      T value = instanceMapper.readValue(jsonContent, clazz);
      value.validateFields();
      return  value;
    } catch (IllegalAccessException | InstantiationException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected abstract void validateFields() ;

  public abstract Properties toProps();

  public abstract String toJson();
}
