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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;

import java.io.IOException;
import java.util.Properties;

public abstract class AbstractFileCallbackConfig {
  public static <T extends AbstractFileCallbackConfig> T fromJsonString(String jsonContent,
                                                                        Class<T> clazz) {
    try {
      if (jsonContent == null) {
        return clazz.newInstance();
      }
      ObjectMapper instanceMapper = new ObjectMapper();
      instanceMapper.setPropertyNamingStrategy(
              PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
      instanceMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
      instanceMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
      instanceMapper.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS);
      return instanceMapper.readValue(jsonContent, clazz);
    } catch (IllegalAccessException | InstantiationException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  public abstract Properties toProps();
}
