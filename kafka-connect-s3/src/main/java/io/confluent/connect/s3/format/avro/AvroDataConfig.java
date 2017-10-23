/*
 * Copyright 2017 Confluent Inc.
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

package io.confluent.connect.s3.format.avro;

import io.confluent.connect.storage.common.ComposableConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

import static io.confluent.connect.avro.AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG;
import static io.confluent.connect.avro.AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_DEFAULT;
import static io.confluent.connect.avro.AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_DOC;
import static io.confluent.connect.avro.AvroDataConfig.CONNECT_META_DATA_CONFIG;
import static io.confluent.connect.avro.AvroDataConfig.CONNECT_META_DATA_DEFAULT;
import static io.confluent.connect.avro.AvroDataConfig.CONNECT_META_DATA_DOC;
import static io.confluent.connect.avro.AvroDataConfig.SCHEMAS_CACHE_SIZE_CONFIG;
import static io.confluent.connect.avro.AvroDataConfig.SCHEMAS_CACHE_SIZE_DEFAULT;
import static io.confluent.connect.avro.AvroDataConfig.SCHEMAS_CACHE_SIZE_DOC;

/**
 * Adapts {@link io.confluent.connect.avro.AvroDataConfig} which uses {@link io.confluent.common.config.AbstractConfig} which is
 * incompatible with {@link org.apache.kafka.common.config.AbstractConfig}.
 */
public class AvroDataConfig extends AbstractConfig implements ComposableConfig {

  public static ConfigDef baseConfigDef() {
    return new ConfigDef()
        .define(
            ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG,
            ConfigDef.Type.BOOLEAN,
            ENHANCED_AVRO_SCHEMA_SUPPORT_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            ENHANCED_AVRO_SCHEMA_SUPPORT_DOC
        ).define(
            CONNECT_META_DATA_CONFIG,
            ConfigDef.Type.BOOLEAN,
            CONNECT_META_DATA_DEFAULT,
            ConfigDef.Importance.LOW,
            CONNECT_META_DATA_DOC
        ).define(
            SCHEMAS_CACHE_SIZE_CONFIG,
            ConfigDef.Type.INT,
            SCHEMAS_CACHE_SIZE_DEFAULT,
            ConfigDef.Importance.LOW,
            SCHEMAS_CACHE_SIZE_DOC);
  }


  public AvroDataConfig(Map<?, ?> props) {
    super(baseConfigDef(), props);
  }

  @Override
  public Object get(String s) {
    return super.get(s);
  }
}
