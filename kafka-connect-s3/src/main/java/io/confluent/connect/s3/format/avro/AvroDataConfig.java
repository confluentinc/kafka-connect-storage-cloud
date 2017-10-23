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
