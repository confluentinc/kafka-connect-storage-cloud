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

package io.confluent.connect.s3.format.csv;

import io.confluent.connect.s3.S3SinkConnectorConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.HashMap;
import java.util.Map;

public class CsvConverterConfig extends AbstractConfig {

  static CsvConverterConfig csvConverterConfig(S3SinkConnectorConfig config) {
    Map<String, Object> props = new HashMap();
    props.put("csv.field.sep", config.get("csv.field.sep"));
    props.put("csv.compat", config.get("csv.compat"));
    props.put("csv.line.sep", config.get("csv.line.sep"));
    props.put("csv.fields.list", config.get("csv.fields.list"));
    return new CsvConverterConfig(props);
  }

  public static ConfigDef addConfig(ConfigDef configDef) {
    configDef.define("csv.field.sep",
            ConfigDef.Type.STRING,
            ",",
            ConfigDef.Importance.HIGH,
            "CSV field separator character(s)"
    );
    configDef.define("csv.compat",
            ConfigDef.Type.BOOLEAN,
            false,
            ConfigDef.Importance.MEDIUM,
            "CSV compatibility mode"
    );
    configDef.define("csv.line.sep",
            ConfigDef.Type.STRING,
            "\n",
            ConfigDef.Importance.HIGH,
            "CSV line separator character(s)"
    );
    configDef.define("csv.fields.list",
            ConfigDef.Type.LIST,
            "",
            ConfigDef.Importance.MEDIUM,
            "Defines header and order of fields. If the field is not mentioned "
                    + "it will be skipped."
    );
    return configDef;
  }

  private static ConfigDef baseConfigDef() {
    ConfigDef configDef = new ConfigDef();
    addConfig(configDef);
    return configDef;
  }

  public CsvConverterConfig(Map<String, ?> props) {
    super(baseConfigDef(), props);
  }

  public static ConfigDef configDef() {
    return baseConfigDef();
  }

}
