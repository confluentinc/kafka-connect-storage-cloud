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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.storage.ConverterConfig;

import java.util.Map;

public class CsvConverterConfig extends ConverterConfig {

  private static final ConfigDef CONFIG;

  static {
    CONFIG = ConverterConfig.newConfigDef();
    CONFIG.define("csv.field.sep",
            ConfigDef.Type.STRING,
            ",",
            ConfigDef.Importance.HIGH,
            "CSV field separator character(s)"
    );
    CONFIG.define("csv.line.sep",
            ConfigDef.Type.STRING,
            "\n",
            ConfigDef.Importance.HIGH,
            "CSV line separator character(s)"
    );
  }

  public CsvConverterConfig(Map<String, ?> props) {
    super(CONFIG, props);
  }

  public static ConfigDef configDef() {
    return CONFIG;
  }

}
