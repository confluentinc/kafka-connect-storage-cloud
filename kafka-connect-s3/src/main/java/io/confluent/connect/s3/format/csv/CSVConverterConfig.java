package io.confluent.connect.s3.format.csv;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.storage.ConverterConfig;

import java.util.Map;

public class CSVConverterConfig extends ConverterConfig {

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

    public static ConfigDef configDef() {
        return CONFIG;
    }

    public CSVConverterConfig(Map<String, ?> props) {
        super(CONFIG, props);
    }

}
