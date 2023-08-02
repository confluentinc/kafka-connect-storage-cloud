package io.confluent.connect.s3;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;

public class S3SinkConnectorV2 extends S3SinkConnector {
    @Override
    public Class<? extends Task> taskClass() {
        return S3SinkTaskV2.class;
    }
    public S3SinkConnectorV2() {
        // no-arg constructor required by Connect framework.
    }

    @Override
    public ConfigDef config() {
        ConfigDef configDef = S3SinkConnectorConfig.getConfig();
        configDef.define("transform.threads.max",
                ConfigDef.Type.INT,
                1,
                ConfigDef.Importance.HIGH,
                "Max number of threads to use for converting batch of records in parallel.",
                "parallel_processing",
                1,
                ConfigDef.Width.LONG,
                "Max number of threads to use for converting batch of records in parallel."
        );
        return configDef;
    }
}
