package io.confluent.connect.s3.extensions;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.common.protocol.types.Field;

import java.util.Map;
public class OcpiLocationsPayload implements OcpiPayload {
    private String timestamp;
    private String id;

    @SuppressWarnings("unchecked")
    @JsonProperty("timestamp")
    private void unpackNested(Map<String,Object> timestamp) {
        // For timestamps of form: timestamp: { type: 'Property', value: '2021-05-07T06:06:30Z' },
        this.timestamp = (String)timestamp.get("value");
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String toString(){
        return "OcpiPayload [ entityId: "+ getId() +", Zulu timestamp: "+ getTimestamp() + " ]";
    }
}
