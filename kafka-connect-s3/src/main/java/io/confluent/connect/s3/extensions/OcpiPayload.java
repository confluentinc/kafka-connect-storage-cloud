package io.confluent.connect.s3.extensions;

public class OcpiPayload {
    private String timestamp;
    private String id;

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
