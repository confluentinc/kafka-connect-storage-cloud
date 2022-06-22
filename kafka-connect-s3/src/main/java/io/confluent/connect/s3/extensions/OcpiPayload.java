package io.confluent.connect.s3.extensions;

public interface OcpiPayload {
    String getTimestamp();

    void setTimestamp(String timestamp);

    String getId();

    void setId(String id);

    String toString();
}
