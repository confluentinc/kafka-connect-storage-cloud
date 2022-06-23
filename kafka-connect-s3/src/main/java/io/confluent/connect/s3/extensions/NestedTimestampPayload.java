/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.connect.s3.extensions;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class NestedTimestampPayload implements UdxPayload {
  private String timestamp;
  private String id;

  @SuppressWarnings("unchecked")
  @JsonProperty("timestamp")
  private void unpackNested(Map<String, Object> timestamp) {
    // For timestamps of form: timestamp: { type: 'Property', value: '2021-05-07T06:06:30Z' },
    this.timestamp = (String) timestamp.get("value");
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

  public String toString() {
    return "OcpiPayload [ entityId: " + getId() + ", timestamp: " + getTimestamp() + " ]";
  }
}
