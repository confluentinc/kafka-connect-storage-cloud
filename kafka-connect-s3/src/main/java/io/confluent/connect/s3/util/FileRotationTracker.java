/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.connect.s3.util;

import io.confluent.connect.storage.schema.SchemaIncompatibilityType;
import java.util.HashMap;
import java.util.Map;

public final class FileRotationTracker {

  private final Map<String, RotationMetrics> metrics = new HashMap<>();

  private static final class RotationMetrics {

    int rotationByFlushSize = 0;

    int rotationByRotationInterval = 0;

    int rotationByScheduledRotationInterval = 0;

    int rotationByDiffSchema = 0;

    int rotationByDiffName = 0;

    int rotationByDiffParams = 0;

    int rotationByDiffType = 0;

    int rotationByDiffVersion = 0;

    public void incrementRotationBySchemaChangeCount(
        SchemaIncompatibilityType schemaIncompatibilityType) {
      switch (schemaIncompatibilityType) {
        case DIFFERENT_NAME:
          rotationByDiffName++;
          break;
        case DIFFERENT_SCHEMA:
          rotationByDiffSchema++;
          break;
        case DIFFERENT_PARAMS:
          rotationByDiffParams++;
          break;
        case DIFFERENT_TYPE:
          rotationByDiffType++;
          break;
        case DIFFERENT_VERSION:
          rotationByDiffVersion++;
          break;
        default:
      }
    }

    public void incrementRotationByFlushSizeCount() {
      rotationByFlushSize++;
    }

    public void incrementRotationByRotationIntervalCount() {
      rotationByRotationInterval++;
    }

    public void incrementRotationByScheduledRotationIntervalCount() {
      rotationByScheduledRotationInterval++;
    }
  }

  public void incrementRotationBySchemaChangeCount(String outputPartition,
      SchemaIncompatibilityType schemaIncompatibilityType) {
    if (!metrics.containsKey(outputPartition)) {
      metrics.put(outputPartition, new RotationMetrics());
    }
    metrics.get(outputPartition).incrementRotationBySchemaChangeCount(schemaIncompatibilityType);
  }

  public void incrementRotationByFlushSizeCount(String outputPartition) {
    if (!metrics.containsKey(outputPartition)) {
      metrics.put(outputPartition, new RotationMetrics());
    }
    metrics.get(outputPartition).incrementRotationByFlushSizeCount();
  }

  public void incrementRotationByRotationIntervalCount(String outputPartition) {
    if (!metrics.containsKey(outputPartition)) {
      metrics.put(outputPartition, new RotationMetrics());
    }
    metrics.get(outputPartition).incrementRotationByRotationIntervalCount();
  }

  public void incrementRotationByScheduledRotationIntervalCount(String outputPartition) {
    if (!metrics.containsKey(outputPartition)) {
      metrics.put(outputPartition, new RotationMetrics());
    }
    metrics.get(outputPartition).incrementRotationByScheduledRotationIntervalCount();
  }

  public void clear() {
    metrics.clear();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, RotationMetrics> metrics : metrics.entrySet()) {
      RotationMetrics rotationMetrics = metrics.getValue();
      sb.append("OutputPartition: ");
      sb.append(metrics.getKey());
      sb.append(", RotationByInterval: ");
      sb.append(rotationMetrics.rotationByRotationInterval);
      sb.append(", RotationByScheduledInterval: ");
      sb.append(rotationMetrics.rotationByScheduledRotationInterval);
      sb.append(", RotationByFlushSize: ");
      sb.append(rotationMetrics.rotationByFlushSize);
      sb.append(", RotationByDiffName: ");
      sb.append(rotationMetrics.rotationByDiffName);
      sb.append(", RotationByDiffSchema: ");
      sb.append(rotationMetrics.rotationByDiffSchema);
      sb.append(", RotationByDiffType: ");
      sb.append(rotationMetrics.rotationByDiffType);
      sb.append(", RotationByDiffVersion: ");
      sb.append(rotationMetrics.rotationByDiffVersion);
      sb.append(", RotationByDiffParams: ");
      sb.append(rotationMetrics.rotationByDiffParams);
      sb.append("\n");
    }
    return sb.toString();
  }
}


