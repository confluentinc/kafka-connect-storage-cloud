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

package io.confluent.connect.s3.util;

public class OutputPartitionInfo {

  private int timeRotationCount;

  private int schemaRotationCount;

  private int sizeRotationCount;

  private long totalWriteCount;

  private long currentWriteCount;

  public OutputPartitionInfo(int timeRotationCount, int schemaRotationCount, int sizeRotationCount,
      long totalWriteCount, long currentWriteCount) {
    this.timeRotationCount = timeRotationCount;
    this.schemaRotationCount = schemaRotationCount;
    this.sizeRotationCount = sizeRotationCount;
    this.totalWriteCount = totalWriteCount;
    this.currentWriteCount = currentWriteCount;
  }

  public int getTimeRotationCount() {
    return timeRotationCount;
  }

  public void setTimeRotationCount(int timeRotationCount) {
    this.timeRotationCount = timeRotationCount;
  }

  public int getSchemaRotationCount() {
    return schemaRotationCount;
  }

  public void setSchemaRotationCount(int schemaRotationCount) {
    this.schemaRotationCount = schemaRotationCount;
  }

  public int getSizeRotationCount() {
    return sizeRotationCount;
  }

  public void setSizeRotationCount(int sizeRotationCount) {
    this.sizeRotationCount = sizeRotationCount;
  }

  public long getTotalWriteCount() {
    return totalWriteCount;
  }

  public void setTotalWriteCount(int totalWriteCount) {
    this.totalWriteCount = totalWriteCount;
  }

  public long getCurrentWriteCount() {
    return currentWriteCount;
  }

  public void setCurrentWriteCount(long currentWriteCount) {
    this.currentWriteCount = currentWriteCount;
  }
}
