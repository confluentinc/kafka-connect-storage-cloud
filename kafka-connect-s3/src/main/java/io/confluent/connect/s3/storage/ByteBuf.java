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

package io.confluent.connect.s3.storage;

/**
 * A interface for S3OutputStream to write s3-part.
 */
public interface ByteBuf {

  void put(byte b);

  void put(byte[] src, int offset, int length);

  boolean hasRemaining();

  int remaining();

  int position();

  void clear();

  byte[] array();
}
