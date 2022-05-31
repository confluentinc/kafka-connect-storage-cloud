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

import java.nio.ByteBuffer;

/**
 * A simple byte buf
 */
public class SimpleByteBuf implements ByteBuf {

  private ByteBuffer buffer;

  public SimpleByteBuf(int capacity) {
    this.buffer = ByteBuffer.allocate(capacity);
  }

  @Override
  public void put(byte b) {
    this.buffer.put(b);
  }

  @Override
  public void put(byte[] src, int offset, int length) {
    this.buffer.put(src, offset, length);
  }

  @Override
  public boolean hasRemaining() {
    return this.buffer.hasRemaining();
  }

  @Override
  public int remaining() {
    return this.buffer.remaining();
  }

  @Override
  public int position() {
    return this.buffer.position();
  }

  @Override
  public void clear() {
    this.buffer.clear();
  }

  @Override
  public byte[] array() {
    return this.buffer.array();
  }
}
