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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.BufferOverflowException;

/**
 * A elastic byte buffer with a logic size as max size.
 * The formula to expand: initCapacity * 2 ^ (incrementFactor * N)
 */
public class ElasticByteBuffer implements ByteBuf {

  private static final Logger log = LoggerFactory.getLogger(ElasticByteBuffer.class);

  public static final int INCREMENT_FACTOR = 1;

  /* logical capacity */
  private int capacity;

  /* initial physical capacity  */
  private int initPhysicalCap;

  /* the next position to write */
  private int position;

  /* physical buf */
  private byte[] buf;

  public ElasticByteBuffer(int capacity, int initPhysicalCap) {
    if (capacity <= 0) {
      throw new IllegalArgumentException("capacity must greater than zero");
    }

    if (initPhysicalCap <= 0) {
      log.debug("initial physical capacity must be greater than zero");
      throw new IllegalArgumentException("initial physical capacity must be greater than zero");
    }

    this.capacity = capacity;
    this.initPhysicalCap = initPhysicalCap;

    initialize();
  }

  private void initialize() {
    this.position = 0;
    int initCapacity = Math.min(this.capacity, this.initPhysicalCap);
    this.buf = new byte[initCapacity];
  }

  private void expand() {
    int currSize = this.buf.length;
    int calNewSize = currSize << INCREMENT_FACTOR;

    int newSize = 0;
    if (calNewSize < currSize) {
      // down overflow
      newSize = this.capacity;
    } else {
      newSize = Math.min(this.capacity, calNewSize);
    }

    byte[] currBuf = this.buf;
    this.buf = new byte[newSize];
    System.arraycopy(currBuf, 0, this.buf, 0, currSize);
  }

  public void put(byte b) {
    if (!hasRemaining()) {
      throw new BufferOverflowException();
    }

    if (physicalRemaining() <= 0) {
      // expand physical buf
      expand();
    }

    this.buf[this.position] = b;
    this.position++;
  }

  public void put(byte[] src, int offset, int length) {

    checkBounds(offset, length, src.length);

    if (remaining() < length) {
      throw new BufferOverflowException();
    }

    int remainingOffset = offset;
    int remainingLen = length;
    while (true) {
      if (physicalRemaining() <= 0) {
        // expand physical buf
        expand();
      }

      if (physicalRemaining() >= remainingLen) {
        System.arraycopy(src, remainingOffset, this.buf, this.position, remainingLen);
        this.position += remainingLen;
        break;
      } else {
        int physicalRemaining = physicalRemaining();
        System.arraycopy(src, remainingOffset, this.buf, this.position, physicalRemaining);
        this.position += physicalRemaining;
        remainingOffset += physicalRemaining;
        remainingLen -= physicalRemaining;
      }
    }
  }

  static void checkBounds(int off, int len, int size) { // package-private
    if ((off | len | (off + len) | (size - (off + len))) < 0) {
      throw new IndexOutOfBoundsException();
    }
  }

  public int physicalRemaining() {
    return this.buf.length - this.position;
  }

  public boolean hasRemaining() {
    return capacity > position;
  }

  public int remaining() {
    return capacity - position;
  }

  public int position() {
    return this.position;
  }

  public void clear() {
    if (this.buf.length <= this.initPhysicalCap) {
      // has not ever expanded, just reset position
      this.position = 0;
    } else {
      initialize();
    }
  }

  public final byte[] array() {
    return this.buf;
  }

}
