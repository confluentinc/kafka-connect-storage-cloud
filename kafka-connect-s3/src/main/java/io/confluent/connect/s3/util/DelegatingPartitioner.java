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

import io.confluent.connect.storage.partitioner.Partitioner;

/**
 * Abstract base class for partitioners that wrap/delegate to another partitioner.
 * Provides common infrastructure for partitioner chains, allowing recursive unwrapping
 * to find specific partitioner types (e.g., TimeBasedPartitioner for timestamp extraction).
 *
 * <p>Subclasses are responsible for implementing all Partitioner methods and typically
 * delegate these calls to the wrapped partitioner.</p>
 *
 * @param <T> the type of partition fields
 */
public abstract class DelegatingPartitioner<T> implements Partitioner<T> {

  protected final Partitioner<T> delegatePartitioner;

  /**
   * Constructs a delegating partitioner that wraps another partitioner.
   *
   * @param delegatePartitioner the partitioner to wrap
   */
  public DelegatingPartitioner(Partitioner<T> delegatePartitioner) {
    this.delegatePartitioner = delegatePartitioner;
  }

  /**
   * Returns the delegate partitioner that this partitioner wraps.
   * This allows callers to recursively unwrap partitioner chains to find specific
   * partitioner types.
   *
   * @return the delegate partitioner
   */
  public Partitioner<T> getDelegatePartitioner() {
    return delegatePartitioner;
  }
}
