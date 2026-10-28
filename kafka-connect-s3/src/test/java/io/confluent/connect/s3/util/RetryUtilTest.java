/*
 * Copyright 2026 Confluent Inc.
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

import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

public class RetryUtilTest {

  @Test
  public void testRetriesUntilSuccess() {
    AtomicInteger calls = new AtomicInteger();
    RetryUtil.retry(() -> {
      if (calls.incrementAndGet() < 3) {
        throw new IllegalStateException("transient");
      }
    }, e -> true, "test", 4, 1, 2.0, 10, 0.2);
    assertEquals(3, calls.get());
  }

  @Test
  public void testThrowsLastFailureWhenAttemptsExhausted() {
    AtomicInteger calls = new AtomicInteger();
    IllegalStateException failure = new IllegalStateException("transient");
    RuntimeException thrown = assertThrows(RuntimeException.class, () -> RetryUtil.retry(() -> {
      calls.incrementAndGet();
      throw failure;
    }, e -> true, "test", 3, 1, 2.0, 10, 0.2));
    assertSame(failure, thrown);
    assertEquals(3, calls.get());
  }

  @Test
  public void testDoesNotRetryNonRetriableFailure() {
    AtomicInteger calls = new AtomicInteger();
    assertThrows(IllegalArgumentException.class, () -> RetryUtil.retry(() -> {
      calls.incrementAndGet();
      throw new IllegalArgumentException("permanent");
    }, e -> false, "test", 3, 1, 2.0, 10, 0.2));
    assertEquals(1, calls.get());
  }
}
