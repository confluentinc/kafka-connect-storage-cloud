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

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class RetryUtil {
  private static final Logger log = LoggerFactory.getLogger(RetryUtil.class);

  /**
   * @param runnable
   *     An executable piece of code which will be retried in case an expected exception occurred.
   * @param exceptionClass
   *     The function will be retried only if it throws an instance of exceptionClass.
   * @param totalNumIterations
   *     This is the overall execution count e.g. if 3 then it'd be retried a max of twice.
   * @param delayInMillis
   *     Delay between 2 retries, each time it'd be doubled.
   */
  public static void exponentialBackoffRetry(final Runnable runnable,
                         final Class<? extends Exception> exceptionClass,
                         final int totalNumIterations,
                         final long delayInMillis) throws ConnectException {
    long expDelayInMillis = delayInMillis;
    for (int i = 1; i <= totalNumIterations; i++) {
      try {
        runnable.run();
        break;
      } catch (Exception e) {
        if (e.getClass().equals(exceptionClass)) {
          log.warn("Attempt {} of {} failed.", i, totalNumIterations, e);
          if (i == totalNumIterations) {
            wrapAndThrowAsConnectException(e);
          } else {
            log.warn("Awaiting {} milliseconds before retrying.", expDelayInMillis);
            await(expDelayInMillis);
            expDelayInMillis <<= 1;
          }
        } else {
          wrapAndThrowAsConnectException(e);
        }
      }
    }
  }

  private static void wrapAndThrowAsConnectException(Exception e) throws ConnectException {
    if (e instanceof ConnectException) {
      throw (ConnectException) e;
    }
    throw new ConnectException(e);
  }

  private static void await(long millis) {
    try {
      TimeUnit.MILLISECONDS.sleep(millis);
    } catch (InterruptedException e) {
      // Restore the interrupted status
      Thread.currentThread().interrupt();
    }
  }
}
