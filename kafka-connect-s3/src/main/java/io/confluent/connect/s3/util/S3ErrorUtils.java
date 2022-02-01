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

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.retry.PredefinedRetryPolicies;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;

import java.io.IOException;

/**
 * Utilities related to basic S3 error/exception analysis.
 */
public class S3ErrorUtils {

  /**
   * Return whether the given exception is a "retryable" exception.
   * @param exception The exception to analyze
   * @return true if the exception is retryable
   */
  private static boolean isRetriableException(Throwable exception) {
    if (exception == null) {
      return false;
    }

    // IOException, in many places, is passed the AWS exception
    // when it is thrown. We search here to check that exception
    // for the IOException case.  Otherwise, the IOException
    // is considered not retriable.
    // Exception: An IOException embedded within an `AmazonClientException`
    // should be passed via the `AmazonClientException` object
    // as its parent (as the SDK does), in which case, shouldRetry()
    // will often find it retriable.
    for (Throwable cause : ExceptionUtils.getThrowableList(exception)) {
      if (cause instanceof AmazonClientException) {
        // The AWS SDK maintains a check for what it considers to be
        // retriable exceptions.
        return PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION.shouldRetry(
                AmazonWebServiceRequest.NOOP,
                (AmazonClientException) cause,
                Integer.MAX_VALUE
        );
      }

      if (!(cause instanceof IOException)) {
        return false;
      }
    }

    return false;
  }

  /**
   * Throw a `ConnectException` exception which may or may not
   * be of (or derived from) type `RetriableException`.
   * @param t The exception to analyze
   * @throws ConnectException exception of (or derived from) `ConnectException` which
   *         may also be of type `RetriableException`.
   */
  public static void throwConnectException(Throwable t) throws ConnectException {
    // If this is already a ConnectException of some sort, just rethrow it
    if (t instanceof ConnectException) {
      throw (ConnectException) t;
    }
    if (isRetriableException(t)) {
      throw new RetriableException(t.getMessage(), t);
    }
    throw new ConnectException(t.getMessage(), t);
  }
}
