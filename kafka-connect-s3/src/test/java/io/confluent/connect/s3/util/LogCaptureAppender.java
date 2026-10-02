/*
 * Copyright 2024 Confluent Inc.
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

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A minimal log4j appender that captures rendered log messages for a set of loggers. Tests use it
 * to assert that sensitive values (record field values, credentials, S3 object keys) do not reach
 * the logs. Test-only; not for production use.
 */
public class LogCaptureAppender extends AppenderSkeleton implements AutoCloseable {

  private final List<LoggingEvent> events = new ArrayList<>();
  private final Map<Logger, Level> previousLevels = new HashMap<>();

  /**
   * Attach a fresh appender to the given loggers. The appender threshold is INFO, so a message that
   * has been demoted to DEBUG is verifiably absent from the capture even if the logger itself still
   * enables DEBUG. Each logger is also raised to at least INFO so INFO events are dispatched; levels
   * are restored on {@link #close()}.
   */
  public static LogCaptureAppender attach(Class<?>... loggerClasses) {
    LogCaptureAppender appender = new LogCaptureAppender();
    appender.setThreshold(Level.INFO);
    for (Class<?> loggerClass : loggerClasses) {
      Logger logger = Logger.getLogger(loggerClass);
      appender.previousLevels.put(logger, logger.getLevel());
      logger.setLevel(Level.INFO);
      logger.addAppender(appender);
    }
    return appender;
  }

  public synchronized List<String> messages() {
    List<String> out = new ArrayList<>();
    for (LoggingEvent event : events) {
      out.add(String.valueOf(event.getRenderedMessage()));
    }
    return out;
  }

  public synchronized boolean anyMessageContains(String substring) {
    for (LoggingEvent event : events) {
      if (String.valueOf(event.getRenderedMessage()).contains(substring)) {
        return true;
      }
    }
    return false;
  }

  @Override
  protected synchronized void append(LoggingEvent event) {
    events.add(event);
  }

  @Override
  public void close() {
    for (Map.Entry<Logger, Level> entry : previousLevels.entrySet()) {
      entry.getKey().removeAppender(this);
      entry.getKey().setLevel(entry.getValue());
    }
    previousLevels.clear();
  }

  @Override
  public boolean requiresLayout() {
    return false;
  }
}
