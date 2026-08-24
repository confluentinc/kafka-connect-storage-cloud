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

package io.confluent.connect.s3;

import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class S3SinkConnectorTest {

  @Test
  public void testVersion() {
    String version = new S3SinkConnector().version();
    assertNotNull(version);
    assertFalse(version.isEmpty());
  }

  @Test
  public void connectorType() {
    Connector connector = new S3SinkConnector();
    assertTrue(SinkConnector.class.isAssignableFrom(connector.getClass()));
  }

  @Test
  public void testTaskClassIsDefaultBeforeStart() {
    // config is null until start(); the null-guard must fall through to S3SinkTask
    Class<? extends Task> taskClass = new S3SinkConnector().taskClass();
    assertEquals(S3SinkTask.class, taskClass);
  }

  @Test
  public void testTaskClassIsDefaultInGenericMode() {
    S3SinkConnectorConfig config = mock(S3SinkConnectorConfig.class);
    when(config.isBackupMode()).thenReturn(false);

    Class<? extends Task> taskClass = new S3SinkConnector(config).taskClass();

    assertEquals(S3SinkTask.class, taskClass);
  }

  @Test
  public void testTaskClassIsBackupTaskInBackupMode() {
    S3SinkConnectorConfig config = mock(S3SinkConnectorConfig.class);
    when(config.isBackupMode()).thenReturn(true);

    Class<? extends Task> taskClass = new S3SinkConnector(config).taskClass();

    assertEquals(BackupS3SinkTask.class, taskClass);
  }
}

