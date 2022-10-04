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

package io.confluent.connect.s3.hive;

import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.hive.HiveConfig;
import io.confluent.connect.storage.hive.HiveFactory;
import io.confluent.connect.storage.hive.HiveMetaStore;
import io.confluent.connect.storage.hive.HiveUtil;
import io.confluent.connect.storage.partitioner.Partitioner;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveMetaStoreUpdaterImpl implements HiveMetaStoreUpdater {
  private static final Logger log = LoggerFactory.getLogger(HiveMetaStoreUpdaterImpl.class);

  private boolean hiveIntegrationEnabled;
  private long shutDownTimeout;
  private HiveMetaStore hiveMetaStore;
  private String hiveDatabase;
  private ExecutorService executorService;
  private Queue<Callable<Void>> deferredTasks;
  private Queue<Future<Void>> hiveUpdateFutures;
  private Set<String> hivePartitions;
  private HiveUtil hiveUtil;

  public HiveMetaStoreUpdaterImpl(
      final S3SinkConnectorConfig connectorConfig,
      final Format<S3SinkConnectorConfig, String> format
  ) {

    this.shutDownTimeout = connectorConfig.getLong(
      S3SinkConnectorConfig.SHUTDOWN_TIMEOUT_CONFIG
    );
    this.hiveIntegrationEnabled = connectorConfig.hiveIntegrationEnabled();
    if (this.hiveIntegrationEnabled) {
      this.hiveMetaStore = new HiveMetaStore(connectorConfig);
      try {
        final HiveFactory hiveFactory = (HiveFactory)format.getHiveFactory();
        this.hiveUtil = hiveFactory.createHiveUtil(connectorConfig, hiveMetaStore);
      } catch (Exception e) {
        log.warn(
            String.format(
              "Unable to get HiveFactory from {}. Hive integration cannot be enabled.",
              format.getClass().getName()
            ),
            e
        );
        this.hiveIntegrationEnabled = false;
      }
      this.hiveDatabase = connectorConfig.getString(HiveConfig.HIVE_DATABASE_CONFIG);
      this.executorService = Executors.newSingleThreadExecutor();
      this.deferredTasks = new LinkedList<>();
      this.hiveUpdateFutures = new LinkedList<>();
      this.deferredTasks = new LinkedList<>();
      this.hivePartitions = new HashSet<>();
    }
  }

  public void createHiveTable(
      final String tableName,
      final Schema schema,
      final Partitioner partitioner,
      final TopicPartition tp) {

    if (!hiveIntegrationEnabled) {
      return;
    }
    deferredTasks.add(() -> {
      try {
        this.hiveUtil.createTable(hiveDatabase, tableName, schema, partitioner, tp.topic());
      } catch (Throwable e) {
        log.error("Creating Hive table threw unexpected error", e);
      }
      return null;
    });
  }

  public void alterHiveSchema(final String tableName, final Schema schema) {
    if (!hiveIntegrationEnabled) {
      return;
    }
    deferredTasks.add(() -> {
      try {
        this.hiveUtil.alterSchema(hiveDatabase, tableName, schema);
      } catch (Throwable e) {
        log.error("Altering Hive schema threw unexpected error", e);
      }
      return null;
    });
  }

  public void addHivePartition(final String tableName, final String location) {
    if (!hiveIntegrationEnabled || hivePartitions.contains(location)) {
      return;
    }
    deferredTasks.add(() -> {
      try {
        hiveMetaStore.addPartition(hiveDatabase, tableName, location);
      } catch (Throwable e) {
        log.error("Adding Hive partition threw unexpected error", e);
      }
      return null;
    });
  }

  public Queue<Future<Void>> getHiveUpdateFutures() {
    return hiveUpdateFutures;
  }

  public void apply() {
    for (Callable<Void> task : deferredTasks) {
      hiveUpdateFutures.add(executorService.submit(task));
    }
    deferredTasks.clear();
  }

  @Override
  public void shutdown() {
    boolean terminated = false;
    try {
      log.info("Shutting down Hive executor service.");
      executorService.shutdown();
      log.info("Awaiting termination.");
      terminated = executorService.awaitTermination(shutDownTimeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      // ignored
    }

    if (!terminated) {
      log.warn(
          "Unclean Hive executor service shutdown, you probably need to sync with Hive next "
              + "time you start the connector"
      );
      executorService.shutdownNow();
    }
    deferredTasks.clear();
    hiveUpdateFutures.clear();
    hivePartitions.clear();
  }

}
