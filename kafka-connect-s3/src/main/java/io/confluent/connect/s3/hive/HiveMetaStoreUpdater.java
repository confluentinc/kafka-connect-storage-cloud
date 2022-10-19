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

import io.confluent.connect.storage.partitioner.Partitioner;
import java.util.Queue;
import java.util.concurrent.Future;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;

public interface HiveMetaStoreUpdater {

  void createHiveTable(
      final String tableName,
      final Schema schema,
      final Partitioner partitioner,
      final TopicPartition tp);

  void alterHiveSchema(final String tableName, final Schema schema);

  void addHivePartition(final String tableName, final String location);

  Queue<Future<Void>> getHiveUpdateFutures();

  void apply();

  void shutdown();

}
