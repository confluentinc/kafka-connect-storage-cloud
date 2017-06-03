/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.s3;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.junit.After;

import java.util.HashMap;
import java.util.Map;

import io.confluent.connect.s3.format.avro.AvroFormat;
import io.confluent.connect.storage.StorageSinkTestBase;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.hive.HiveConfig;
import io.confluent.connect.storage.hive.schema.DefaultSchemaGenerator;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import io.confluent.connect.storage.schema.SchemaCompatibility;
import io.confluent.connect.storage.schema.StorageSchemaCompatibility;

public class S3SinkConnectorTestBase extends StorageSinkTestBase {

  protected static final String S3_TEST_URL = "http://127.0.0.1:8181";
  protected static final String S3_TEST_BUCKET_NAME = "kafka.bucket";

  protected S3SinkConnectorConfig connectorConfig;
  protected String topicsDir;
  protected Map<String, Object> parsedConfig;
  protected SchemaCompatibility compatibility;

  @Override
  protected Map<String, String> createProps() {
    url = S3_TEST_URL;
    Map<String, String> props = super.createProps();
    props.put(StorageCommonConfig.STORAGE_CLASS_CONFIG, "io.confluent.connect.s3.storage.S3Storage");
    props.put(S3SinkConnectorConfig.S3_BUCKET_CONFIG, S3_TEST_BUCKET_NAME);
    props.put(S3SinkConnectorConfig.FORMAT_CLASS_CONFIG, AvroFormat.class.getName());
    props.put(MockS3SinkConnectorConfig.TEST_PART_SIZE_CONFIG, "1024");
    props.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, PartitionerConfig.PARTITIONER_CLASS_DEFAULT.getName());
    props.put(PartitionerConfig.SCHEMA_GENERATOR_CLASS_CONFIG, DefaultSchemaGenerator.class.getName());
    props.put(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG, "int");
    props.put(PartitionerConfig.PATH_FORMAT_CONFIG, "'year'=YYYY_'month'=MM_'day'=dd_'hour'=HH");
    props.put(PartitionerConfig.LOCALE_CONFIG, "en");
    props.put(PartitionerConfig.TIMEZONE_CONFIG, "America/Los_Angeles");
    props.put(HiveConfig.HIVE_CONF_DIR_CONFIG, "America/Los_Angeles");
    return props;
  }

  //@Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    connectorConfig = new MockS3SinkConnectorConfig(properties);
    topicsDir = connectorConfig.getString(StorageCommonConfig.TOPICS_DIR_CONFIG);
    parsedConfig = new HashMap<>(connectorConfig.plainValues());
    compatibility = StorageSchemaCompatibility.getCompatibility(
                    connectorConfig.getString(HiveConfig.SCHEMA_COMPATIBILITY_CONFIG));
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

  public AmazonS3 newS3Client(S3SinkConnectorConfig config) {
    AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard()
               .withAccelerateModeEnabled(config.getBoolean(S3SinkConnectorConfig.WAN_MODE_CONFIG))
               .withPathStyleAccessEnabled(true)
               .withCredentials(new DefaultAWSCredentialsProviderChain());

    builder = url == null ?
                  builder.withRegion(config.getString(S3SinkConnectorConfig.REGION_CONFIG)) :
                  builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(url, ""));

    return builder.build();
  }

}

