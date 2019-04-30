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

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import io.confluent.common.utils.SystemTime;
import io.confluent.common.utils.Time;
import io.confluent.connect.s3.format.avro.AvroFormat;
import io.confluent.connect.storage.StorageSinkConnectorConfig;
import io.confluent.connect.storage.StorageSinkTestBase;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import io.confluent.connect.storage.schema.SchemaCompatibility;
import io.confluent.connect.storage.schema.StorageSchemaCompatibility;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.powermock.api.mockito.PowerMockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class S3SinkConnectorTestBase extends StorageSinkTestBase {

  private static final Logger log = LoggerFactory.getLogger(S3SinkConnectorTestBase.class);

  protected static final String S3_TEST_URL = "http://127.0.0.1:8181";
  protected static final String S3_TEST_BUCKET_NAME = "kafka.bucket";
  protected static final Time SYSTEM_TIME = new SystemTime();

  protected S3SinkConnectorConfig connectorConfig;
  protected String topicsDir;
  protected Map<String, Object> parsedConfig;
  protected SchemaCompatibility compatibility;

  @Rule
  public TestRule watcher = new TestWatcher() {
    @Override
    protected void starting(Description description) {
      log.info(
          "Starting test: {}.{}",
          description.getTestClass().getSimpleName(),
          description.getMethodName()
      );
    }
  };

  @Override
  protected Map<String, String> createProps() {
    url = S3_TEST_URL;
    Map<String, String> props = super.createProps();
    props.put(StorageCommonConfig.STORAGE_CLASS_CONFIG, "io.confluent.connect.s3.storage.S3Storage");
    props.put(S3SinkConnectorConfig.S3_BUCKET_CONFIG, S3_TEST_BUCKET_NAME);
    props.put(S3SinkConnectorConfig.FORMAT_CLASS_CONFIG, AvroFormat.class.getName());
    props.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, PartitionerConfig.PARTITIONER_CLASS_DEFAULT.getName());
    props.put(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG, "int");
    props.put(PartitionerConfig.PATH_FORMAT_CONFIG, "'year'=YYYY_'month'=MM_'day'=dd_'hour'=HH");
    props.put(PartitionerConfig.LOCALE_CONFIG, "en");
    props.put(PartitionerConfig.TIMEZONE_CONFIG, "America/Los_Angeles");
    return props;
  }

  //@Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    connectorConfig = PowerMockito.spy(new S3SinkConnectorConfig(properties));
    PowerMockito.doReturn(1024).when(connectorConfig).getPartSize();
    topicsDir = connectorConfig.getString(StorageCommonConfig.TOPICS_DIR_CONFIG);
    parsedConfig = new HashMap<>(connectorConfig.plainValues());
    compatibility = StorageSchemaCompatibility.getCompatibility(StorageSinkConnectorConfig.SCHEMA_COMPATIBILITY_CONFIG);
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

