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

package io.confluent.connect.s3.util;

import com.amazonaws.services.s3.internal.BucketNameUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.StorageFactory;
import io.confluent.connect.storage.common.StorageCommonConfig;

public class S3BucketCheck {

  public static boolean checkBucketExists(S3SinkConnectorConfig config) {
    @SuppressWarnings("unchecked")
    Class<? extends S3Storage> storageClass =
        (Class<? extends S3Storage>) config.getClass(StorageCommonConfig.STORAGE_CLASS_CONFIG);

    final S3Storage bucketCheckHelper = StorageFactory.createStorage(storageClass,
        S3SinkConnectorConfig.class, config,
        config.getString(StorageCommonConfig.STORE_URL_CONFIG));

    return bucketCheckHelper.bucketExists();
  }

  public static ConfigDef.Validator bucketNameValidator() {
    return new BucketNameValidator();
  }

  private static class BucketNameValidator implements ConfigDef.Validator {
    @Override
    public void ensureValid(String name, Object bucket) {
      String bucketName = ((String) bucket).trim();
      try {
        BucketNameUtils.validateBucketName(bucketName);
      } catch (IllegalArgumentException e) {
        throw new ConfigException(
            String.format(
                "'%s' is not a valid bucket name and does not follow AWS guidelines. %s",
                bucketName,
                e.getMessage()
            )
        );
      }
    }

    @Override
    public String toString() {
      return "Follows the AWS guidelines for S3 bucket names";
    }
  }
}
