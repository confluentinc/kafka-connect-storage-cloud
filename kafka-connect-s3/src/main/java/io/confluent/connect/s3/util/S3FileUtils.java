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

package io.confluent.connect.s3.util;

import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.http.HttpStatusCode;
import io.confluent.connect.storage.common.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3FileUtils {

  private final S3Client s3Client;

  private static final Logger log = LoggerFactory.getLogger(S3FileUtils.class);

  public S3FileUtils(S3Client s3Client) {
    this.s3Client = s3Client;
  }

  public boolean bucketExists(String bucketName) {
    if (StringUtils.isBlank(bucketName)) {
      return false;
    }
    try {
      s3Client.headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
      return true;
    } catch (AwsServiceException ase) {
      // A redirect error or an AccessDenied exception means the bucket exists but it's not in
      // this region or we don't have permissions to it.
      if ((ase.statusCode() == HttpStatusCode.MOVED_PERMANENTLY)
          || "AccessDenied".equals(ase.awsErrorDetails().errorCode())) {
        log.info("Bucket {} exists, but not in this region or we don't have permissions to it.",
            bucketName);
        return true;
      }
      if (ase.statusCode() == HttpStatusCode.NOT_FOUND) {
        log.info("Bucket {} does not exist.", bucketName);
        return false;
      }
      throw ase;
    }
  }

  public boolean fileExists(String bucket, String key) {
    try {
      s3Client.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build());
      return true;
    } catch (S3Exception e) {
      if (e.statusCode() == HttpStatusCode.NOT_FOUND) {
        return false;
      }
      if (e.statusCode() == HttpStatusCode.MOVED_PERMANENTLY
          || "AccessDenied".equals(e.awsErrorDetails().errorCode())) {
        log.warn("Connector failed with 403 error. Defaulting as file exists", e);
        // To avoid failing connector due to missing ACL, we consider as file exists.
        // We should be fine to assume file exists here since the call is being made only as an
        // additional sanity check after file upload failed with 412
        return true;
      }
      throw e;
    }
  }

}
