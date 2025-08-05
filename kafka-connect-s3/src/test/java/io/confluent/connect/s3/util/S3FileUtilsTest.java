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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

public class S3FileUtilsTest {

  private S3Client s3Mock;
  private S3FileUtils s3FileUtils;

  @Before
  public void setUp() {
    s3Mock = mock(S3Client.class);
    s3FileUtils = new S3FileUtils(s3Mock);
  }

  @Test
  public void testBucketExistsWithValidBucket() {
    // Given
    String bucketName = "valid-bucket";
    when(s3Mock.headBucket(any(HeadBucketRequest.class)))
        .thenReturn(HeadBucketResponse.builder().build());

    // When
    boolean result = s3FileUtils.bucketExists(bucketName);

    // Then
    assertTrue(result);
  }

  @Test
  public void testBucketExistsWithBlankBucketName() {
    // When & Then
    assertFalse(s3FileUtils.bucketExists(""));
    assertFalse(s3FileUtils.bucketExists("   "));
    assertFalse(s3FileUtils.bucketExists(null));
  }

  @Test
  public void testBucketExistsWithNotFoundError() {
    // Given
    String bucketName = "non-existent-bucket";
    AwsErrorDetails errorDetails = AwsErrorDetails.builder()
        .errorCode("NoSuchBucket")
        .errorMessage("The specified bucket does not exist")
        .build();
    AwsServiceException notFoundException = AwsServiceException.builder()
        .statusCode(HttpStatusCode.NOT_FOUND)
        .awsErrorDetails(errorDetails)
        .build();
    when(s3Mock.headBucket(any(HeadBucketRequest.class)))
        .thenThrow(notFoundException);

    // When
    boolean result = s3FileUtils.bucketExists(bucketName);

    // Then
    assertFalse(result);
  }

  @Test
  public void testBucketExistsWithRedirectError() {
    // Given
    String bucketName = "bucket-in-different-region";
    AwsErrorDetails errorDetails = AwsErrorDetails.builder()
        .errorCode("PermanentRedirect")
        .errorMessage("The bucket you are attempting to access must be addressed using the specified endpoint")
        .build();
    AwsServiceException redirectException = AwsServiceException.builder()
        .statusCode(HttpStatusCode.MOVED_PERMANENTLY)
        .awsErrorDetails(errorDetails)
        .build();
    when(s3Mock.headBucket(any(HeadBucketRequest.class)))
        .thenThrow(redirectException);

    // When
    boolean result = s3FileUtils.bucketExists(bucketName);

    // Then
    assertTrue(result);
  }

  @Test
  public void testBucketExistsWithAccessDeniedError() {
    // Given
    String bucketName = "access-denied-bucket";
    AwsErrorDetails errorDetails = AwsErrorDetails.builder()
        .errorCode("AccessDenied")
        .errorMessage("Access Denied")
        .build();
    AwsServiceException accessDeniedException = AwsServiceException.builder()
        .statusCode(HttpStatusCode.FORBIDDEN)
        .awsErrorDetails(errorDetails)
        .build();
    when(s3Mock.headBucket(any(HeadBucketRequest.class)))
        .thenThrow(accessDeniedException);

    // When
    boolean result = s3FileUtils.bucketExists(bucketName);

    // Then
    assertTrue(result);
  }

  @Test(expected = AwsServiceException.class)
  public void testBucketExistsWithOtherAwsServiceException() {
    // Given
    String bucketName = "bucket-with-error";
    AwsErrorDetails errorDetails = AwsErrorDetails.builder()
        .errorCode("InternalError")
        .errorMessage("We encountered an internal error. Please try again.")
        .build();
    AwsServiceException otherException = AwsServiceException.builder()
        .statusCode(HttpStatusCode.INTERNAL_SERVER_ERROR)
        .awsErrorDetails(errorDetails)
        .build();
    when(s3Mock.headBucket(any(HeadBucketRequest.class)))
        .thenThrow(otherException);

    // When
    s3FileUtils.bucketExists(bucketName);

    // Then
    // Should throw the exception
  }

  @Test
  public void testFileExistsWithExistingFile() {
    // Given
    String bucket = "test-bucket";
    String key = "test/file.txt";
    when(s3Mock.headObject(any(HeadObjectRequest.class)))
        .thenReturn(HeadObjectResponse.builder().build());

    // When
    boolean result = s3FileUtils.fileExists(bucket, key);

    // Then
    assertTrue(result);
  }

  @Test
  public void testFileExistsWithNonExistentFile() {
    // Given
    String bucket = "test-bucket";
    String key = "non-existent/file.txt";
    NoSuchKeyException noSuchKeyException = NoSuchKeyException.builder()
        .message("The specified key does not exist.")
        .build();
    when(s3Mock.headObject(any(HeadObjectRequest.class)))
        .thenThrow(noSuchKeyException);

    // When
    boolean result = s3FileUtils.fileExists(bucket, key);

    // Then
    assertFalse(result);
  }

  @Test
  public void testFileExistsWithVariousKeys() {
    // Given
    String bucket = "test-bucket";
    when(s3Mock.headObject(any(HeadObjectRequest.class)))
        .thenReturn(HeadObjectResponse.builder().build());

    // When & Then
    assertTrue(s3FileUtils.fileExists(bucket, "simple-file.txt"));
    assertTrue(s3FileUtils.fileExists(bucket, "path/to/file.json"));
    assertTrue(s3FileUtils.fileExists(bucket, "deep/nested/path/file.avro"));
    assertTrue(s3FileUtils.fileExists(bucket, "file-with-special-chars_123.parquet"));
  }

  @Test
  public void testFileExistsWithEmptyStrings() {
    // Given
    String bucket = "test-bucket";
    String key = "";
    when(s3Mock.headObject(any(HeadObjectRequest.class)))
        .thenReturn(HeadObjectResponse.builder().build());

    // When
    boolean result = s3FileUtils.fileExists(bucket, key);

    // Then
    assertTrue(result);
  }
}