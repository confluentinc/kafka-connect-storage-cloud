package io.confluent.connect.s3.storage;

import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import io.confluent.connect.s3.S3SinkConnectorTestBase;
import org.junit.Before;

import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import org.junit.Test;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;

import java.io.IOException;

public class S3OutputStreamTest extends S3SinkConnectorTestBase {

  private S3Client s3Mock;
  private S3OutputStream stream;
  final static String S3_TEST_KEY_NAME = "key";
  final static String S3_EXCEPTION_MESSAGE = "this is an s3 exception";


  @Before
  public void before() throws Exception {
    super.setUp();
    s3Mock = mock(S3Client.class);
    stream = new S3OutputStream(S3_TEST_KEY_NAME, connectorConfig, s3Mock);
  }

  @Test
  public void testPropagateUnretriableS3Exceptions() {
    SdkClientException e = SdkClientException.builder().message(S3_EXCEPTION_MESSAGE).build();

    when(s3Mock.createMultipartUpload(any(CreateMultipartUploadRequest.class))).thenThrow(e);
    assertThrows(IOException.class, () -> stream.commit());
  }

  @Test
  public void testPropagateRetriableS3Exceptions() {
    AwsServiceException e = AwsServiceException.builder().message(S3_EXCEPTION_MESSAGE).build();

    when(s3Mock.createMultipartUpload(any(CreateMultipartUploadRequest.class))).thenThrow(e);
    assertThrows(IOException.class, () -> stream.commit());
  }

  @Test
  public void testPropagateOtherRetriableS3Exceptions() {
    when(s3Mock.createMultipartUpload(any(CreateMultipartUploadRequest.class))).thenThrow(AwsServiceException.builder().message(S3_EXCEPTION_MESSAGE).build());
    assertThrows(IOException.class, () -> stream.commit());
  }
}
