package io.confluent.connect.s3.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.connect.s3.S3SinkConnectorConfig;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;

import io.confluent.connect.s3.S3SinkConnectorTestBase;
import io.confluent.connect.s3.errors.FileExistsException;
import io.findify.s3mock.S3Mock;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Before;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.S3Exception;

import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import org.junit.Test;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Map;

public class S3OutputStreamTest extends S3SinkConnectorTestBase {

  private S3Client s3Mock;
  private S3OutputStream stream;
  final static String S3_TEST_KEY_NAME = "key";
  final static String S3_EXCEPTION_MESSAGE = "this is an s3 exception";

  @Captor
  ArgumentCaptor<CreateMultipartUploadRequest> initMultipartRequestCaptor;

  @Captor
  ArgumentCaptor<CompleteMultipartUploadRequest> completeMultipartRequestCaptor;

  @Before
  public void before() throws Exception {
    super.setUp();
    s3Mock = mock(S3Client.class);
    stream = new S3OutputStream(S3_TEST_KEY_NAME, connectorConfig, s3Mock);
    MockitoAnnotations.initMocks(this);
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

  @Test
  public void testNewMultipartUploadAESSSE() throws IOException {

    Map<String, String> props = createProps();
    props.put(S3SinkConnectorConfig.SSEA_CONFIG, ServerSideEncryption.AES256.toString());
    stream = new S3OutputStream(S3_TEST_KEY_NAME, new S3SinkConnectorConfig(props), s3Mock);

    when(s3Mock.createMultipartUpload(any(CreateMultipartUploadRequest.class))).thenReturn(mock(CreateMultipartUploadResponse.class));
    stream.newMultipartUpload();

    verify(s3Mock).createMultipartUpload(initMultipartRequestCaptor.capture());

    assertNotNull(initMultipartRequestCaptor.getValue());
    assertEquals(ServerSideEncryption.AES256, initMultipartRequestCaptor.getValue().serverSideEncryption());
    assertNull(initMultipartRequestCaptor.getValue().sseCustomerKey());
    assertNull(initMultipartRequestCaptor.getValue().ssekmsKeyId());
  }

  @Test
  public void testNewMultipartUploadKMSSSE() throws IOException {

    Map<String, String> props = createProps();
    props.put(S3SinkConnectorConfig.SSEA_CONFIG, ServerSideEncryption.AWS_KMS.toString());
    props.put(S3SinkConnectorConfig.SSE_KMS_KEY_ID_CONFIG, "key1");
    stream = new S3OutputStream(S3_TEST_KEY_NAME, new S3SinkConnectorConfig(props), s3Mock);

    when(s3Mock.createMultipartUpload(any(CreateMultipartUploadRequest.class))).thenReturn(mock(CreateMultipartUploadResponse.class));
    stream.newMultipartUpload();

    verify(s3Mock).createMultipartUpload(initMultipartRequestCaptor.capture());

    assertNotNull(initMultipartRequestCaptor.getValue());
    assertEquals(ServerSideEncryption.AWS_KMS, initMultipartRequestCaptor.getValue().serverSideEncryption());
    assertNotNull(initMultipartRequestCaptor.getValue().ssekmsKeyId());
    assertNull(initMultipartRequestCaptor.getValue().sseCustomerKey());

  }

  @Test
  public void testNewMultipartUploadCustomerKeySSE() throws IOException {

    Map<String, String> props = createProps();
    props.put(S3SinkConnectorConfig.SSEA_CONFIG, ServerSideEncryption.AES256.toString());
    props.put(S3SinkConnectorConfig.SSE_CUSTOMER_KEY, "key1");
    stream = new S3OutputStream(S3_TEST_KEY_NAME, new S3SinkConnectorConfig(props), s3Mock);

    when(s3Mock.createMultipartUpload(any(CreateMultipartUploadRequest.class))).thenReturn(mock(CreateMultipartUploadResponse.class));
    stream.newMultipartUpload();

    verify(s3Mock).createMultipartUpload(initMultipartRequestCaptor.capture());

    assertNotNull(initMultipartRequestCaptor.getValue());
    assertEquals("AES256", initMultipartRequestCaptor.getValue().sseCustomerAlgorithm());
    assertNotNull(initMultipartRequestCaptor.getValue().sseCustomerKey());
    assertNull(initMultipartRequestCaptor.getValue().ssekmsKeyId());

  }

  @Test
  public void testNewMultipartUploadDefaultSSE() throws IOException {
    stream = new S3OutputStream(S3_TEST_KEY_NAME, connectorConfig, s3Mock);

    when(s3Mock.createMultipartUpload(any(CreateMultipartUploadRequest.class))).thenReturn(mock(CreateMultipartUploadResponse.class));
    stream.newMultipartUpload();

    verify(s3Mock).createMultipartUpload(initMultipartRequestCaptor.capture());

    assertNotNull(initMultipartRequestCaptor.getValue());
    assertNull(initMultipartRequestCaptor.getValue().serverSideEncryption());
    assertNull(initMultipartRequestCaptor.getValue().sseCustomerKey());
    assertNull(initMultipartRequestCaptor.getValue().ssekmsKeyId());
  }

  @Test
  public void testCompleteMultipartUploadWithConditionalWrites() throws IOException {
    Map<String, String> props = super.createProps();
    props.put(S3SinkConnectorConfig.ENABLE_CONDITIONAL_WRITES_CONFIG, "true");
    props.put(S3SinkConnectorConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG, "100");

    stream = new S3OutputStream(S3_TEST_KEY_NAME, new S3SinkConnectorConfig(props), s3Mock);

    when(s3Mock.completeMultipartUpload(any(CompleteMultipartUploadRequest.class))).thenReturn(mock(CompleteMultipartUploadResponse.class));
    when(s3Mock.createMultipartUpload(any(CreateMultipartUploadRequest.class))).thenReturn(mock(CreateMultipartUploadResponse.class));
    when(s3Mock.uploadPart(any(UploadPartRequest.class), any(RequestBody.class))).thenReturn(mock(UploadPartResponse.class));

    stream.commit();

    verify(s3Mock).completeMultipartUpload(completeMultipartRequestCaptor.capture());

    assertNotNull(completeMultipartRequestCaptor.getValue());
    assertEquals("*", completeMultipartRequestCaptor.getValue().ifNoneMatch());
  }

  @Test
  public void testCompleteMultipartUploadWithoutConditionalWrites() throws IOException {
    Map<String, String> props = super.createProps();
    props.put(S3SinkConnectorConfig.ENABLE_CONDITIONAL_WRITES_CONFIG, "false");
    props.put(S3SinkConnectorConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG, "100");

    stream = new S3OutputStream(S3_TEST_KEY_NAME, new S3SinkConnectorConfig(props), s3Mock);

    when(s3Mock.completeMultipartUpload(any(CompleteMultipartUploadRequest.class))).thenReturn(mock(CompleteMultipartUploadResponse.class));
    when(s3Mock.createMultipartUpload(any(CreateMultipartUploadRequest.class))).thenReturn(mock(CreateMultipartUploadResponse.class));
    when(s3Mock.uploadPart(any(UploadPartRequest.class), any(RequestBody.class))).thenReturn(mock(UploadPartResponse.class));

    stream.commit();

    verify(s3Mock).completeMultipartUpload(completeMultipartRequestCaptor.capture());

    assertNotNull(completeMultipartRequestCaptor.getValue());
    assertNull(completeMultipartRequestCaptor.getValue().ifNoneMatch());
  }

  @Test(expected = FileExistsException.class)
  public void testCompleteMultipartUploadThrowsExceptionWhenFileExists() throws IOException {
    Map<String, String> props = super.createProps();
    props.put(S3SinkConnectorConfig.ENABLE_CONDITIONAL_WRITES_CONFIG, "true");
    props.put(S3SinkConnectorConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG, "100");

    stream = new S3OutputStream(S3_TEST_KEY_NAME, new S3SinkConnectorConfig(props), s3Mock);

    AwsServiceException exception = S3Exception.builder().statusCode(412)
        .build();

    when(s3Mock.completeMultipartUpload(any(CompleteMultipartUploadRequest.class))).thenThrow(exception);
    when(s3Mock.createMultipartUpload(any(CreateMultipartUploadRequest.class))).thenReturn(mock(CreateMultipartUploadResponse.class));
    when(s3Mock.uploadPart(any(UploadPartRequest.class), any(RequestBody.class))).thenReturn(mock(UploadPartResponse.class));
    when(s3Mock.headObject(any(HeadObjectRequest.class))).thenReturn(mock(HeadObjectResponse.class));

    stream.commit();

    verify(s3Mock).completeMultipartUpload(completeMultipartRequestCaptor.capture());

    assertNotNull(completeMultipartRequestCaptor.getValue());
    assertNull(completeMultipartRequestCaptor.getValue().ifNoneMatch());
  }

  @Test(expected = FileExistsException.class)
  public void testCompleteMultipartUploadThrowsExceptionWhenS3Returns200WithPreconditionFailedError() throws IOException {
    Map<String, String> props = super.createProps();
    props.put(S3SinkConnectorConfig.ENABLE_CONDITIONAL_WRITES_CONFIG, "true");
    props.put(S3SinkConnectorConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG, "100");

    stream = new S3OutputStream(S3_TEST_KEY_NAME, new S3SinkConnectorConfig(props), s3Mock);

    AwsServiceException exception = S3Exception.builder().statusCode(200)
        .awsErrorDetails(AwsErrorDetails.builder().errorCode("PreconditionFailed").build())
        .build();

    when(s3Mock.completeMultipartUpload(any(CompleteMultipartUploadRequest.class))).thenThrow(exception);
    when(s3Mock.createMultipartUpload(any(CreateMultipartUploadRequest.class))).thenReturn(mock(CreateMultipartUploadResponse.class));
    when(s3Mock.uploadPart(any(UploadPartRequest.class), any(RequestBody.class))).thenReturn(mock(UploadPartResponse.class));
    when(s3Mock.headObject(any(HeadObjectRequest.class))).thenReturn(mock(HeadObjectResponse.class));

    stream.commit();

    verify(s3Mock).completeMultipartUpload(completeMultipartRequestCaptor.capture());

    assertNotNull(completeMultipartRequestCaptor.getValue());
    assertNull(completeMultipartRequestCaptor.getValue().ifNoneMatch());
  }

  @Test(expected = IOException.class)
  public void testCompleteMultipartUploadRethrowsExceptionWhenAmazonS3Exception() throws IOException {
    Map<String, String> props = super.createProps();
    props.put(S3SinkConnectorConfig.ENABLE_CONDITIONAL_WRITES_CONFIG, "true");
    props.put(S3SinkConnectorConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG, "100");

    stream = new S3OutputStream(S3_TEST_KEY_NAME, new S3SinkConnectorConfig(props), s3Mock);

    AwsServiceException exception = S3Exception.builder().statusCode(422).message("file conflict")
        .build();

    when(s3Mock.completeMultipartUpload(any(CompleteMultipartUploadRequest.class))).thenThrow(exception);
    when(s3Mock.createMultipartUpload(any(CreateMultipartUploadRequest.class))).thenReturn(mock(CreateMultipartUploadResponse.class));
    when(s3Mock.uploadPart(any(UploadPartRequest.class), any(RequestBody.class))).thenReturn(mock(UploadPartResponse.class));

    stream.commit();

    verify(s3Mock).completeMultipartUpload(completeMultipartRequestCaptor.capture());

    assertNotNull(completeMultipartRequestCaptor.getValue());
    assertNull(completeMultipartRequestCaptor.getValue().ifNoneMatch());
  }

  @Test(expected = ConnectException.class)
  public void testCompleteMultipartUploadWithIncorrectS3ResponseThrowsConnectException() throws IOException {
    Map<String, String> props = super.createProps();
    props.put(S3SinkConnectorConfig.ENABLE_CONDITIONAL_WRITES_CONFIG, "true");
    props.put(S3SinkConnectorConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG, "100");

    stream = new S3OutputStream(S3_TEST_KEY_NAME, new S3SinkConnectorConfig(props), s3Mock);

    AwsServiceException exception = S3Exception.builder().statusCode(412).message("file exists")
        .build();

    when(s3Mock.completeMultipartUpload(any(CompleteMultipartUploadRequest.class))).thenThrow(exception);
    when(s3Mock.createMultipartUpload(any(CreateMultipartUploadRequest.class))).thenReturn(mock(CreateMultipartUploadResponse.class));
    when(s3Mock.uploadPart(any(UploadPartRequest.class), any(RequestBody.class))).thenReturn(mock(UploadPartResponse.class));
    when(s3Mock.headObject(any(HeadObjectRequest.class))).thenReturn(mock(HeadObjectResponse.class));

    stream.commit();

    verify(s3Mock).completeMultipartUpload(completeMultipartRequestCaptor.capture());

    assertNotNull(completeMultipartRequestCaptor.getValue());
    assertNull(completeMultipartRequestCaptor.getValue().ifNoneMatch());
  }
}
