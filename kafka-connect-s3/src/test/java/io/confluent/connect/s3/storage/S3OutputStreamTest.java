package io.confluent.connect.s3.storage;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonServiceException.ErrorType;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.SSEAlgorithm;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.S3SinkConnectorTestBase;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Map;

public class S3OutputStreamTest extends S3SinkConnectorTestBase {

  private S3OutputStream stream;
  final static String S3_TEST_KEY_NAME = "key";
  final static String S3_EXCEPTION_MESSAGE = "this is an s3 exception";
  private AmazonS3 s3Mock;

  @Captor
  ArgumentCaptor<InitiateMultipartUploadRequest> captor;

  @Before
  public void before() throws Exception {
    super.setUp();
    s3Mock = mock(AmazonS3.class);
    stream = new S3OutputStream(S3_TEST_KEY_NAME, connectorConfig, s3Mock);
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testPropagateUnretriableS3Exceptions() {
    AmazonServiceException e = new AmazonServiceException(S3_EXCEPTION_MESSAGE);
    e.setErrorType(ErrorType.Client);

    when(s3Mock.initiateMultipartUpload(any())).thenThrow(e);
    assertThrows(IOException.class, () -> stream.commit());
  }

  @Test
  public void testPropagateRetriableS3Exceptions() {
    AmazonServiceException e = new AmazonServiceException(S3_EXCEPTION_MESSAGE);
    e.setErrorType(ErrorType.Service);

    when(s3Mock.initiateMultipartUpload(any())).thenThrow(e);
    assertThrows(IOException.class, () -> stream.commit());
  }

  @Test
  public void testPropagateOtherRetriableS3Exceptions() {
    when(s3Mock.initiateMultipartUpload(any())).thenThrow(new AmazonClientException(S3_EXCEPTION_MESSAGE));
    assertThrows(IOException.class, () -> stream.commit());
  }

  @Test
  public void testNewMultipartUploadAESSSE() throws IOException {

    Map<String, String> props = createProps();
    props.put(S3SinkConnectorConfig.SSEA_CONFIG, SSEAlgorithm.AES256.toString());
    stream = new S3OutputStream(S3_TEST_KEY_NAME, new S3SinkConnectorConfig(props), s3Mock);

    when(s3Mock.initiateMultipartUpload(any())).thenReturn(mock(InitiateMultipartUploadResult.class));
    stream.newMultipartUpload();

    verify(s3Mock).initiateMultipartUpload(captor.capture());

    assertNotNull(captor.getValue());
    assertNotNull(captor.getValue().getObjectMetadata());
    assertNull(captor.getValue().getSSECustomerKey());
    assertNull(captor.getValue().getSSEAwsKeyManagementParams());
  }

  @Test
  public void testNewMultipartUploadKMSSSE() throws IOException {

    Map<String, String> props = createProps();
    props.put(S3SinkConnectorConfig.SSEA_CONFIG, SSEAlgorithm.KMS.toString());
    props.put(S3SinkConnectorConfig.SSE_KMS_KEY_ID_CONFIG, "key1");
    stream = new S3OutputStream(S3_TEST_KEY_NAME, new S3SinkConnectorConfig(props), s3Mock);

    when(s3Mock.initiateMultipartUpload(any())).thenReturn(mock(InitiateMultipartUploadResult.class));
    stream.newMultipartUpload();

    verify(s3Mock).initiateMultipartUpload(captor.capture());

    assertNotNull(captor.getValue());
    assertNull(captor.getValue().getObjectMetadata());
    assertNotNull(captor.getValue().getSSEAwsKeyManagementParams());
    assertNull(captor.getValue().getSSECustomerKey());

  }

  @Test
  public void testNewMultipartUploadCustomerKeySSE() throws IOException {

    Map<String, String> props = createProps();
    props.put(S3SinkConnectorConfig.SSEA_CONFIG, SSEAlgorithm.AES256.toString());
    props.put(S3SinkConnectorConfig.SSE_CUSTOMER_KEY, "key1");
    stream = new S3OutputStream(S3_TEST_KEY_NAME, new S3SinkConnectorConfig(props), s3Mock);

    when(s3Mock.initiateMultipartUpload(any())).thenReturn(mock(InitiateMultipartUploadResult.class));
    stream.newMultipartUpload();

    verify(s3Mock).initiateMultipartUpload(captor.capture());

    assertNotNull(captor.getValue());
    assertNull(captor.getValue().getObjectMetadata());
    assertNotNull(captor.getValue().getSSECustomerKey());
    assertNull(captor.getValue().getSSEAwsKeyManagementParams());

  }

  @Test
  public void testNewMultipartUploadDefaultSSE() throws IOException {
    stream = new S3OutputStream(S3_TEST_KEY_NAME, connectorConfig, s3Mock);

    when(s3Mock.initiateMultipartUpload(any())).thenReturn(mock(InitiateMultipartUploadResult.class));
    stream.newMultipartUpload();

    verify(s3Mock).initiateMultipartUpload(captor.capture());

    assertNotNull(captor.getValue());
    assertNull(captor.getValue().getObjectMetadata());
    assertNull(captor.getValue().getSSECustomerKey());
    assertNull(captor.getValue().getSSEAwsKeyManagementParams());
  }
}
