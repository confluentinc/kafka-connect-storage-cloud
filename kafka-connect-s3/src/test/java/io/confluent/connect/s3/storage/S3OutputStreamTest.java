package io.confluent.connect.s3.storage;

import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonServiceException.ErrorType;
import com.amazonaws.services.s3.AmazonS3;
import io.confluent.connect.s3.S3SinkConnectorTestBase;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Before;
import org.junit.Test;

public class S3OutputStreamTest extends S3SinkConnectorTestBase {

  private AmazonS3 s3Mock;
  private S3OutputStream stream;

  @Before
  public void before() throws Exception {
    super.setUp();
    s3Mock = mock(AmazonS3.class);
    stream = new S3OutputStream("key", connectorConfig, s3Mock);
  }

  @Test
  public void testPropagateUnretriableS3Exceptions() {
    AmazonServiceException e = new AmazonServiceException("this is an s3 exception");
    e.setErrorType(ErrorType.Client);

    when(s3Mock.initiateMultipartUpload(any())).thenThrow(e);
    assertThrows("Unable to initiate Multipart Upload.", ConnectException.class, () -> stream.commit());
  }

  @Test
  public void testPropagateRetriableS3Exceptions() {
    AmazonServiceException e = new AmazonServiceException("this is an s3 exception");
    e.setErrorType(ErrorType.Service);

    when(s3Mock.initiateMultipartUpload(any())).thenThrow(e);
    assertThrows("Multipart upload failed to complete.", DataException.class, () -> stream.commit());
  }

  @Test
  public void testPropagateOtherRetriableS3Exceptions() {
    when(s3Mock.initiateMultipartUpload(any())).thenThrow(new AmazonClientException("this is an other s3 exception"));
    assertThrows("Multipart upload failed to complete.", DataException.class, () -> stream.commit());
  }
}
