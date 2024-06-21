package io.confluent.connect.s3.storage;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.org.bouncycastle.util.Arrays;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;

import io.confluent.common.utils.SystemTime;
import io.confluent.common.utils.Time;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.S3SinkConnectorTestBase;
import io.confluent.connect.s3.S3SinkTaskTest;
import io.confluent.connect.storage.schema.SchemaCompatibility;

public class S3OutputStreamDigestTest extends S3SinkConnectorTestBase {

  private AmazonS3 s3Mock;
  private S3OutputStream stream;
  final static String S3_TEST_KEY_NAME = "key";
  final static String S3_EXCEPTION_MESSAGE = "this is an s3 exception";
  private Map<String,String> localProps = new HashMap<>();

  @Override
  public void setUp() throws Exception {
    super.setUp();
    s3Mock = mock(AmazonS3.class);
    properties.putAll(localProps);
    connectorConfig = PowerMockito.spy(new S3SinkConnectorConfig(properties));
    stream = new S3OutputStream(S3_TEST_KEY_NAME, connectorConfig, s3Mock);

    InitiateMultipartUploadResult initiateResult = mock(InitiateMultipartUploadResult.class);
    when(s3Mock.initiateMultipartUpload(Mockito.any())).thenReturn(initiateResult);
    when(initiateResult.getUploadId()).thenReturn("upload-id");

    UploadPartResult result = mock(UploadPartResult.class);
    when(s3Mock.uploadPart(Mockito.any())).thenReturn(result);
    when(result.getPartETag()).thenReturn(mock(PartETag.class));
  }

  @Override
  protected Map<String,String> createProps() {
    Map<String,String> props = super.createProps();
    props.putAll(localProps);
    props.put(S3SinkConnectorConfig.SEND_DIGEST_CONFIG, "true");
    return props;
  }

  @Test
  public void testDigestOnePartIsCorrect() throws Exception {
    int size = 5242880;
    localProps.put(S3SinkConnectorConfig.PART_SIZE_CONFIG, String.valueOf(size));
    setUp();

    byte myBytes[] = new byte[size - 1];
    Random r = new Random(1);
    r.nextBytes(myBytes);

    stream.write(myBytes);
    stream.commit();

    ArgumentCaptor<UploadPartRequest> uploadPartRequestArgumentCaptor = ArgumentCaptor.forClass(
        UploadPartRequest.class);
    Mockito.verify(s3Mock, times(1)).uploadPart(uploadPartRequestArgumentCaptor.capture());

    UploadPartRequest request = uploadPartRequestArgumentCaptor.getValue();
    Assert.assertNotNull(request);
    Assert.assertEquals(request.getMd5Digest(), Base64.getEncoder().encodeToString(DigestUtils.md5(myBytes)));
  }

  @Test
  public void testDigestMultiPartIsCorrect() throws Exception {
    int size = 5242880;
    localProps.put(S3SinkConnectorConfig.PART_SIZE_CONFIG, String.valueOf(size));
    setUp();

    byte[] myBytes = new byte[size + 1];
    Random r = new Random(1);
    r.nextBytes(myBytes);

    stream.write(myBytes);
    stream.commit();

    ArgumentCaptor<UploadPartRequest> uploadPartRequestArgumentCaptor = ArgumentCaptor.forClass(
        UploadPartRequest.class);
    Mockito.verify(s3Mock, times(2)).uploadPart(uploadPartRequestArgumentCaptor.capture());

    Assert.assertEquals(2, uploadPartRequestArgumentCaptor.getAllValues().size());

    UploadPartRequest request1 = uploadPartRequestArgumentCaptor.getAllValues().get(0);
    Assert.assertNotNull(request1);
    Assert.assertEquals(request1.getMd5Digest(),
        Base64.getEncoder().encodeToString(DigestUtils.md5(Arrays.copyOf(myBytes, size))));

    UploadPartRequest request2 = uploadPartRequestArgumentCaptor.getAllValues().get(1);
    Assert.assertNotNull(request2);

    byte[] myBytesEnd = new byte[1];
    myBytesEnd[0] = myBytes[size];
    Assert.assertEquals(request2.getMd5Digest(),
        Base64.getEncoder().encodeToString(DigestUtils.md5(myBytesEnd)));
  }
}
