package io.confluent.connect.s3.storage;

import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.S3SinkConnectorTestBase;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.testcontainers.shaded.org.bouncycastle.util.Arrays;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class S3OutputStreamDigestTest extends S3SinkConnectorTestBase {

  private S3Client s3Mock;
  private S3OutputStream stream;
  final static String S3_TEST_KEY_NAME = "key";
  final static String S3_EXCEPTION_MESSAGE = "this is an s3 exception";
  private Map<String,String> localProps = new HashMap<>();

  @Override
  public void setUp() throws Exception {
    super.setUp();
    s3Mock = mock(S3Client.class);
    properties.putAll(localProps);
    connectorConfig = PowerMockito.spy(new S3SinkConnectorConfig(properties));
    stream = new S3OutputStream(S3_TEST_KEY_NAME, connectorConfig, s3Mock);

    CreateMultipartUploadResponse initiateResult = mock(CreateMultipartUploadResponse.class);
    when(s3Mock.createMultipartUpload(Mockito.any(CreateMultipartUploadRequest.class))).thenReturn(initiateResult);
    when(initiateResult.uploadId()).thenReturn("upload-id");

    UploadPartResponse result = mock(UploadPartResponse.class);
    when(s3Mock.uploadPart(Mockito.any(UploadPartRequest.class), Mockito.any(RequestBody.class))).thenReturn(result);
    when(result.eTag()).thenReturn(mock(String.class));
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

    byte[] payload = new byte[size - 1];
    Random r = new Random(1);
    r.nextBytes(payload);

    stream.write(payload);
    stream.commit();

    ArgumentCaptor<UploadPartRequest> uploadPartRequestArgumentCaptor = ArgumentCaptor.forClass(
        UploadPartRequest.class);
    Mockito.verify(s3Mock, times(1)).uploadPart(uploadPartRequestArgumentCaptor.capture(), Mockito.any(RequestBody.class));

    UploadPartRequest request = uploadPartRequestArgumentCaptor.getValue();
    Assert.assertNotNull(request);
    Assert.assertEquals(request.contentMD5(), Base64.getEncoder().encodeToString(DigestUtils.md5(payload)));
  }

  @Test
  public void testDigestMultiPartIsCorrect() throws Exception {
    int size = 5242880;
    localProps.put(S3SinkConnectorConfig.PART_SIZE_CONFIG, String.valueOf(size));
    setUp();

    byte[] payload = new byte[size + 1];
    Random r = new Random(1);
    r.nextBytes(payload);

    stream.write(payload);
    stream.commit();

    ArgumentCaptor<UploadPartRequest> uploadPartRequestArgumentCaptor = ArgumentCaptor.forClass(
        UploadPartRequest.class);
    Mockito.verify(s3Mock, times(2)).uploadPart(uploadPartRequestArgumentCaptor.capture(), Mockito.any(RequestBody.class));

    Assert.assertEquals(2, uploadPartRequestArgumentCaptor.getAllValues().size());

    UploadPartRequest request1 = uploadPartRequestArgumentCaptor.getAllValues().get(0);
    Assert.assertNotNull(request1);
    Assert.assertEquals(request1.contentMD5(),
        Base64.getEncoder().encodeToString(DigestUtils.md5(Arrays.copyOf(payload, size))));

    UploadPartRequest request2 = uploadPartRequestArgumentCaptor.getAllValues().get(1);
    Assert.assertNotNull(request2);

    byte[] payloadEnd = new byte[1];
    payloadEnd[0] = payload[size];
    Assert.assertEquals(request2.contentMD5(),
        Base64.getEncoder().encodeToString(DigestUtils.md5(payloadEnd)));
  }
}
