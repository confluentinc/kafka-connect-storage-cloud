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

package io.confluent.connect.s3.storage;

import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.http.HttpStatusCode;


import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.errors.FileExistsException;
import io.confluent.connect.storage.common.util.StringUtils;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.parquet.io.PositionOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.function.Supplier;

/**
 * Output stream enabling multi-part uploads of Kafka records.
 *
 * <p>The implementation has borrowed the general structure of Hadoop's implementation.
 */
public class S3OutputStream extends PositionOutputStream {
  private static final Logger log = LoggerFactory.getLogger(S3OutputStream.class);
  private final S3Client s3;
  private final String bucket;
  private final String key;
  private final String ssea;
  private String sseCustomerKey; // TODO: Manual test with SSE key
  private final String sseKmsKeyId;

  // private final ProgressListener progressListener; TODO find alternative?
  private final int partSize;
  private final ObjectCannedACL cannedAcl; // TODO: Test with canned acl
  private boolean closed;
  private final ByteBuf buffer;
  private MultipartUpload multiPartUpload;
  private final CompressionType compressionType;
  private final int compressionLevel;
  private volatile OutputStream compressionFilter;
  private Long position;
  private final boolean enableDigest;
  private final boolean enableConditionalWrites;

  private static final String PRECONDITION_FAILED_ERROR = "PreconditionFailed";

  public S3OutputStream(String key, S3SinkConnectorConfig conf, S3Client s3) {
    this.s3 = s3;
    this.bucket = conf.getBucketName();
    this.key = key;
    this.ssea = conf.getSsea();
    final String sseCustomerKeyConfig = conf.getSseCustomerKey();
    this.sseCustomerKey = (ServerSideEncryption.AES256.toString().equalsIgnoreCase(ssea)
        && StringUtils.isNotBlank(sseCustomerKeyConfig))
      ? sseCustomerKeyConfig : null;
    this.sseKmsKeyId = conf.getSseKmsKeyId();
    this.partSize = conf.getPartSize();
    this.cannedAcl = conf.getCannedAcl();
    this.closed = false;
    this.enableDigest = conf.isSendDigestEnabled();

    final boolean elasticBufEnable = conf.getElasticBufferEnable();
    if (elasticBufEnable) {
      final int elasticBufInitialCap = conf.getElasticBufferInitCap();
      this.buffer = new ElasticByteBuffer(this.partSize, elasticBufInitialCap);
    } else {
      this.buffer = new SimpleByteBuffer(this.partSize);
    }

    this.multiPartUpload = null;
    this.compressionType = conf.getCompressionType();
    this.compressionLevel = conf.getCompressionLevel();
    this.position = 0L;

    this.enableConditionalWrites = conf.shouldEnableConditionalWrites();
    log.info("Create S3OutputStream for bucket '{}' key '{}'", bucket, key);
  }

  @Override
  public void write(int b) throws IOException {
    buffer.put((byte) b);
    if (!buffer.hasRemaining()) {
      uploadPart();
    }
    position++;
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if (outOfRange(off, b.length) || len < 0 || outOfRange(off + len, b.length)) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return;
    }

    if (buffer.remaining() <= len) {
      int firstPart = buffer.remaining();
      buffer.put(b, off, firstPart);
      position += firstPart;
      uploadPart();
      write(b, off + firstPart, len - firstPart);
    } else {
      buffer.put(b, off, len);
      position += len;
    }
  }

  private static boolean outOfRange(int off, int len) {
    return off < 0 || off > len;
  }

  private void uploadPart() throws IOException {
    uploadPart(partSize);
    buffer.clear();
  }

  private void uploadPart(final int size) throws IOException {
    if (multiPartUpload == null) {
      log.debug("New multi-part upload for bucket '{}' key '{}'", bucket, key);
      multiPartUpload = newMultipartUpload();
    }
    try {
      multiPartUpload.uploadPart(new ByteArrayInputStream(buffer.array()), size);
    } catch (Exception e) {
      if (multiPartUpload != null) {
        multiPartUpload.abort();
        log.debug("Multipart upload aborted for bucket '{}' key '{}'.", bucket, key);
      }
      throw new IOException("Part upload failed: ", e);
    }
  }

  public void commit() throws IOException {
    if (closed) {
      log.warn(
          "Tried to commit data for bucket '{}' key '{}' on a closed stream. Ignoring.",
          bucket,
          key
      );
      return;
    }

    try {
      compressionType.finalize(compressionFilter);
      if (buffer.hasRemaining()) {
        uploadPart(buffer.position());
      }
      multiPartUpload.complete();
      log.debug("Upload complete for bucket '{}' key '{}'", bucket, key);
    } catch (IOException e) {
      log.error(
          "Multipart upload failed to complete for bucket '{}' key '{}'. Reason: {}",
          bucket,
          key,
          e.getMessage()
      );
      throw e;
    } finally {
      buffer.clear();
      multiPartUpload = null;
      internalClose();
    }
  }

  @Override
  public void close() throws IOException {
    internalClose();
  }

  private void internalClose() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    if (multiPartUpload != null) {
      multiPartUpload.abort();
      log.debug("Multipart upload aborted for bucket '{}' key '{}'.", bucket, key);
    }
    super.close();
  }

  MultipartUpload newMultipartUpload() throws IOException {
    CreateMultipartUploadRequest.Builder initRequest =
        CreateMultipartUploadRequest.builder().acl(cannedAcl).bucket(bucket).key(key);

    if (ServerSideEncryption.AES256.toString().equalsIgnoreCase(ssea)
        && sseCustomerKey == null) {
      log.debug("Using SSE (AES256) without customer key");
      initRequest.serverSideEncryption(ServerSideEncryption.AES256);
    } else if (ServerSideEncryption.AWS_KMS.toString().equalsIgnoreCase(ssea)
        && StringUtils.isNotBlank(sseKmsKeyId)) {
      log.debug("Using KMS Key ID: {}", sseKmsKeyId);
      initRequest.ssekmsKeyId(sseKmsKeyId);
      initRequest.serverSideEncryption(ServerSideEncryption.AWS_KMS);
    } else if (sseCustomerKey != null) {
      log.debug("Using KMS Customer Key");
      initRequest.sseCustomerKey(sseCustomerKey);
      initRequest.sseCustomerAlgorithm("AES256");
    }

    return handleAmazonExceptions(
      () -> new MultipartUpload(s3.createMultipartUpload(initRequest.build()).uploadId())
    );
  }

  /**
   * Return the given Supplier value, converting any thrown AmazonClientException object
   * to an IOException object (containing the AmazonClientException object) and
   * throw that instead.
   * @param supplier The supplier to evaluate
   * @param <T> The object type returned by the Supplier
   * @return The value returned by the Supplier
   * @throws IOException Any IOException or AmazonClientException thrown while
   *                     retreiving the Supplier's value
   */
  private static <T> T handleAmazonExceptions(Supplier<T> supplier) throws IOException {
    try {
      return supplier.get();
    } catch (SdkException e) {
      throw new IOException(e.getMessage(), e);
    }
  }

  private class MultipartUpload {
    private final String uploadId;
    //private final List<String> partETags;
    private final List<CompletedPart> completedParts;

    public MultipartUpload(String uploadId) {
      this.uploadId = uploadId;
      //this.partETags = new ArrayList<>();
      this.completedParts = new ArrayList<>();
      log.debug(
          "Initiated multi-part upload for bucket '{}' key '{}' with id '{}'",
          bucket,
          key,
          uploadId
      );
    }

    public void uploadPart(ByteArrayInputStream inputStream, int partSize) {
      int currentPartNumber = completedParts.size() + 1;
      UploadPartRequest.Builder requestBuilder = UploadPartRequest.builder()
          .bucket(bucket)
          .key(key)
          .uploadId(uploadId)
          .sseCustomerKey(sseCustomerKey)
          .partNumber(currentPartNumber)
          .contentLength((long) partSize);

      RequestBody requestBody = RequestBody.fromInputStream(inputStream, partSize);

      if (enableDigest) {
        requestBuilder = requestBuilder.contentMD5(computeDigest(inputStream, partSize));
      }

      log.debug("Uploading part {} for id '{}'", currentPartNumber, uploadId);
      UploadPartResponse uploadPartResponse = s3.uploadPart(requestBuilder.build(), requestBody);
      //partETags.add(uploadPartResponse.eTag());
      completedParts.add(CompletedPart.builder()
          .eTag(uploadPartResponse.eTag())
          .partNumber(currentPartNumber)
          .build());
    }

    /**
     * Consumes {@code partSize} bytes from the provided {@code inputStream} and computes the MD5
     * digest.
     * Resets the {@code inputStream} before returning digest.
     *
     * @param inputStream {@link ByteArrayInputStream} for upload part request payload
     * @param partSize upload part size byte count
     * @return MD5 digest of the provided input stream
     */
    private String computeDigest(ByteArrayInputStream inputStream, int partSize) {
      byte[] streamBytes = new byte[partSize];
      inputStream.read(streamBytes, 0, partSize);
      String digest = Base64.getEncoder().encodeToString(DigestUtils.md5(streamBytes));
      inputStream.reset();
      log.debug("Computed digest {} for id '{}'", digest, uploadId);
      return digest;
    }

    public void complete() throws IOException {
      log.debug("Completing multi-part upload for key '{}', id '{}'", key, uploadId);
      CompleteMultipartUploadRequest completeRequest =
          CompleteMultipartUploadRequest.builder()
              .bucket(bucket)
              .key(key)
              .uploadId(uploadId)
              .multipartUpload(CompletedMultipartUpload.builder()
                  .parts(completedParts)
                  .build())
              .build();

      if (enableConditionalWrites) {
        completeRequest.ifNoneMatch();
      }

      handleAmazonExceptions(
          () -> {
            try {
              return s3.completeMultipartUpload(completeRequest);
            } catch (S3Exception e) {
              log.error("Failed to complete multipart upload of file {}", key, e);
              // There can be cases where the s3 api returns 200 status code, but the error code
              // is set to "PreconditionFailed". We include additional check on error code to
              // capture such cases.
              if (e.statusCode() == 412
                  || PRECONDITION_FAILED_ERROR.equals(e.awsErrorDetails().errorCode())) {
                // Sanity check to double-check file exists in S3 before skipping the offset
                boolean exists = fileExists();
                if (exists) {
                  throw new FileExistsException("File already exists");
                }
                throw new ConnectException("Unexpected state - Contradicting response from "
                    + "conditional upload and file exists call in S3");
              }
              throw e;
            }
          }
      );
    }



    private boolean fileExists() {
      try {
        s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build());
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

    public void abort() {
      log.warn("Aborting multi-part upload with id '{}'", uploadId);
      try {
        s3.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                .bucket(bucket)
                .key(key)
                .uploadId(uploadId)
            .build());
      } catch (Exception e) {
        // ignoring failure on abort.
        log.warn("Unable to abort multipart upload, you may need to purge uploaded parts: ", e);
      }
    }
  }

  public OutputStream wrapForCompression() {
    if (compressionFilter == null) {
      // Initialize compressionFilter the first time this method is called.
      compressionFilter = compressionType.wrapForOutput(this, compressionLevel);
    }
    return compressionFilter;
  }

  public long getPos() {
    return position;
  }
}
