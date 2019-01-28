/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.s3.storage;

import com.amazonaws.AmazonClientException;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.SSEAlgorithm;
import com.amazonaws.services.s3.model.SSECustomerKey;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import com.amazonaws.services.s3.model.UploadPartRequest;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.storage.common.util.StringUtils;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadPoolExecutor;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Output stream enabling multi-part uploads of Kafka records.
 *
 * <p>The implementation has borrowed the general structure of Hadoop's implementation.
 */
public class S3OutputStream extends OutputStream {
  private static final Logger log = LoggerFactory.getLogger(S3OutputStream.class);
  private final AmazonS3 s3;
  private final String bucket;
  private final String key;
  private final String ssea;
  private final SSECustomerKey sseCustomerKey;
  private final String sseKmsKeyId;
  private final ProgressListener progressListener;
  private final int partSize;
  private final CannedAccessControlList cannedAcl;
  private boolean closed;
  private ByteBuffer buffer;
  private MultipartUpload multiPartUpload;
  private final CompressionType compressionType;
  private volatile OutputStream compressionFilter;

  public S3OutputStream(String key, S3SinkConnectorConfig conf, AmazonS3 s3) {
    this.s3 = s3;
    this.bucket = conf.getBucketName();
    this.key = key;
    this.ssea = conf.getSsea();
    final String sseCustomerKeyConfig = conf.getSseCustomerKey();
    this.sseCustomerKey = (SSEAlgorithm.AES256.toString().equalsIgnoreCase(ssea)
        && StringUtils.isNotBlank(sseCustomerKeyConfig))
      ? new SSECustomerKey(sseCustomerKeyConfig) : null;
    this.sseKmsKeyId = conf.getSseKmsKeyId();
    this.partSize = conf.getPartSize();
    this.cannedAcl = conf.getCannedAcl();
    this.closed = false;
    this.buffer = ByteBuffer.allocate(this.partSize);
    this.progressListener = new ConnectProgressListener();
    this.multiPartUpload = null;
    this.compressionType = conf.getCompressionType();
    log.debug("Create S3OutputStream for bucket '{}' key '{}'", bucket, key);
  }

  @Override
  public void write(int b) throws IOException {
    buffer.put((byte) b);
    if (!buffer.hasRemaining()) {
      uploadPart();
    }
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
      uploadPart();
      write(b, off + firstPart, len - firstPart);
    } else {
      buffer.put(b, off, len);
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
    log.debug("New multi-part upload for bucket '{}' key '{}'", bucket, key);
    if (multiPartUpload == null) {
      multiPartUpload = newMultipartUpload();
    }
    try {
      multiPartUpload.uploadPart(new ByteArrayInputStream(buffer.array().clone()), size);
    } catch (Exception e) {
      if (multiPartUpload != null) {
        multiPartUpload.abort();
        log.debug("Multipart upload aborted for bucket '{}' key '{}'.", bucket, key);
      }
      throw new IOException("Part upload failed: ", e.getCause());
    }
    log.info("Called uploadPart with size {}", size);
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
    } catch (Exception e) {
      log.error("Multipart upload failed to complete for bucket '{}' key '{}'", bucket, key);
      throw new DataException("Multipart upload failed to complete.", e);
    } finally {
      buffer.clear();
      multiPartUpload = null;
      close();
    }
  }

  @Override
  public void close() throws IOException {
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

  private ObjectMetadata newObjectMetadata() {
    ObjectMetadata meta = new ObjectMetadata();
    if (StringUtils.isNotBlank(ssea)) {
      meta.setSSEAlgorithm(ssea);
    }
    return meta;
  }

  private MultipartUpload newMultipartUpload() throws IOException {
    InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(
        bucket,
        key,
        newObjectMetadata()
    ).withCannedACL(cannedAcl);

    if (SSEAlgorithm.KMS.toString().equalsIgnoreCase(ssea)
        && StringUtils.isNotBlank(sseKmsKeyId)) {
      initRequest.setSSEAwsKeyManagementParams(new SSEAwsKeyManagementParams(sseKmsKeyId));
    } else if (sseCustomerKey != null) {
      initRequest.setSSECustomerKey(sseCustomerKey);
    }

    try {
      return new MultipartUpload(s3.initiateMultipartUpload(initRequest).getUploadId());
    } catch (AmazonClientException e) {
      // TODO: elaborate on the exception interpretation. If this is an AmazonServiceException,
      // there's more info to be extracted.
      throw new IOException("Unable to initiate MultipartUpload: " + e, e);
    }
  }

  private class MultipartUpload {
    private final String uploadId;
    private final List<Future<PartETag>> partETags;
    private AtomicInteger requests;
    private ExecutorService pool;

    public MultipartUpload(String uploadId) {
      this.uploadId = uploadId;
      this.partETags = new ArrayList<>();
      this.requests = new AtomicInteger(0);
      // Initialize threadpool for multipart uploads
      int cores = Math.max(1, Runtime.getRuntime().availableProcessors());
      this.pool = new ThreadPoolExecutor(cores, cores * 2, 1, TimeUnit.MINUTES,
              new MultipartUploadQueue(cores * 2), Executors.defaultThreadFactory(),
              new ThreadPoolExecutor.CallerRunsPolicy());
      log.info("Initialized pool for S3OutputStream using cores value of '{}'", cores);
      log.debug(
          "Initiated multi-part upload for bucket '{}' key '{}' with id '{}'",
          bucket,
          key,
          uploadId
      );
    }

    public void uploadPart(final ByteArrayInputStream inputStream, final int partSize) {
      final int currentPartNumber = requests.incrementAndGet();
      Future<PartETag> task = pool.submit(new Callable<PartETag>() {
        public PartETag call() throws Exception {
          log.info("Starting multipart upload request {} ", currentPartNumber);
          UploadPartRequest request = new UploadPartRequest()
                  .withBucketName(bucket)
                  .withKey(key)
                  .withUploadId(uploadId)
                  .withSSECustomerKey(sseCustomerKey)
                  .withInputStream(inputStream)
                  .withPartNumber(currentPartNumber)
                  .withPartSize(partSize)
                  .withGeneralProgressListener(progressListener);
          return s3.uploadPart(request).getPartETag();
        }
      });

      log.debug("Uploading part {} for id '{}'", currentPartNumber, uploadId);
      partETags.add(task);
    }

    public void complete() {
      log.info("Completing multi-part upload for key '{}', id '{}'", key, uploadId);
      ArrayList<PartETag> tags = new ArrayList<>();
      for (Future<PartETag> tagTask : partETags) {
        try {
          tags.add(tagTask.get());
        } catch (InterruptedException e) {
          log.error("Unable to get multipart upload result", e);
        } catch (ExecutionException e) {
          log.error("Unable to get multipart upload result", e);
        }
      }
      pool.shutdown();

      CompleteMultipartUploadRequest completeRequest =
              new CompleteMultipartUploadRequest(bucket, key, uploadId, tags);
      s3.completeMultipartUpload(completeRequest);
    }

    public void abort() {
      log.warn("Aborting multi-part upload with id '{}'", uploadId);
      try {
        s3.abortMultipartUpload(new AbortMultipartUploadRequest(bucket, key, uploadId));
      } catch (Exception e) {
        // ignoring failure on abort.
        log.warn("Unable to abort multipart upload, you may need to purge uploaded parts: ", e);
      } finally {
        pool.shutdown();
      }
    }
  }

  public final class MultipartUploadQueue extends ArrayBlockingQueue<Runnable> {
    private static final long serialVersionUID = -817911632652898427L;

    public MultipartUploadQueue(int capacity) {
      super(capacity);
    }

    @Override
    public boolean offer(Runnable task) {
      try {
        put(task);
      } catch (InterruptedException e) {
        return false;
      }
      return true;
    }

  }

  public OutputStream wrapForCompression() {
    if (compressionFilter == null) {
      // Initialize compressionFilter the first time this method is called.
      compressionFilter = compressionType.wrapForOutput(this);
    }
    return compressionFilter;
  }

  // Dummy listener for now, just logs the event progress.
  private static class ConnectProgressListener implements ProgressListener {
    public void progressChanged(ProgressEvent progressEvent) {
      log.debug("Progress event: " + progressEvent);
    }
  }
}
