/*
 * Copyright [2020 - 2020] Confluent Inc.
 */

package io.confluent.connect.s3.integration;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.s3.util.S3Utils;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

@Category(IntegrationTest.class)
public abstract class BaseConnectorNetworkIT extends BaseConnectorIT {

  private static final Logger log = LoggerFactory.getLogger(BaseConnectorNetworkIT.class);

  protected static final long CONSUME_MAX_DURATION_MS = TimeUnit.MINUTES.toMillis(8);
  protected static PumbaPauseContainer pumbaPauseContainer;


  /**
   * Creates root client that will be used to change bucket permissions.
   * Prerequisite : Access key and Secret access key should be set as environment variables
   */
  protected void createS3RootClient() {
    log.info("Creating root S3 Client.");
    S3Client = AmazonS3ClientBuilder.standard()
        .withCredentials(
            new AWSStaticCredentialsProvider(
                new BasicAWSCredentials(
                    System.getenv("ROOT_USER_ACCESS_KEY_ID"),
                    System.getenv("ROOT_USER_SECRET_ACCESS_KEY"))))
        .withRegion("ap-south-1")
        .build();
  }

  /*
   Overridden this method to use a more suitable maxWaitMs since the Network IT test take a longer
   time to run.
  */
  @Override
  protected long waitForFilesInBucket(
      String bucketName,
      int numFiles)
      throws InterruptedException {
    return S3Utils.waitForFilesInBucket(S3Client, bucketName, numFiles, CONSUME_MAX_DURATION_MS);
  }

  protected void startPumbaPauseContainer() {
    pumbaPauseContainer = new PumbaPauseContainer();
    pumbaPauseContainer.start();
  }

}
