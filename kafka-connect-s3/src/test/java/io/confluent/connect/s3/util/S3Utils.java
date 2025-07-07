package io.confluent.connect.s3.util;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import org.apache.kafka.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class S3Utils {
    private static final Logger log = LoggerFactory.getLogger(S3Utils.class);

    /**
     * Wait up to {@code timeoutMs} maximum time limit for the connector to write the specified
     * number of files.
     *
     * @param bucketName  S3 bucket name
     * @param numFiles    expected number of files in the bucket
     * @param timeoutMs   maximum time in milliseconds to wait
     * @return the time this method discovered the connector has written the files
     * @throws InterruptedException if this was interrupted
     */
    public static long waitForFilesInBucket(AmazonS3 s3, String bucketName, int numFiles, long timeoutMs)
            throws InterruptedException {
        TestUtils.waitForCondition(
                () -> assertFileCountInBucket(s3, bucketName, numFiles).orElse(false),
                timeoutMs,
                "Files not written to S3 bucket in time."
        );
        return System.currentTimeMillis();
    }

    /**
     * Confirm that the file count in a bucket matches the expected number of files.
     *
     * @param bucketName the name of the bucket containing the files
     * @param expectedNumFiles the number of files expected
     * @return true if the number of files in the bucket match the expected number; false otherwise
     */
    private static Optional<Boolean> assertFileCountInBucket(AmazonS3 s3, String bucketName, int expectedNumFiles) {
        try {
            return Optional.of(getBucketFileCount(s3, bucketName) == expectedNumFiles);
        } catch (Exception e) {
            log.warn("Could not check file count in bucket: {}", bucketName);
            return Optional.empty();
        }
    }

    /**
     * Recursively query the bucket to get the total number of files that exist in the bucket.
     *
     * @param bucketName the name of the bucket containing the files.
     * @return the number of files in the bucket
     */
    private static int getBucketFileCount(AmazonS3 s3, String bucketName) {
        int totalFilesInBucket = 0;
        ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(bucketName);
        ListObjectsV2Result result;
        do {
            /*
            Need the result object to extract the continuation token from the request as each request
            to listObjectsV2() returns a maximum of 1000 files.
            */
            result = s3.listObjectsV2(request);
            totalFilesInBucket += result.getKeyCount();
            String token = result.getNextContinuationToken();
            // To get the next batch of files.
            request.setContinuationToken(token);
        } while(result.isTruncated());
        return totalFilesInBucket;
    }
}
