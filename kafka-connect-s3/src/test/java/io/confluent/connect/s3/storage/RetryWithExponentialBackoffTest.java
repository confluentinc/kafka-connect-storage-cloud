package io.confluent.connect.s3.storage;

import com.amazonaws.SdkClientException;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class RetryWithExponentialBackoffTest {

   @Test
   public void testNoException(){
      final AtomicInteger count = new AtomicInteger();
      S3OutputStream.RetryWithExponentialBackoff retry = S3OutputStream.RetryWithExponentialBackoff.blocking(new Runnable() {
         @Override
         public void run() {
            count.incrementAndGet();
         }
      }, 3, "Error!");
      Assert.assertEquals(1, count.get());
      Assert.assertEquals(0, retry.getFailCount());
   }

   @Test
   public void testAlwaysFailing(){
      final AtomicInteger count = new AtomicInteger();
      boolean failed = false;
      try {
         S3OutputStream.RetryWithExponentialBackoff.blocking(new Runnable() {
            @Override
            public void run() {
               count.incrementAndGet();
               throw new SdkClientException("Error!");
            }
         }, 3, "Error!");
      } catch (ConnectException ce){
         failed = true;
         Assert.assertTrue(ce.getCause() instanceof SdkClientException);
      }
      Assert.assertTrue(failed);
      Assert.assertEquals(3, count.get());
   }

   @Test
   public void testFailTwoTimes(){
      final AtomicInteger count = new AtomicInteger();
      S3OutputStream.RetryWithExponentialBackoff retry = S3OutputStream.RetryWithExponentialBackoff.blocking(new Runnable() {
         @Override
         public void run() {
            count.incrementAndGet();
            if(count.get() < 3){
               throw new SdkClientException("Error!");
            }
         }
      }, 3, "Error!");
      Assert.assertEquals(2, retry.getFailCount());
   }

   @Test(expected = ConnectException.class)
   public void testNoRetries(){
      S3OutputStream.RetryWithExponentialBackoff.blocking(new Runnable() {
         @Override
         public void run() {
            throw new SdkClientException("Boom!");
         }
      }, 0, "Error!");
   }

   @Test(expected = RuntimeException.class)
   public void testOnlyRetrySdkClientException(){
      S3OutputStream.RetryWithExponentialBackoff.blocking(new Runnable() {
         @Override
         public void run() {
            throw new RuntimeException("Boom!");
         }
      }, 3, "Error!");
   }
}
