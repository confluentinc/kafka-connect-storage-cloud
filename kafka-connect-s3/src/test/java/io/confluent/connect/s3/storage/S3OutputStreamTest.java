package io.confluent.connect.s3.storage;

import com.amazonaws.SdkClientException;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class S3OutputStreamTest {

   @Test
   public void testRetryNoException(){
      final AtomicInteger count = new AtomicInteger();
      S3OutputStream.retry(new Runnable() {
         @Override
         public void run() {
            count.incrementAndGet();
         }
      }, 3, "Error!");
      Assert.assertEquals(1, count.get());
   }

   @Test
   public void testRetryAlwaysFailing(){
      final AtomicInteger count = new AtomicInteger();
      boolean failed = false;
      try {
         S3OutputStream.retry(new Runnable() {
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

   @Test(expected = ConnectException.class)
   public void testNoRetries(){
      S3OutputStream.retry(new Runnable() {
         @Override
         public void run() {
            throw new SdkClientException("Boom!");
         }
      }, 0, "Error!");
   }

   @Test(expected = RuntimeException.class)
   public void testOnlyRetrySdkClientException(){
      S3OutputStream.retry(new Runnable() {
         @Override
         public void run() {
            throw new RuntimeException("Boom!");
         }
      }, 3, "Error!");
   }

   @Test(expected = TimeoutException.class)
   public void testRetrySleepTime() throws InterruptedException, ExecutionException, TimeoutException {
      FutureTask<Void> futureTask = new FutureTask<>(new Runnable() {
         @Override
         public void run() {
            S3OutputStream.retry(new Runnable() {
               @Override
               public void run() {
                  throw new SdkClientException("Boom!");
               }
            }, 3, "Error!");
         }
      }, null);
      Executors.newFixedThreadPool(1).execute(futureTask);
      futureTask.get(1000, TimeUnit.MILLISECONDS);
   }
}
