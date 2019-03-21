package io.confluent.connect.s3.format.orc;

import com.amazonaws.services.s3.AmazonS3;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.storage.S3OutputStream;

import java.io.IOException;

public class S3OutputStreamWrapper extends S3OutputStream {

  private boolean shouldBeCommitted;

  public S3OutputStreamWrapper(String key, S3SinkConnectorConfig conf, AmazonS3 s3) {
    super(key, conf, s3);
  }


  public void comiitBeforeClose() {
    shouldBeCommitted = true;
  }

  @Override
  public void close() throws IOException {
    if (shouldBeCommitted) {
      shouldBeCommitted=false;
      commit();
    } else {
      super.close();
    }
  }
}
