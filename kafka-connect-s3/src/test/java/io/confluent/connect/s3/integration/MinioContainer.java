package io.confluent.connect.s3.integration;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;

import java.time.Duration;
import java.util.Arrays;

public class MinioContainer extends GenericContainer<MinioContainer> {

  private static final String DEFAULT_DOCKER_IMAGE = "minio/minio:latest";

  private static final String HEALTH_ENDPOINT = "/minio/health/ready";

  private static final int DEFAULT_SERVER_PORT = 9000;

  private static final int DEFAULT_CONSOLE_PORT = 9001;

  // Must be used as AWS_ACCESS_KEY and AWS_SECRET_KEY in AWS S3 Client
  public static final String MINIO_USERNAME = "minioadmin";

  public static final String MINIO_PASSWORD = "minioadmin";

  public MinioContainer() {
    this(DEFAULT_DOCKER_IMAGE);
  }

  public MinioContainer(String dockerImageName) {
    super(dockerImageName);
    this.logger().info("Starting an Minio container using [{}]", dockerImageName);
    this.setPortBindings(Arrays.asList(String.format("%d:%d", DEFAULT_SERVER_PORT, DEFAULT_SERVER_PORT),
            String.format("%d:%d", DEFAULT_CONSOLE_PORT, DEFAULT_CONSOLE_PORT)));
    this.withCommand(String.format("server /data --address :%d --console-address :%d",
            DEFAULT_SERVER_PORT, DEFAULT_CONSOLE_PORT));
    setWaitStrategy(new HttpWaitStrategy()
            .forPort(DEFAULT_SERVER_PORT)
            .forPath(HEALTH_ENDPOINT)
            .withStartupTimeout(Duration.ofMinutes(2)));
  }

  public String getUrl() {
    return String.format("http://%s:%s", this.getHost(), this.getMappedPort(DEFAULT_SERVER_PORT));
  }
}
