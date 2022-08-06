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

package io.confluent.connect.s3.util;

import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.http.conn.ssl.SdkTLSSocketFactory;

import java.io.FileInputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.storage.common.util.StringUtils;

import static io.confluent.connect.s3.S3SinkConnectorConfig.SSL_TRUSTSTORE_LOCATION_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.SSL_TRUSTSTORE_PASSWORD_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.SSL_TRUSTSTORE_TYPE_CONFIG;

public class TrustStoreConfig {
  private static final Logger log = LoggerFactory.getLogger(TrustStoreConfig.class);

  private final Path trustStoreLocation;
  private final String password;
  private final String type;

  public TrustStoreConfig(S3SinkConnectorConfig config) {
    try {
      trustStoreLocation = Paths.get(
          config.getString(SSL_TRUSTSTORE_LOCATION_CONFIG).trim());
      String typeValue = config.getString(SSL_TRUSTSTORE_TYPE_CONFIG);
      type = StringUtils.isNotBlank(typeValue)
          ? typeValue.toLowerCase().trim()
          : KeyStore.getDefaultType().toString();
      Password passwordValue = config.getPassword(SSL_TRUSTSTORE_PASSWORD_CONFIG);
      password = StringUtils.isNotBlank(passwordValue.value())
          ? passwordValue.value()
          : new Password(null).toString();
      log.debug("Using Custom trust store {}", this);
    } catch (Exception e) {
      throw new ConfigException(
          SSL_TRUSTSTORE_LOCATION_CONFIG,
          config.getString(SSL_TRUSTSTORE_LOCATION_CONFIG),
          e.toString());
    }
  }

  public ConnectionSocketFactory getSslSocketFactory() {
    try {
      char[] password = this.password.toCharArray();
      String keyStoreFile = this.trustStoreLocation.toAbsolutePath().toString();
      KeyStore keyStore = KeyStore.getInstance(this.type);

      try (FileInputStream fis = new FileInputStream(keyStoreFile)) {
        keyStore.load(fis, password);
      }
      TrustManagerFactory tmf;
      tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(keyStore);
      TrustManager[] tms = tmf.getTrustManagers();
      SSLContext sslContext = SSLContext.getInstance("TLS");
      sslContext.init(null, tms, null);
      SdkTLSSocketFactory sslSocketFactory = new SdkTLSSocketFactory(sslContext, null);
      return sslSocketFactory;
    } catch (Exception e) {
      throw new ConfigException(
          SSL_TRUSTSTORE_LOCATION_CONFIG,
          this.trustStoreLocation,
          e.toString());
    }
  }

  public Path trustStoreLocation() {
    return trustStoreLocation;
  }

  public String type() {
    return type;
  }

  public String password() {
    return password;
  }

  @Override
  public String toString() {
    return "TrustStoreConfig{"
        + "trustStoreLocation=" + trustStoreLocation.toAbsolutePath().toString()
        + ", type='" + type + '\''
        + ", password='*****************'"
        + '}';
  }
}
