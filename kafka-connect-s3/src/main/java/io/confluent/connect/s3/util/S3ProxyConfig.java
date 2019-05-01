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

import com.amazonaws.Protocol;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Locale;

import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.storage.common.util.StringUtils;

import static io.confluent.connect.s3.S3SinkConnectorConfig.S3_PROXY_PASS_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.S3_PROXY_URL_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.S3_PROXY_USER_CONFIG;

public class S3ProxyConfig {
  private static final Logger log = LoggerFactory.getLogger(S3ProxyConfig.class);

  private final Protocol protocol;
  private final String host;
  private final int port;
  private final String user;
  private final String pass;

  public S3ProxyConfig(S3SinkConnectorConfig config) {
    try {
      URL url = new URL(config.getString(S3_PROXY_URL_CONFIG));
      protocol = extractProtocol(url.getProtocol());
      host = url.getHost();
      port = url.getPort();
      String username = config.getString(S3_PROXY_USER_CONFIG);
      user = StringUtils.isNotBlank(username)
             ? username
             : extractUser(url.getUserInfo());
      Password password = config.getPassword(S3_PROXY_PASS_CONFIG);
      pass = StringUtils.isNotBlank(password.value())
             ? password.value()
             : extractPass(url.getUserInfo());
      log.info("Using proxy config {}", this);
    } catch (MalformedURLException e) {
      throw new ConfigException(
          S3_PROXY_URL_CONFIG,
          config.getString(S3_PROXY_URL_CONFIG),
          e.toString()
      );
    }
  }

  public static Protocol extractProtocol(String protocol) {
    if (StringUtils.isBlank(protocol)) {
      return Protocol.HTTPS;
    }
    return "http".equals(protocol.trim().toLowerCase(Locale.ROOT)) ? Protocol.HTTP : Protocol.HTTPS;
  }

  public static String extractUser(String userInfo) {
    return StringUtils.isBlank(userInfo) ? null : userInfo.split(":")[0];
  }

  public static String extractPass(String userInfo) {
    if (StringUtils.isBlank(userInfo)) {
      return null;
    }

    String[] parts = userInfo.split(":", 2);
    return parts.length == 2 ? parts[1] : null;
  }

  public Protocol protocol() {
    return protocol;
  }

  public String host() {
    return host;
  }

  public int port() {
    return port;
  }

  public String user() {
    return user;
  }

  public String pass() {
    return pass;
  }

  @Override
  public String toString() {
    return "S3ProxyConfig{"
        + "protocol=" + protocol
        + ", host='" + host + '\''
        + ", port=" + port
        + ", user='" + user + '\''
        + ", pass='" + pass + '\''
        + '}';
  }
}
