/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.gcs.util;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URL;
import java.util.Locale;

import io.confluent.connect.gcs.GcsSinkConnectorConfig;
import io.confluent.connect.storage.common.util.StringUtils;

import static io.confluent.connect.gcs.GcsSinkConnectorConfig.GCS_PROXY_PASS_CONFIG;
import static io.confluent.connect.gcs.GcsSinkConnectorConfig.GCS_PROXY_URL_CONFIG;
import static io.confluent.connect.gcs.GcsSinkConnectorConfig.GCS_PROXY_USER_CONFIG;

public class GcsProxyConfig {
  private final Proxy.Type protocol;
  private final String host;
  private final int port;
  private final String user;
  private final String pass;

  public GcsProxyConfig(GcsSinkConnectorConfig config) {
    try {
      URL url = new URL(config.getString(GCS_PROXY_URL_CONFIG));
      protocol = extractProtocol(url.getProtocol());
      host = url.getHost();
      port = url.getPort();
      String username = config.getString(GCS_PROXY_USER_CONFIG);
      user = StringUtils.isNotBlank(username)
             ? username
             : extractUser(url.getUserInfo());
      Password password = config.getPassword(GCS_PROXY_PASS_CONFIG);
      pass = StringUtils.isNotBlank(password.value())
             ? password.value()
             : extractPass(url.getUserInfo());
    } catch (MalformedURLException e) {
      throw new ConfigException(
          GCS_PROXY_URL_CONFIG,
          config.getString(GCS_PROXY_URL_CONFIG),
          e.toString()
      );
    }
  }

  public static Proxy.Type extractProtocol(String protocol) {
    if (StringUtils.isBlank(protocol)) {
      return Proxy.Type.HTTP;
    }
    switch (protocol.trim().toLowerCase(Locale.ROOT)) {
      case "direct":
        return Proxy.Type.DIRECT;
      case "socks":
        return Proxy.Type.SOCKS;
      case "https":
      default:
        return Proxy.Type.HTTP;
    }
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

  public Proxy.Type protocol() {
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
}
