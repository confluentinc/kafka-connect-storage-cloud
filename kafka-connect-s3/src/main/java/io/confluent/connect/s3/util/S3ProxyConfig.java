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

package io.confluent.connect.s3.util;

import com.amazonaws.Protocol;
import org.apache.kafka.common.config.ConfigException;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Locale;

import io.confluent.connect.storage.common.util.StringUtils;

public class S3ProxyConfig {
  private final Protocol protocol;
  private final String host;
  private final int port;
  private final String user;
  private final String pass;

  public S3ProxyConfig(String settingsUrl) {
    try {
      URL url = new URL(settingsUrl);
      protocol = extractProtocol(url.getProtocol());
      host = url.getHost();
      port = url.getPort();
      user = extractUser(url.getUserInfo());
      pass = extractPass(url.getUserInfo());
    } catch (MalformedURLException e) {
      throw new ConfigException("msg");
    }
  }

  public static Protocol extractProtocol(String protocol) {
    if (StringUtils.isBlank(protocol)) {
      return Protocol.HTTPS;
    }
    return "http".equals(protocol.trim().toLowerCase(Locale.ROOT)) ? Protocol.HTTP : Protocol.HTTPS;
  }

  public static String extractUser(String userInfo) {
    return StringUtils.isBlank(userInfo) ? "" : userInfo.split(":")[0];
  }

  public static String extractPass(String userInfo) {
    if (StringUtils.isBlank(userInfo)) {
      return "";
    }

    String[] parts = userInfo.split(":", 2);
    return parts.length == 2 ? parts[1] : "";
  }

  public String protocol() {
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
