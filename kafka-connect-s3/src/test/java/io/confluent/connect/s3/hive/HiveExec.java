/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.connect.s3.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.cli.CliDriver;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class HiveExec {

  public static final Log log = LogFactory.getLog(HiveExec.class);
  private HiveConf hiveConf;
  private CliDriver cliDriver;
  private static final String HIVE_SASL_ENABLED = "hive.metastore.sasl.enabled";

  /**
   * HiveExec constructor
   * @param config S3 Connector configuration
   */
  public HiveExec(final HiveConf hiveConf) {
    this.hiveConf = hiveConf;
    SessionState.start(new CliSessionState(hiveConf));
    cliDriver = new CliDriver();
  }

  /**
   * Execute a Hive query.
   * @param query The query to execute in Hive
   * @throws IOException
   */
  public void executeQuery(String query) throws IOException {
    try {
      log.debug("Using in-process Hive instance.");
      String[] argv = getHiveArgs(
          "-S",
//          "--hiveconf", "hive.log.level=TRACE",
//          "--hiveconf", "hive.root.logger=console",
          "--hiveconf", "fs.s3a.endpoint=http://127.0.0.1:8181",
          "--hiveconf", "fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
          "--hiveconf", "fs.s3a.path.style.access=true",
          "--hiveconf", "fs.s3a.connection.ssl.enabled=false",
          "--hiveconf", "fs.s3a.change.detection.version.required=false",
          "-e", query
      );

      int status = cliDriver.run(argv);
      if (status != 0) {
        throw new IOException("Hive CliDriver exited with status=" + status);
      }
    } catch (Exception e) {
      throw new IOException("Exception thrown in Hive", e);
    }
  }


  private String[] getHiveArgs(String... args) {
    List<String> newArgs = new LinkedList<>();
    newArgs.addAll(Arrays.asList(args));
    if (hiveConf.getBoolean(HIVE_SASL_ENABLED, false)) {
      newArgs.add("--hiveconf");
      newArgs.add("hive.metastore.sasl.enabled=true");
    }
    return newArgs.toArray(new String[newArgs.size()]);
  }
}
