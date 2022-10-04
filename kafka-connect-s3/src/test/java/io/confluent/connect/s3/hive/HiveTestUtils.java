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

import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.Partitioner;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Map;

public class HiveTestUtils {

  public static Partitioner getPartitioner(Map<String, Object> parsedConfig) {
    Partitioner partitioner = new DefaultPartitioner();
    partitioner.configure(parsedConfig);
    return partitioner;
  }

  public static String[] parseOutput(String output) {
    return output.replace(" ", "").split("\t");
  }

  public static String runHive(HiveExec hiveExec, String query) throws Exception {
    ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
    ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
    PrintStream outSaved = System.out;
    PrintStream errSaved = System.err;
    System.setOut(new PrintStream(outBytes, true));
    System.setErr(new PrintStream(errBytes, true));
    try {
      hiveExec.executeQuery(query);
    } finally {
      System.setOut(outSaved);
      System.setErr(errSaved);
    }
    ByteArrayInputStream outBytesIn = new ByteArrayInputStream(outBytes.toByteArray());
    ByteArrayInputStream errBytesIn = new ByteArrayInputStream(errBytes.toByteArray());
    BufferedReader is = new BufferedReader(new InputStreamReader(outBytesIn));
    BufferedReader es = new BufferedReader(new InputStreamReader(errBytesIn));
    StringBuilder output = new StringBuilder();
    String line;
    while ((line = is.readLine()) != null) {
      if (output.length() > 0) {
        output.append("\n");
      }
      output.append(line);
    }
    if (output.length() == 0) {
      output = new StringBuilder();
      while ((line = es.readLine()) != null) {
        output.append("\n");
        output.append(line);
      }
    }
    return output.toString();
  }
}
