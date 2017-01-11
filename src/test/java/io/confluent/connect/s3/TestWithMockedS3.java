/*
 * Copyright 2016 Confluent Inc.
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

package io.confluent.connect.s3;

import io.findify.s3mock.S3Mock;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Map;
import java.util.UUID;

public class TestWithMockedS3 extends S3SinkConnectorTestBase {

  protected S3Mock s3mock;
  protected String port;
  @Rule
  public TemporaryFolder s3mockRoot = new TemporaryFolder();

  protected static final String DM = "_";
  protected static final String FM = "#";

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    port = url.substring(url.lastIndexOf(":") + 1);
    File s3mockDir = s3mockRoot.newFolder("s3-tests-" + UUID.randomUUID().toString());
    System.out.println("Create folder: " + s3mockDir.getCanonicalPath());
    s3mock = S3Mock.create(Integer.parseInt(port), s3mockDir.getCanonicalPath());
    s3mock.start();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    s3mock.stop();
  }

}
