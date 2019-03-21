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

package io.confluent.connect.s3.format.orc;

import io.confluent.connect.s3.storage.S3OutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

class S3OrcFileSystem extends FileSystem {

  S3OutputStream s3OutputStream;

  public S3OrcFileSystem(S3OutputStream s3OutputStream) {
    this.s3OutputStream = s3OutputStream;
  }

  @Override
  public URI getUri() {
    throw new UnsupportedOperationException();
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FSDataOutputStream create(Path f,
                                   FsPermission permission, boolean overwrite,
                                   int bufferSize, short replication,
                                   long blockSize, Progressable progress) throws IOException {

    return new FSDataOutputStream(s3OutputStream, null);
  }

  @Override
  public void setPermission(Path p, FsPermission permission) throws IOException {
    if (System.getProperty("os.name").startsWith("Windows")) {
      //do nothing
    } else {
      super.setPermission(p, permission);
    }
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress
  ) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setWorkingDirectory(Path newDir) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Path getWorkingDirectory() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    throw new UnsupportedOperationException();
  }
}
