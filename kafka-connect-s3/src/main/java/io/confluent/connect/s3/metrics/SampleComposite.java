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

package io.confluent.connect.s3.metrics;

import java.util.Collection;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;

public class SampleComposite implements CompositeData {

  @Override
  public CompositeType getCompositeType() {
    try {
      return new CompositeType(
          "SampleComposite",
          "Description for SampleComposite type",
          new String[]{"first", "second"},
          new String[]{"first item description", "second item description"},
          new OpenType[]{SimpleType.INTEGER, SimpleType.INTEGER}
      );
    } catch (OpenDataException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public Object get(String key) {
    return null;
  }

  @Override
  public Object[] getAll(String[] keys) {
    return new Object[0];
  }

  @Override
  public boolean containsKey(String key) {
    return false;
  }

  @Override
  public boolean containsValue(Object value) {
    return false;
  }

  @Override
  public Collection<?> values() {
    return null;
  }
}
