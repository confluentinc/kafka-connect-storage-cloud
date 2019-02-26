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

package io.confluent.connect.s3;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.SchemaProjectorException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import io.confluent.connect.storage.schema.StorageSchemaCompatibility;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SchemaCompatibilityTest extends S3SinkConnectorTestBase {

  String key;
  Schema schema;
  Struct record;
  Schema newSchema;
  Struct newRecord;
  Schema schemaNoVersion;
  Struct recordNoVersion;

  private Map<String, StorageSchemaCompatibility> validModes;

  @Before
  public void setUp() throws Exception {
    super.setUp();

    validModes = new HashMap<>();
    validModes.put("NONE", StorageSchemaCompatibility.NONE);
    validModes.put("BACKWARD", StorageSchemaCompatibility.BACKWARD);
    validModes.put("FORWARD", StorageSchemaCompatibility.FORWARD);
    validModes.put("FULL", StorageSchemaCompatibility.FULL);

    key = "key";
    schema = createSchema();
    record = createRecord(schema);
    newSchema = createNewSchema();
    newRecord = createNewRecord(newSchema);
    schemaNoVersion = createSchemaNoVersion();
    recordNoVersion = createRecord(createSchemaNoVersion());
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Test
  public void testCompatibilityModes() throws Exception {
    setUp();

    for(Map.Entry<String, StorageSchemaCompatibility> expected : validModes.entrySet()) {
      assertEquals(expected.getValue(), StorageSchemaCompatibility.getCompatibility(expected.getKey()));
    }
    assertEquals(validModes.size(), StorageSchemaCompatibility.values().length);

    assertEquals(StorageSchemaCompatibility.NONE, StorageSchemaCompatibility.getCompatibility(null));
  }

  @Test
  public void testChangingSchemaValidations() throws Exception {
    setUp();

    for (StorageSchemaCompatibility mode : validModes.values()) {
      SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, 16);
      SinkRecord newSinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, 16);
      SinkRecord sinkRecordNoVersion =
          new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schemaNoVersion, recordNoVersion, 16);
      SinkRecord sinkRecordNoSchema = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, null, record, 16);

      // Test when both schemas are null
      assertFalse(mode.shouldChangeSchema(sinkRecordNoSchema, null, null));

      try {
        mode.shouldChangeSchema(sinkRecord, null, null);
        fail("Expected " + SchemaProjectorException.class.getName() + " to be thrown.");
      } catch (SchemaProjectorException spe) {
        // success
      } catch (Exception e) {
        fail("Expected " + SchemaProjectorException.class.getName() + " to be thrown.");
      }

      // Test when one of the schema versions is null
      switch (mode) {
        case BACKWARD:
        case FULL:
        case FORWARD:
          try {
            mode.shouldChangeSchema(sinkRecordNoVersion, null, sinkRecord.valueSchema());
            fail("Expected " + SchemaProjectorException.class.getName() + " to be thrown.");
          } catch (SchemaProjectorException spe) {
            // success
          } catch (Exception e) {
            fail("Expected " + SchemaProjectorException.class.getName() + " to be thrown.");
          }
          break;
        case NONE:
          assertTrue(mode.shouldChangeSchema(sinkRecordNoVersion, null, sinkRecord.valueSchema()));
          assertFalse(mode.shouldChangeSchema(sinkRecordNoVersion, null, sinkRecordNoVersion.valueSchema()));
      }
    }
  }

  @Test
  public void testChangingSchema() throws Exception {
    setUp();

    for (StorageSchemaCompatibility mode : validModes.values()) {
      SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, 16);
      SinkRecord newSinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, newSchema, newRecord, 16);
      SinkRecord sinkRecordNoVersion =
          new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schemaNoVersion, recordNoVersion, 16);
      SinkRecord sinkRecordNoSchema = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, null, record, 16);

      // Test when one of the schema versions is null
      switch (mode) {
        case BACKWARD:
        case FULL:
          // new record has higher version
          assertTrue(mode.shouldChangeSchema(newSinkRecord, null, sinkRecord.valueSchema()));
          // new record has lower version
          assertFalse(mode.shouldChangeSchema(sinkRecord, null, newSinkRecord.valueSchema()));
          // Same
          assertFalse(mode.shouldChangeSchema(sinkRecord, null, sinkRecord.valueSchema()));
          break;
        case FORWARD:
          // new record has higher version
          assertFalse(mode.shouldChangeSchema(newSinkRecord, null, sinkRecord.valueSchema()));
          // new record has lower version
          assertTrue(mode.shouldChangeSchema(sinkRecord, null, newSinkRecord.valueSchema()));
          // Same
          assertFalse(mode.shouldChangeSchema(sinkRecord, null, sinkRecord.valueSchema()));
          break;
        case NONE:
        default:
          // new record has higher version
          assertTrue(mode.shouldChangeSchema(newSinkRecord, null, sinkRecord.valueSchema()));
          // new record has lower version
          assertTrue(mode.shouldChangeSchema(sinkRecord, null, newSinkRecord.valueSchema()));
          // Same
          assertFalse(mode.shouldChangeSchema(sinkRecord, null, sinkRecord.valueSchema()));
      }
    }
  }
}

