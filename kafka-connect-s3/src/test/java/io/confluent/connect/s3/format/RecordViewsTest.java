package io.confluent.connect.s3.format;

import java.util.Date;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import io.confluent.connect.s3.format.RecordViews.HeaderRecordView;
import io.confluent.connect.s3.format.RecordViews.KeyRecordView;
import io.confluent.connect.s3.format.RecordViews.ValueRecordView;

import static io.confluent.connect.s3.format.RecordViews.HeaderRecordView.SINGLE_HEADER_SCHEMA;
import static org.junit.Assert.assertEquals;

public class RecordViewsTest {

  @Test
  public void testToString() {
    assertEquals("ValueRecordView", new ValueRecordView().toString());
    assertEquals("KeyRecordView", new KeyRecordView().toString());
    assertEquals("HeaderRecordView", new HeaderRecordView().toString());
  }

  @Test
  public void testGetExtension() {
    assertEquals("", new ValueRecordView().getExtension());
    assertEquals(".keys", new KeyRecordView().getExtension());
    assertEquals(".headers", new HeaderRecordView().getExtension());
  }

  @Test
  public void testRecordKeyValueViews() {
    SinkRecord sampleRecord = getSampleSinkRecord();
    assertEquals(sampleRecord.key(), new KeyRecordView().getView(sampleRecord, false));
    assertEquals(sampleRecord.value(), new ValueRecordView().getView(sampleRecord, false));
    assertEquals(sampleRecord.keySchema(), new KeyRecordView().getViewSchema(sampleRecord, false));
    assertEquals(sampleRecord.valueSchema(), new ValueRecordView().getViewSchema(sampleRecord, false));
  }

  @Test
  public void testHeaderConvertsToString() {
    List<Struct> expectedHeaders = Arrays.asList(
        new Struct(SINGLE_HEADER_SCHEMA)
            .put("key", "string").put("value", "string"),
        new Struct(SINGLE_HEADER_SCHEMA).put("key", "int").put("value", "12"),
        new Struct(SINGLE_HEADER_SCHEMA).put("key", "boolean").put("value", "false"));

    Object headerView = new HeaderRecordView().getView(getSampleSinkRecord(), false);
    assertEquals(expectedHeaders, headerView);
  }

  private SinkRecord getSampleSinkRecord() {
    Headers headers = new ConnectHeaders()
        .addString("string", "string")
        .addInt("int", 12)
        .addBoolean("boolean", false);

    Schema keyschema = getExampleKeyStructSchema();
    Schema valueschema = getExampleValueStructSchema();

    return new SinkRecord(
        "topic", 0,
        keyschema, getExampleKeyStruct(keyschema),
        valueschema, getExampleValueStruct(valueschema),
        0, 0L, TimestampType.NO_TIMESTAMP_TYPE, headers);
  }

  private Schema getExampleKeyStructSchema() {
    return SchemaBuilder.struct()
        .field("recordKeyID", Schema.INT64_SCHEMA)
        .field("keyString", Schema.STRING_SCHEMA)
        .field("keyBytes", Schema.BYTES_SCHEMA)
        .build();
  }

  private Struct getExampleKeyStruct(Schema structSchema) {
    Date sampleDate = new Date(1111111);
    sampleDate.setTime(0);
    return new Struct(structSchema)
        .put("recordKeyID", (long) 0)
        .put("keyString", "theStringVal")
        .put("keyBytes", "theBytes".getBytes());
  }

  private Schema getExampleValueStructSchema() {
    return SchemaBuilder.struct()
        .field("ID", Schema.INT64_SCHEMA)
        .field("myString", Schema.STRING_SCHEMA)
        .field("myBool", Schema.BOOLEAN_SCHEMA)
        .field("myBytes", Schema.BYTES_SCHEMA)
        .field("myDate", org.apache.kafka.connect.data.Date.SCHEMA)
        .field("myTime", Timestamp.SCHEMA)
        .build();
  }

  private Struct getExampleValueStruct(Schema structSchema) {
    Date sampleDate = new Date(1111111);
    sampleDate.setTime(0);
    return new Struct(structSchema)
        .put("ID", (long) 0)
        .put("myString", "theStringVal")
        .put("myBool", true)
        .put("myBytes", "theBytes".getBytes())
        .put("myDate", sampleDate)
        .put("myTime", new Date(33333333));
  }
}
