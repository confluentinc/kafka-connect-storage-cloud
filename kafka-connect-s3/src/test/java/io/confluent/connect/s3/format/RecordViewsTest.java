package io.confluent.connect.s3.format;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Struct;
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
  public void testHeaderConvertsToString() {
    Headers headers = new ConnectHeaders()
        .addString("string", "string")
        .addInt("int", 12)
        .addBoolean("boolean", false);

    List<Struct> expected = Arrays.asList(
        new Struct(SINGLE_HEADER_SCHEMA)
            .put("key", "string").put("value", "string"),
        new Struct(SINGLE_HEADER_SCHEMA).put("key", "int").put("value", "12"),
        new Struct(SINGLE_HEADER_SCHEMA).put("key", "boolean").put("value", "false"));

    SinkRecord record = new SinkRecord(
        "topic", 0, null, null, null, null, 0, 0L, TimestampType.NO_TIMESTAMP_TYPE, headers);

    Object headerView = new HeaderRecordView().getView(record);
    assertEquals(expected, headerView);
  }
}
