package io.confluent.connect.s3.format;

import static org.junit.Assert.assertEquals;

import io.confluent.connect.s3.format.RecordViews.HeaderRecordView;
import io.confluent.connect.s3.format.RecordViews.KeyRecordView;
import io.confluent.connect.s3.format.RecordViews.ValueRecordView;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

public class RecordViewSetterTest {

  private static List<String> givenFilenames = new ArrayList<>(Arrays.asList(
      "x.avro",
      "asdf.avro",
      "keys.avro",
      "key.avro",
      "headers.avro",
      "header.avro",
      "sample-filename.avro",
      "sample.filename.avro",
      "sample.file.name.avro"
  ));

  @Test
  public void getAdjustedFilenameForValues() {
    RecordViewSetter rv = new RecordViewSetter();
    rv.setRecordView(new ValueRecordView());

    List<String> expectedFilenames = new ArrayList<>(Arrays.asList(
        "x.avro",
        "asdf.avro",
        "keys.avro",
        "key.avro",
        "headers.avro",
        "header.avro",
        "sample-filename.avro",
        "sample.filename.avro",
        "sample.file.name.avro"
    ));

    for (int i = 0; i < expectedFilenames.size(); i++) {
      String adjustedFilename = rv.getAdjustedFilename(givenFilenames.get(i), ".avro");
      assertEquals(expectedFilenames.get(i), adjustedFilename);
    }
  }

  @Test
  public void getAdjustedFilenameForKeys() {
    RecordViewSetter rv = new RecordViewSetter();
    rv.setRecordView(new KeyRecordView());

    List<String> expectedFilenames = new ArrayList<>(Arrays.asList(
        "x.keys.avro",
        "asdf.keys.avro",
        "keys.keys.avro",
        "key.keys.avro",
        "headers.keys.avro",
        "header.keys.avro",
        "sample-filename.keys.avro",
        "sample.filename.keys.avro",
        "sample.file.name.keys.avro"
    ));

    for (int i = 0; i < expectedFilenames.size(); i++) {
      String adjustedFilename = rv.getAdjustedFilename(givenFilenames.get(i), ".avro");
      assertEquals(expectedFilenames.get(i), adjustedFilename);
    }
  }

  @Test
  public void getAdjustedFilenameForHeaders() {
    RecordViewSetter rv = new RecordViewSetter();
    rv.setRecordView(new HeaderRecordView());

    List<String> expectedFilenames = new ArrayList<>(Arrays.asList(
        "x.headers.avro",
        "asdf.headers.avro",
        "keys.headers.avro",
        "key.headers.avro",
        "headers.headers.avro",
        "header.headers.avro",
        "sample-filename.headers.avro",
        "sample.filename.headers.avro",
        "sample.file.name.headers.avro"
    ));

    for (int i = 0; i < expectedFilenames.size(); i++) {
      String adjustedFilename = rv.getAdjustedFilename(givenFilenames.get(i), ".avro");
      assertEquals(expectedFilenames.get(i), adjustedFilename);
    }
  }

  @Test
  public void testParquetEncodingExtensions() {
    RecordViewSetter rv = new RecordViewSetter();
    rv.setRecordView(new HeaderRecordView());

    List<String> givenFilenames1 = new ArrayList<>(Arrays.asList(
        "x.snappy.parquet",
        "sample-filename.snappy.parquet",
        "sample.filename.snappy.parquet",
        "sample.file.name.snappy.parquet"
    ));

    List<String> expectedFilenames = new ArrayList<>(Arrays.asList(
        "x.headers.snappy.parquet",
        "sample-filename.headers.snappy.parquet",
        "sample.filename.headers.snappy.parquet",
        "sample.file.name.headers.snappy.parquet"
    ));

    for (int i = 0; i < expectedFilenames.size(); i++) {
      String adjustedFilename = rv.getAdjustedFilename(givenFilenames1.get(i), ".snappy.parquet");
      assertEquals(expectedFilenames.get(i), adjustedFilename);
    }
  }

}
