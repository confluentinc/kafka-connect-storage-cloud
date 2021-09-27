package io.confluent.connect.s3.util;

import static io.confluent.connect.s3.util.Utils.getAdjustedFilename;
import static org.junit.Assert.assertEquals;

import io.confluent.connect.s3.format.RecordViews.HeaderRecordView;
import io.confluent.connect.s3.format.RecordViews.KeyRecordView;
import io.confluent.connect.s3.format.RecordViews.ValueRecordView;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

public class UtilsTest {

  private static List<String> givenFilenames = new ArrayList<>(Arrays.asList(
      "x.avro",
      "asdf.avro",
      "keys.avro",
      "key.avro",
      "headers.avro",
      "header.avro",
      "sample-filename.avro",
      "sample.filename.avro",
      "sample.file.name.avro",
      "sample.file.avro.RawEvent.avro",
      "x",
      "avro",
      ".avro",
      "fooavrobar.txt"
  ));

  @Test
  public void getAdjustedFilenameForValues() {
    List<String> expectedFilenames = new ArrayList<>(Arrays.asList(
        "x.avro",
        "asdf.avro",
        "keys.avro",
        "key.avro",
        "headers.avro",
        "header.avro",
        "sample-filename.avro",
        "sample.filename.avro",
        "sample.file.name.avro",
        "sample.file.avro.RawEvent.avro",
        "x.avro",
        "avro.avro",
        ".avro",
        "fooavrobar.txt.avro"
    ));

    for (int i = 0; i < expectedFilenames.size(); i++) {
      String adjustedFilename = getAdjustedFilename(new ValueRecordView(), givenFilenames.get(i), ".avro");
      assertEquals(expectedFilenames.get(i), adjustedFilename);
    }
  }

  @Test
  public void getAdjustedFilenameForKeys() {
    List<String> expectedFilenames = new ArrayList<>(Arrays.asList(
        "x.keys.avro",
        "asdf.keys.avro",
        "keys.keys.avro",
        "key.keys.avro",
        "headers.keys.avro",
        "header.keys.avro",
        "sample-filename.keys.avro",
        "sample.filename.keys.avro",
        "sample.file.name.keys.avro",
        "sample.file.avro.RawEvent.keys.avro",
        "x.keys.avro",
        "avro.keys.avro",
        ".keys.avro",
        "fooavrobar.txt.keys.avro"
    ));

    for (int i = 0; i < expectedFilenames.size(); i++) {
      String adjustedFilename = getAdjustedFilename(new KeyRecordView(), givenFilenames.get(i), ".avro");
      assertEquals(expectedFilenames.get(i), adjustedFilename);
    }
  }

  @Test
  public void getAdjustedFilenameForHeaders() {
    List<String> expectedFilenames = new ArrayList<>(Arrays.asList(
        "x.headers.avro",
        "asdf.headers.avro",
        "keys.headers.avro",
        "key.headers.avro",
        "headers.headers.avro",
        "header.headers.avro",
        "sample-filename.headers.avro",
        "sample.filename.headers.avro",
        "sample.file.name.headers.avro",
        "sample.file.avro.RawEvent.headers.avro",
        "x.headers.avro",
        "avro.headers.avro",
        ".headers.avro",
        "fooavrobar.txt.headers.avro"
    ));

    for (int i = 0; i < expectedFilenames.size(); i++) {
      String adjustedFilename = getAdjustedFilename(new HeaderRecordView(), givenFilenames.get(i), ".avro");
      assertEquals(expectedFilenames.get(i), adjustedFilename);
    }
  }

  @Test
  public void testParquetEncodingExtensions() {
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
      String adjustedFilename = getAdjustedFilename(new HeaderRecordView(), givenFilenames1.get(i), ".snappy.parquet");
      assertEquals(expectedFilenames.get(i), adjustedFilename);
    }
  }
}
