package io.confluent.connect.s3.util;

import io.confluent.connect.storage.schema.SchemaIncompatibilityType;
import org.junit.Assert;
import org.junit.Test;

public class FileRotationTrackerTest {

  private static final String FILE_1 = "file1";

  private static final String FILE_2 = "file2";

  private static final String FILE_3 = "file3";

  @Test
  public void testRotationIncrements() {
    FileRotationTracker fileRotationTracker = new FileRotationTracker();
    fileRotationTracker.incrementRotationByRotationIntervalCount(FILE_1);
    fileRotationTracker.incrementRotationByFlushSizeCount(FILE_2);
    fileRotationTracker
        .incrementRotationBySchemaChangeCount(FILE_3, SchemaIncompatibilityType.DIFFERENT_VERSION);
    fileRotationTracker.incrementRotationByFlushSizeCount(FILE_1);
    String actual = fileRotationTracker.toString();
    String expected = "OutputPartition: file2, RotationByInterval: 0, RotationByScheduledInterval: 0, RotationByFlushSize: 1, RotationByDiffName: 0, RotationByDiffSchema: 0, RotationByDiffType: 0, RotationByDiffVersion: 0, RotationByDiffParams: 0\n"
        + "OutputPartition: file3, RotationByInterval: 0, RotationByScheduledInterval: 0, RotationByFlushSize: 0, RotationByDiffName: 0, RotationByDiffSchema: 0, RotationByDiffType: 0, RotationByDiffVersion: 1, RotationByDiffParams: 0\n"
        + "OutputPartition: file1, RotationByInterval: 1, RotationByScheduledInterval: 0, RotationByFlushSize: 1, RotationByDiffName: 0, RotationByDiffSchema: 0, RotationByDiffType: 0, RotationByDiffVersion: 0, RotationByDiffParams: 0\n";
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testRotationCountClearance() {
    FileRotationTracker fileRotationTracker = new FileRotationTracker();
    fileRotationTracker.incrementRotationByRotationIntervalCount(FILE_1);
    fileRotationTracker.incrementRotationByFlushSizeCount(FILE_2);
    fileRotationTracker
        .incrementRotationBySchemaChangeCount(FILE_3, SchemaIncompatibilityType.DIFFERENT_VERSION);
    fileRotationTracker.incrementRotationByFlushSizeCount(FILE_1);
    String actual = fileRotationTracker.toString();
    String expected = "OutputPartition: file2, RotationByInterval: 0, RotationByScheduledInterval: 0, RotationByFlushSize: 1, RotationByDiffName: 0, RotationByDiffSchema: 0, RotationByDiffType: 0, RotationByDiffVersion: 0, RotationByDiffParams: 0\n"
        + "OutputPartition: file3, RotationByInterval: 0, RotationByScheduledInterval: 0, RotationByFlushSize: 0, RotationByDiffName: 0, RotationByDiffSchema: 0, RotationByDiffType: 0, RotationByDiffVersion: 1, RotationByDiffParams: 0\n"
        + "OutputPartition: file1, RotationByInterval: 1, RotationByScheduledInterval: 0, RotationByFlushSize: 1, RotationByDiffName: 0, RotationByDiffSchema: 0, RotationByDiffType: 0, RotationByDiffVersion: 0, RotationByDiffParams: 0\n";
    Assert.assertEquals(expected, actual);
    fileRotationTracker.clear();
    actual = fileRotationTracker.toString();
    Assert.assertEquals("", actual);
  }
}