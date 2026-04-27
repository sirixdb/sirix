/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round-trip tests for {@link ProjectionIndexLeafPage} — append rows via
 * the writer API, serialize, deserialize, and verify the reader sees the
 * same cells. Exercises all three column kinds in one page.
 */
final class ProjectionIndexLeafPageTest {

  private static final byte[] KINDS_NUM_BOOL_STR = {
      ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG,
      ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN,
      ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT
  };

  @Test
  void emptyPageRoundTrips() {
    final ProjectionIndexLeafPage p = new ProjectionIndexLeafPage(KINDS_NUM_BOOL_STR);
    final byte[] bytes = p.serialize();
    final ProjectionIndexLeafPage rt = ProjectionIndexLeafPage.deserialize(bytes);
    assertEquals(0, rt.getRowCount());
    assertEquals(3, rt.getColumnCount());
    assertEquals(Long.MAX_VALUE, rt.firstRecordKey());
    assertEquals(Long.MIN_VALUE, rt.lastRecordKey());
  }

  @Test
  void fourRowsRoundTripAllColumnKinds() {
    final ProjectionIndexLeafPage p = new ProjectionIndexLeafPage(KINDS_NUM_BOOL_STR);
    final String[] depts = {"Eng", "Sales", "Eng", "Ops"};
    for (int i = 0; i < 4; i++) {
      final long[] nums = {40 + i, 0, 0};
      final boolean[] bools = {false, i % 2 == 0, false};
      final String[] strs = {null, null, depts[i]};
      assertTrue(p.appendRow(1000L + i, nums, bools, strs));
    }
    assertEquals(4, p.getRowCount());

    final byte[] bytes = p.serialize();
    final ProjectionIndexLeafPage rt = ProjectionIndexLeafPage.deserialize(bytes);

    assertEquals(4, rt.getRowCount());
    assertEquals(3, rt.getColumnCount());
    assertEquals(1000L, rt.firstRecordKey());
    assertEquals(1003L, rt.lastRecordKey());
    assertArrayEquals(new long[] {1000, 1001, 1002, 1003},
        java.util.Arrays.copyOf(rt.recordKeys(), 4));

    // Numeric column: values 40-43, min=40, max=43.
    assertEquals(40L, rt.columnMin(0));
    assertEquals(43L, rt.columnMax(0));
    final long[] numCol = rt.numericColumn(0);
    for (int i = 0; i < 4; i++) assertEquals(40L + i, numCol[i], "row " + i);

    // Boolean column: rows 0 and 2 are true, others false.
    final long[] bits = rt.booleanColumnBits(1);
    assertTrue((bits[0] & 1L) != 0, "row 0 should be true");
    assertFalse((bits[0] & 2L) != 0, "row 1 should be false");
    assertTrue((bits[0] & 4L) != 0, "row 2 should be true");
    assertFalse((bits[0] & 8L) != 0, "row 3 should be false");

    // String column: dict-ids match insertion order (Eng=0, Sales=1, Ops=2);
    // row 2 reuses "Eng" → dictId 0.
    final int[] ids = rt.stringDictIdColumn(2);
    assertEquals(0, ids[0]);
    assertEquals(1, ids[1]);
    assertEquals(0, ids[2]);
    assertEquals(2, ids[3]);

    final byte[][] dict = rt.stringDictionary(2);
    assertArrayEquals("Eng".getBytes(), dict[0]);
    assertArrayEquals("Sales".getBytes(), dict[1]);
    assertArrayEquals("Ops".getBytes(), dict[2]);
  }

  @Test
  void appendRowReturnsFalseAtCapacity() {
    final byte[] numOnly = {ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG};
    final ProjectionIndexLeafPage p = new ProjectionIndexLeafPage(numOnly);
    final long[] nums = {42};
    final boolean[] bools = {false};
    final String[] strs = {null};
    for (int i = 0; i < ProjectionIndexLeafPage.MAX_ROWS; i++) {
      assertTrue(p.appendRow(i, nums, bools, strs), "row " + i);
    }
    assertFalse(p.appendRow(ProjectionIndexLeafPage.MAX_ROWS, nums, bools, strs));
  }
}
