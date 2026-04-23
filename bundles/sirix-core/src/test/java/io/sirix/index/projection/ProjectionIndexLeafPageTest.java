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

  @Test
  void wireKindsUpgradesNumericLongToForBp() {
    final ProjectionIndexLeafPage p = new ProjectionIndexLeafPage(KINDS_NUM_BOOL_STR);
    final byte[] wire = p.wireKinds();
    assertEquals(ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG_FOR_BP, wire[0]);
    assertEquals(ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN, wire[1]);
    assertEquals(ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT, wire[2]);
  }

  @Test
  void serializedSizeShrinksForNarrowNumericRange() {
    // 1024 rows of values in [0, 100] — bit width 7 should pack ~1024*7/8 ≈ 896
    // body bytes vs 1024*8 = 8192 for raw long storage. The recordKeys still
    // cost 8192 bytes, so total shrink is on the column body only; we verify
    // column-body budget directly.
    final byte[] numOnly = {ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG};
    final ProjectionIndexLeafPage p = new ProjectionIndexLeafPage(numOnly);
    final long[] nums = {0L};
    final boolean[] bools = {false};
    final String[] strs = {null};
    for (int i = 0; i < ProjectionIndexLeafPage.MAX_ROWS; i++) {
      nums[0] = i % 101L;
      assertTrue(p.appendRow(i, nums, bools, strs));
    }
    final byte[] serialised = p.serialize();
    // Budget: header (25) + recordKeys (8192) + column min/max (16) +
    // FOR-BP header (~11) + body (896) ≈ 9140 bytes.
    // Raw equivalent would be 25 + 8192 + 16 + 8192 = 16425 bytes.
    assertTrue(serialised.length < 9500,
        "FOR-BP serialised size should be ~9140: " + serialised.length);
    assertTrue(serialised.length < 16000,
        "FOR-BP must shrink vs raw " + serialised.length + " bytes");
    // Verify kind byte on wire is FOR-BP.
    assertEquals(ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG_FOR_BP, serialised[24]);
  }

  @Test
  void columnBodyBytesShrinkVsRaw() {
    // Direct body-size comparison: two identical pages except for encoding choice.
    // At 256 rows with range [0, 99], raw = 256*8 = 2048 bytes body;
    // FOR-BP ≈ 256 * 7 / 8 = 224 bytes body + ~11 byte header.
    final byte[] numOnly = {ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG};
    final ProjectionIndexLeafPage p = new ProjectionIndexLeafPage(numOnly);
    final long[] nums = {0L};
    final boolean[] bools = {false};
    final String[] strs = {null};
    for (int i = 0; i < 256; i++) {
      nums[0] = i % 100L;
      assertTrue(p.appendRow(i, nums, bools, strs));
    }
    final int size = p.serializedSize();
    // header(25) + recordKeys(256*8=2048) + colMinMax(16) + FOR-BP(~240)
    assertTrue(size < 2400, "FOR-BP serializedSize: " + size);
  }

  @Test
  void roundTripThroughForBpPreservesValues() {
    // Narrow-range values exercise the FOR-BP path; round-trip via
    // deserialize must reconstruct identical values.
    final byte[] numOnly = {ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG};
    final ProjectionIndexLeafPage p = new ProjectionIndexLeafPage(numOnly);
    final long[] nums = {0L};
    final boolean[] bools = {false};
    final String[] strs = {null};
    for (int i = 0; i < 100; i++) {
      nums[0] = 50L + (i * 7L);
      assertTrue(p.appendRow(i, nums, bools, strs));
    }
    final byte[] bytes = p.serialize();
    final ProjectionIndexLeafPage rt = ProjectionIndexLeafPage.deserialize(bytes);
    assertEquals(100, rt.getRowCount());
    final long[] col = rt.numericColumn(0);
    for (int i = 0; i < 100; i++) {
      assertEquals(50L + (i * 7L), col[i], "row " + i);
    }
  }

  @Test
  void constantRunProducesMinimalBody() {
    // All rows equal → bitWidth == 0, no body.
    final byte[] numOnly = {ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG};
    final ProjectionIndexLeafPage p = new ProjectionIndexLeafPage(numOnly);
    final long[] nums = {42L};
    final boolean[] bools = {false};
    final String[] strs = {null};
    for (int i = 0; i < 500; i++) assertTrue(p.appendRow(i, nums, bools, strs));
    final byte[] bytes = p.serialize();
    // Header (24 + 1 kind) + recordKeys (500*8) + min/max (16) + FOR header (~12 bytes)
    // should be much smaller than raw 500*8 = 4000 bytes column body.
    assertTrue(bytes.length < 4100, "constant-run should be compact: " + bytes.length);
    final ProjectionIndexLeafPage rt = ProjectionIndexLeafPage.deserialize(bytes);
    assertEquals(500, rt.getRowCount());
    final long[] col = rt.numericColumn(0);
    for (int i = 0; i < 500; i++) assertEquals(42L, col[i]);
  }

  @Test
  void negativeValuesRoundTripViaFrameOfReference() {
    // Values spanning negatives exercise the FOR base subtraction path.
    final byte[] numOnly = {ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG};
    final ProjectionIndexLeafPage p = new ProjectionIndexLeafPage(numOnly);
    final long[] nums = {0L};
    final boolean[] bools = {false};
    final String[] strs = {null};
    for (int i = 0; i < 200; i++) {
      nums[0] = -1000L + i;
      assertTrue(p.appendRow(i, nums, bools, strs));
    }
    final byte[] bytes = p.serialize();
    final ProjectionIndexLeafPage rt = ProjectionIndexLeafPage.deserialize(bytes);
    assertEquals(200, rt.getRowCount());
    assertEquals(-1000L, rt.columnMin(0));
    assertEquals(-801L, rt.columnMax(0));
    final long[] col = rt.numericColumn(0);
    for (int i = 0; i < 200; i++) assertEquals(-1000L + i, col[i]);
  }
}
