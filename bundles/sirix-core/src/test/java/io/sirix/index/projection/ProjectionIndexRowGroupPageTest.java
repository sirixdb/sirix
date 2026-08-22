/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Round-trip tests for {@link ProjectionIndexRowGroupPage} — append rows via
 * the writer API, serialize, deserialize, and verify the reader sees the
 * same cells. Exercises all three column kinds in one page.
 */
final class ProjectionIndexRowGroupPageTest {

  private static final byte[] KINDS_NUM_BOOL_STR = {
      ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
      ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN,
      ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT
  };

  private static boolean appendNumericRow(final ProjectionIndexRowGroupPage page, final long recordKey,
      final long value, final boolean orderException) {
    return page.appendExtractedUtf8Row(recordKey, new long[] {value}, new boolean[1], new byte[1][], new int[1],
        new String[1][], new boolean[] {true}, new boolean[1], new boolean[1], new boolean[1], orderException);
  }

  private static boolean appendNumericRow(final ProjectionIndexRowGroupPage page, final long recordKey,
      final long value, final byte[] orderLabel) {
    return page.appendExtractedUtf8Row(recordKey, new long[] {value}, new boolean[1], new byte[1][], new int[1],
        new String[1][], new boolean[] {true}, new boolean[1], new boolean[1], new boolean[1], false,
        orderLabel);
  }

  private static int orderMarkerOffset(final ProjectionIndexRowGroupPage page) {
    return 24 + page.getColumnCount() + page.getRowCount() * Long.BYTES;
  }

  @Test
  void emptyPageRoundTrips() {
    final ProjectionIndexRowGroupPage p = new ProjectionIndexRowGroupPage(KINDS_NUM_BOOL_STR);
    final byte[] bytes = p.serialize();
    final ProjectionIndexRowGroupPage rt = ProjectionIndexRowGroupPage.deserialize(bytes);
    assertEquals(0, rt.getRowCount());
    assertEquals(3, rt.getColumnCount());
    assertEquals(Long.MAX_VALUE, rt.firstRecordKey());
    assertEquals(Long.MIN_VALUE, rt.lastRecordKey());
  }

  @Test
  void longOrderLabelsRollOverBeforeAppendingTheRow() {
    final byte[] kinds = {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG};
    ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(kinds);
    ProjectionIndexRowGroupPage fullPage = null;
    for (int row = 1; row <= ProjectionIndexRowGroupPage.MAX_ROWS; row++) {
      final byte[] orderLabel = new byte[257];
      ByteBuffer.wrap(orderLabel).putInt(row);
      final int rowsBeforeAppend = page.getRowCount();
      if (!appendNumericRow(page, row, row, orderLabel)) {
        assertEquals(ProjectionIndexRowGroupPage.MAX_ORDER_LABEL_BYTES / orderLabel.length,
            rowsBeforeAppend);
        assertEquals(rowsBeforeAppend, page.getRowCount());
        fullPage = page;
        page = new ProjectionIndexRowGroupPage(kinds);
        assertTrue(appendNumericRow(page, row, row, orderLabel));
      }
    }

    assertNotNull(fullPage);
    assertEquals(ProjectionIndexRowGroupPage.MAX_ROWS,
        fullPage.getRowCount() + page.getRowCount());
    assertEquals(fullPage.getRowCount(),
        ProjectionIndexRowGroupPage.deserialize(fullPage.serialize()).getRowCount());
    assertEquals(page.getRowCount(), ProjectionIndexRowGroupPage.deserialize(page.serialize()).getRowCount());
  }

  @Test
  void shortOrderLabelRollsOverBeforeItsSyntheticAppend() {
    final byte[] kinds = {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG};
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(kinds);
    for (int row = 0; row < ProjectionIndexRowGroupPage.MAX_ROWS - 1; row++) {
      final byte[] orderLabel = new byte[row < 254 ? 257 : 256];
      ByteBuffer.wrap(orderLabel).putInt(row);
      assertTrue(appendNumericRow(page, row, row, orderLabel));
    }

    assertEquals(ProjectionIndexRowGroupPage.MAX_ORDER_LABEL_BYTES - 2, page.orderLabelLength());
    assertFalse(appendNumericRow(page, ProjectionIndexRowGroupPage.MAX_ROWS,
        ProjectionIndexRowGroupPage.MAX_ROWS, new byte[] {1}));
    assertEquals(ProjectionIndexRowGroupPage.MAX_ROWS - 1, page.getRowCount());
  }

  @Test
  void fourRowsRoundTripAllColumnKinds() {
    final ProjectionIndexRowGroupPage p = new ProjectionIndexRowGroupPage(KINDS_NUM_BOOL_STR);
    final String[] depts = {"Eng", "Sales", "Eng", "Ops"};
    for (int i = 0; i < 4; i++) {
      final long[] nums = {40 + i, 0, 0};
      final boolean[] bools = {false, i % 2 == 0, false};
      final String[] strs = {null, null, depts[i]};
      assertTrue(p.appendRow(1000L + i, nums, bools, strs));
    }
    assertEquals(4, p.getRowCount());

    final byte[] bytes = p.serialize();
    final ProjectionIndexRowGroupPage rt = ProjectionIndexRowGroupPage.deserialize(bytes);

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
  void noneOrderMetadataIsLazyAndRoundTrips() {
    final ProjectionIndexRowGroupPage page =
        new ProjectionIndexRowGroupPage(new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG});
    assertTrue(page.appendRow(2L, new long[] {10L}, new boolean[1], new String[1]));
    assertTrue(page.appendRow(5L, new long[] {20L}, new boolean[1], new String[1]));
    assertTrue(page.appendRow(8L, new long[] {30L}, new boolean[1], new String[1]));

    assertFalse(page.hasOrderExceptions());
    assertNull(page.orderExceptionBits(), "normal-only leaves must not allocate an exception bitmap");
    final byte[] raw = page.serialize();
    assertEquals(ProjectionIndexRowGroupPage.ORDER_EXCEPTIONS_NONE, raw[orderMarkerOffset(page)]);

    final ProjectionIndexRowGroupPage restored = ProjectionIndexRowGroupPage.deserialize(raw);
    assertFalse(restored.hasOrderExceptions());
    assertNull(restored.orderExceptionBits());
    assertArrayEquals(new long[] {2L, 5L, 8L}, Arrays.copyOf(restored.recordKeys(), 3));
    assertEquals(2L, restored.firstRecordKey());
    assertEquals(8L, restored.lastRecordKey());
  }

  @Test
  void rewrittenGroupRejectsDuplicateRecordIdentity() {
    final ProjectionIndexRowGroupPage page =
        new ProjectionIndexRowGroupPage(new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG});
    assertTrue(appendNumericRow(page, 41L, 1L, true));
    assertTrue(appendNumericRow(page, 41L, 2L, true));
    final ProjectionRecordLocator.Accessor locator = mock(ProjectionRecordLocator.Accessor.class);
    when(locator.find(41L)).thenReturn(3);

    final IllegalStateException failure = assertThrows(IllegalStateException.class,
        () -> ProjectionIndexChangeListener.validateRewrittenRowGroup(page, 3, locator,
            new LongOpenHashSet(ProjectionIndexRowGroupPage.MAX_ROWS)));

    assertTrue(failure.getMessage().contains("occurs more than once"));
  }

  @Test
  void rewrittenGroupRevalidatesCarriedExceptionLocator() {
    final ProjectionIndexRowGroupPage page =
        new ProjectionIndexRowGroupPage(new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG});
    assertTrue(appendNumericRow(page, 10L, 1L, false));
    assertTrue(appendNumericRow(page, 41L, 2L, true));
    final ProjectionRecordLocator.Accessor locator = mock(ProjectionRecordLocator.Accessor.class);
    // Models an unchanged exceptional row whose pre-existing sparse locator was lost or corrupted.
    // The rewrite did not insert/move key 41, but validation must still inspect it.
    when(locator.find(41L)).thenReturn(0);

    final IllegalStateException failure = assertThrows(IllegalStateException.class,
        () -> ProjectionIndexChangeListener.validateRewrittenRowGroup(page, 3, locator,
            new LongOpenHashSet(ProjectionIndexRowGroupPage.MAX_ROWS)));

    assertTrue(failure.getMessage().contains("instead of rewritten leaf 3 row 1"));
  }

  @Test
  void denseOrderMetadataRoundTripsAtWordBoundaries() {
    for (final int rows : new int[] {1, 64, 65, ProjectionIndexRowGroupPage.MAX_ROWS}) {
      final ProjectionIndexRowGroupPage dense =
          new ProjectionIndexRowGroupPage(new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG});
      final ProjectionIndexRowGroupPage none =
          new ProjectionIndexRowGroupPage(new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG});
      for (int row = 0; row < rows; row++) {
        final long key = row + 1L;
        final boolean exception = row == rows - 1;
        assertTrue(appendNumericRow(dense, key, row, exception));
        assertTrue(appendNumericRow(none, key, row, false));
      }

      final byte[] denseRaw = dense.serialize();
      final byte[] noneRaw = none.serialize();
      final int liveWords = (rows + 63) >>> 6;
      assertEquals(ProjectionIndexRowGroupPage.ORDER_EXCEPTIONS_DENSE, denseRaw[orderMarkerOffset(dense)]);
      assertEquals(noneRaw.length + liveWords * Long.BYTES, denseRaw.length,
          "DENSE must persist only ceil(rowCount/64) live words at rowCount=" + rows);

      final ProjectionIndexRowGroupPage restored = ProjectionIndexRowGroupPage.deserialize(denseRaw);
      assertTrue(restored.hasOrderExceptions());
      assertTrue(restored.orderExceptionAt(rows - 1));
      if (rows > 1) {
        assertFalse(restored.orderExceptionAt(rows - 2));
        assertEquals(1L, restored.firstRecordKey());
        assertEquals(rows - 1L, restored.lastRecordKey());
      } else {
        assertEquals(Long.MAX_VALUE, restored.firstRecordKey());
        assertEquals(Long.MIN_VALUE, restored.lastRecordKey());
      }
    }
  }

  @Test
  void builderReuseClearsLiveExceptionWords() {
    final ProjectionIndexRowGroupPage page =
        new ProjectionIndexRowGroupPage(new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG});
    for (int row = 0; row < 65; row++) {
      assertTrue(appendNumericRow(page, row + 1L, row, row == 64));
    }
    assertTrue(page.orderExceptionAt(64));

    page.resetForBuilderReuse(null);
    assertFalse(page.hasOrderExceptions());
    assertNull(page.orderExceptionBits(), "retained scratch must not become logical DENSE metadata");
    assertTrue(appendNumericRow(page, 100L, 1L, false));
    assertTrue(appendNumericRow(page, 101L, 2L, false));

    final byte[] raw = page.serialize();
    assertEquals(ProjectionIndexRowGroupPage.ORDER_EXCEPTIONS_NONE, raw[orderMarkerOffset(page)]);
    final ProjectionIndexRowGroupPage restored = ProjectionIndexRowGroupPage.deserialize(raw);
    assertFalse(restored.hasOrderExceptions());
    assertArrayEquals(new long[] {100L, 101L}, Arrays.copyOf(restored.recordKeys(), 2));
  }

  @Test
  void malformedOrderMetadataFailsClosed() {
    final ProjectionIndexRowGroupPage dense =
        new ProjectionIndexRowGroupPage(new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG});
    assertTrue(appendNumericRow(dense, 10L, 1L, true));
    final byte[] validDense = dense.serialize();
    final int denseMarker = orderMarkerOffset(dense);

    final byte[] unknownKind = validDense.clone();
    unknownKind[denseMarker] = 2;
    assertThrows(IllegalStateException.class, () -> ProjectionIndexRowGroupPage.deserialize(unknownKind));

    final byte[] noExceptionSet = validDense.clone();
    Arrays.fill(noExceptionSet, denseMarker + 1, denseMarker + 1 + Long.BYTES, (byte) 0);
    assertThrows(IllegalStateException.class, () -> ProjectionIndexRowGroupPage.deserialize(noExceptionSet));

    final byte[] bitBeyondRowCount = validDense.clone();
    bitBeyondRowCount[denseMarker + 1] |= 0x02;
    assertThrows(IllegalStateException.class, () -> ProjectionIndexRowGroupPage.deserialize(bitBeyondRowCount));

    final byte[] wrongFence = validDense.clone();
    ByteBuffer.wrap(wrongFence).order(ByteOrder.LITTLE_ENDIAN).putLong(8, 10L);
    assertThrows(IllegalStateException.class, () -> ProjectionIndexRowGroupPage.deserialize(wrongFence));

    final ProjectionIndexRowGroupPage normal =
        new ProjectionIndexRowGroupPage(new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG});
    assertTrue(appendNumericRow(normal, 2L, 1L, false));
    assertTrue(appendNumericRow(normal, 5L, 2L, false));
    final byte[] nonMonotone = normal.serialize();
    ByteBuffer.wrap(nonMonotone).order(ByteOrder.LITTLE_ENDIAN).putLong(25 + Long.BYTES, 1L);
    assertThrows(IllegalStateException.class, () -> ProjectionIndexRowGroupPage.deserialize(nonMonotone));

    final byte[] negativeKey = normal.serialize();
    ByteBuffer.wrap(negativeKey).order(ByteOrder.LITTLE_ENDIAN).putLong(25, -1L);
    assertThrows(IllegalStateException.class, () -> ProjectionIndexRowGroupPage.deserialize(negativeKey));

    final ProjectionIndexRowGroupPage empty =
        new ProjectionIndexRowGroupPage(new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG});
    final byte[] denseEmpty = empty.serialize();
    denseEmpty[orderMarkerOffset(empty)] = ProjectionIndexRowGroupPage.ORDER_EXCEPTIONS_DENSE;
    assertThrows(IllegalStateException.class, () -> ProjectionIndexRowGroupPage.deserialize(denseEmpty));
  }

  @Test
  void appendRowReturnsFalseAtCapacity() {
    final byte[] numOnly = {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG};
    final ProjectionIndexRowGroupPage p = new ProjectionIndexRowGroupPage(numOnly);
    final long[] nums = {42};
    final boolean[] bools = {false};
    final String[] strs = {null};
    for (int i = 0; i < ProjectionIndexRowGroupPage.MAX_ROWS; i++) {
      assertTrue(p.appendRow(i, nums, bools, strs), "row " + i);
    }
    assertFalse(p.appendRow(ProjectionIndexRowGroupPage.MAX_ROWS, nums, bools, strs));
    assertFalse(p.appendExtractedUtf8Row(ProjectionIndexRowGroupPage.MAX_ROWS, nums, bools, null, null, null, null,
        null, null), "a rejected row must not inspect or take ownership of its UTF-8 buffers");
  }

  @Test
  void extractedUtf8LaneIsByteIdenticalToLegacyStrings() {
    final byte[] kinds = {
        ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT,
        ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL,
        ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET
    };
    final ProjectionIndexRowGroupPage legacy = new ProjectionIndexRowGroupPage(kinds);
    final ProjectionIndexRowGroupPage raw = new ProjectionIndexRowGroupPage(kinds);
    final GlobalValueDictionaryWriter legacyGlobal = new GlobalValueDictionaryWriter();
    final GlobalValueDictionaryWriter rawGlobal = new GlobalValueDictionaryWriter();
    legacy.setGlobalDictionaries(new GlobalValueDictionaryWriter[] {null, legacyGlobal, null});
    raw.setGlobalDictionaries(new GlobalValueDictionaryWriter[] {null, rawGlobal, null});

    final String local = "Grüße 🦄";
    final String global = "世界🌍";
    final byte[] firstLocal = local.getBytes(StandardCharsets.UTF_8);
    final byte[] duplicateLocal = local.getBytes(StandardCharsets.UTF_8);
    final byte[] cleanEmpty = new byte[0];
    final String[][] legacyScalars = {
        {local, global, null},
        {new String(local), new String(global), null},
        {"", "", null},
        {null, null, null},
        {null, null, null}
    };
    final byte[][][] rawScalars = {
        {firstLocal, global.getBytes(StandardCharsets.UTF_8), null},
        {duplicateLocal, global.getBytes(StandardCharsets.UTF_8), null},
        {cleanEmpty, new byte[0], null},
        {null, null, null},
        {null, null, null}
    };
    final String[][][] sets = {
        {null, null, {"z", "a", "z", null}},
        {null, null, new String[0]},
        {null, null, {"", "β"}},
        {null, null, null},
        {null, null, {"ignored"}}
    };
    final boolean[][] present = {
        {true, true, true},
        {true, true, true},
        {true, true, true},
        {false, false, false},
        {true, true, true}
    };
    final boolean[][] unrepresentable = {
        {false, false, false},
        {false, false, false},
        {false, false, false},
        {false, false, false},
        {true, true, true}
    };
    final long[] longs = new long[kinds.length];
    final boolean[] bools = new boolean[kinds.length];

    for (int row = 0; row < legacyScalars.length; row++) {
      assertTrue(legacy.appendRow(100L + row, longs, bools, legacyScalars[row], sets[row], present[row],
          unrepresentable[row], null, null));
      assertTrue(raw.appendExtractedUtf8Row(100L + row, longs, bools, rawScalars[row], sets[row], present[row],
          unrepresentable[row], null, null));
    }

    assertArrayEquals(legacy.serialize(), raw.serialize(),
        "the raw scalar lane must preserve the complete persisted representation");
    assertNotSame(firstLocal, raw.stringDictionary(0)[0],
        "a newly distinct local value must not retain borrowed extractor storage");
    assertNotSame(cleanEmpty, raw.stringDictionary(0)[1],
        "a clean empty value must not retain its caller-owned array");
    for (int i = 0; i < raw.stringDictionarySize(0); i++) {
      assertNotSame(duplicateLocal, raw.stringDictionary(0)[i],
          "a duplicate caller buffer must not be retained");
    }
    assertArrayEquals(new int[] {0, 0, 1, 1, 1},
        Arrays.copyOf(raw.stringDictIdColumn(0), raw.getRowCount()));
    assertArrayEquals(new long[] {1L, 1L, 2L, 0L, 0L},
        Arrays.copyOf(raw.numericColumn(1), raw.getRowCount()));
    assertEquals(2, rawGlobal.entryCount(), "only distinct clean global values belong in the dictionary");
    assertEquals(legacyGlobal.entryCount(), rawGlobal.entryCount());
    assertArrayEquals(global.getBytes(StandardCharsets.UTF_8), rawGlobal.valueBytes(1));
    assertArrayEquals(new byte[0], rawGlobal.valueBytes(2));
    assertArrayEquals(new int[] {3, 0, 2, 0, 0},
        Arrays.copyOf(raw.stringSetCountColumn(2), raw.getRowCount()),
        "STRING_SET order/count/null semantics must stay on the legacy lane");
    assertArrayEquals(new int[] {0, 1, 0, 2, 3},
        Arrays.copyOf(raw.stringSetIdColumn(2), raw.stringSetLength(2)));
    assertTrue(raw.columnUnrepresentable(0));
    assertTrue(raw.columnUnrepresentable(1));
    assertTrue(raw.columnUnrepresentable(2));
  }

  @Test
  void borrowedUtf8ScratchCopiesOnlyDistinctLiveSlices() {
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(
        new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT});
    final byte[] scratch = new byte[64];
    final byte[][] values = {scratch};
    final int[] lengths = new int[1];
    final long[] longs = new long[1];
    final boolean[] bools = new boolean[1];

    lengths[0] = putUtf8(scratch, "alpha");
    assertTrue(page.appendExtractedUtf8Row(1L, longs, bools, values, lengths, null, null, null, null, null));
    lengths[0] = putUtf8(scratch, "alpha");
    assertTrue(page.appendExtractedUtf8Row(2L, longs, bools, values, lengths, null, null, null, null, null));
    lengths[0] = putUtf8(scratch, "beta");
    assertTrue(page.appendExtractedUtf8Row(3L, longs, bools, values, lengths, null, null, null, null, null));

    assertEquals(2, page.stringDictionarySize(0));
    assertArrayEquals("alpha".getBytes(StandardCharsets.UTF_8), page.stringDictionary(0)[0]);
    assertArrayEquals("beta".getBytes(StandardCharsets.UTF_8), page.stringDictionary(0)[1]);
    assertArrayEquals(new int[] {0, 0, 1}, Arrays.copyOf(page.stringDictIdColumn(0), page.getRowCount()));
    assertNotSame(scratch, page.stringDictionary(0)[0]);
    assertNotSame(scratch, page.stringDictionary(0)[1]);
  }

  private static int putUtf8(final byte[] destination, final String value) {
    final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    System.arraycopy(bytes, 0, destination, 0, bytes.length);
    Arrays.fill(destination, bytes.length, destination.length, (byte) 0x5A);
    return bytes.length;
  }

  @Test
  void slabHashIndexPreservesCollisionsGrowthColdReadAndAbaReuse() {
    final byte[] kinds = {ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT};
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(kinds);
    final long[] longs = new long[1];
    final boolean[] bools = new boolean[1];
    final byte[][] values = new byte[1][];
    final int[] lengths = new int[1];
    final String[] distinct = new String[96];
    distinct[0] = "Aa";
    distinct[1] = "BB"; // Same 31-based byte hash as "Aa"; equality must disambiguate it.
    for (int i = 2; i < distinct.length; i++) {
      distinct[i] = "value-" + i + "-Grüße-世界";
    }

    int row = 0;
    for (int dictId = 0; dictId < distinct.length; dictId++) {
      values[0] = distinct[dictId].getBytes(StandardCharsets.UTF_8);
      lengths[0] = values[0].length;
      assertTrue(page.appendExtractedUtf8Row(++row, longs, bools, values, lengths, null, null, null, null, null));
      assertEquals(dictId, page.stringDictIdColumn(0)[row - 1]);
    }
    for (int dictId = distinct.length - 1; dictId >= 0; dictId--) {
      values[0] = distinct[dictId].getBytes(StandardCharsets.UTF_8);
      lengths[0] = values[0].length;
      assertTrue(page.appendExtractedUtf8Row(++row, longs, bools, values, lengths, null, null, null, null, null));
      assertEquals(dictId, page.stringDictIdColumn(0)[row - 1], "duplicate lookup after hash-table growth");
    }
    assertEquals(distinct.length, page.stringDictionarySize(0));

    final ProjectionIndexRowGroupPage cold = ProjectionIndexRowGroupPage.deserialize(page.serialize());
    assertEquals(page.getRowCount(), cold.getRowCount());
    assertArrayEquals(Arrays.copyOf(page.stringDictIdColumn(0), page.getRowCount()),
        Arrays.copyOf(cold.stringDictIdColumn(0), cold.getRowCount()));
    for (int dictId = 0; dictId < distinct.length; dictId++) {
      assertArrayEquals(distinct[dictId].getBytes(StandardCharsets.UTF_8), cold.stringDictionary(0)[dictId]);
    }

    page.resetForBuilderReuse(null);
    final ProjectionIndexRowGroupPage cleanControl = new ProjectionIndexRowGroupPage(kinds);
    row = 0;
    for (int dictId = distinct.length - 1; dictId >= 0; dictId--) {
      values[0] = distinct[dictId].getBytes(StandardCharsets.UTF_8);
      lengths[0] = values[0].length;
      assertTrue(page.appendExtractedUtf8Row(++row, longs, bools, values, lengths, null, null, null, null, null));
      assertTrue(
          cleanControl.appendExtractedUtf8Row(row, longs, bools, values, lengths, null, null, null, null, null));
    }
    assertArrayEquals(cleanControl.serialize(), page.serialize(),
        "a retained hash table must not expose entries from the preceding page generation");
  }

  @Test
  void resetReleasesAnOutlierScalarStringSlab() {
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(
        new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT});
    final long[] longs = new long[1];
    final boolean[] bools = new boolean[1];
    final byte[][] values = {new byte[2 * 1024 * 1024]};
    final int[] lengths = {values[0].length};
    Arrays.fill(values[0], (byte) 'x');

    assertTrue(page.appendExtractedUtf8Row(1L, longs, bools, values, lengths, null, null, null, null, null));
    final byte[] outlierSlab = page.stringDictionaryEntryBacking(0, 0);
    assertTrue(outlierSlab.length >= values[0].length);

    page.resetForBuilderReuse(null);
    values[0] = "small-next-page".getBytes(StandardCharsets.UTF_8);
    lengths[0] = values[0].length;
    assertTrue(page.appendExtractedUtf8Row(2L, longs, bools, values, lengths, null, null, null, null, null));

    final byte[] nextPageSlab = page.stringDictionaryEntryBacking(0, 0);
    assertNotSame(outlierSlab, nextPageSlab);
    assertTrue(nextPageSlab.length < outlierSlab.length,
        "a single arbitrary-size value must not pin its slab for every later page");
    assertArrayEquals(values[0], Arrays.copyOfRange(nextPageSlab, page.stringDictionaryEntryOffset(0, 0),
        page.stringDictionaryEntryOffset(0, 0) + page.stringDictionaryEntryLength(0, 0)));
  }

  @Test
  void cleanScalarNullUtf8FailsLoudly() {
    final byte[] kinds = {
        ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL,
        ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT
    };
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(kinds);
    final ProjectionIndexRowGroupPage cleanControl = new ProjectionIndexRowGroupPage(kinds);
    final GlobalValueDictionaryWriter pageGlobal = new GlobalValueDictionaryWriter();
    final GlobalValueDictionaryWriter controlGlobal = new GlobalValueDictionaryWriter();
    page.setGlobalDictionaries(new GlobalValueDictionaryWriter[] {pageGlobal, null});
    cleanControl.setGlobalDictionaries(new GlobalValueDictionaryWriter[] {controlGlobal, null});
    final long[] longs = new long[kinds.length];
    final boolean[] bools = new boolean[kinds.length];
    final boolean[] present = {true, true};
    final boolean[] representable = {false, false};

    final IllegalStateException thrown = assertThrows(IllegalStateException.class,
        () -> page.appendExtractedUtf8Row(1L, longs, bools,
            new byte[][] {"must-not-be-interned".getBytes(StandardCharsets.UTF_8), null}, null, present,
            representable, null, null));

    assertTrue(thrown.getMessage().contains("null UTF-8 bytes"), thrown.getMessage());
    assertEquals(0, page.getRowCount(), "the failed row must never become visible");
    assertEquals(0, pageGlobal.entryCount(), "prevalidation must run before an earlier global column interns");

    final byte[][] validPageRow = {
        "global-value".getBytes(StandardCharsets.UTF_8),
        "local-value".getBytes(StandardCharsets.UTF_8)
    };
    final byte[][] validControlRow = {
        "global-value".getBytes(StandardCharsets.UTF_8),
        "local-value".getBytes(StandardCharsets.UTF_8)
    };
    assertTrue(page.appendExtractedUtf8Row(2L, longs, bools, validPageRow, null, present, representable, null, null));
    assertTrue(cleanControl.appendExtractedUtf8Row(2L, longs, bools, validControlRow, null, present, representable,
        null, null));
    assertArrayEquals(cleanControl.serialize(), page.serialize(),
        "a rejected row must leave no fences, presence bits, ids or dictionary state behind");
    assertEquals(controlGlobal.entryCount(), pageGlobal.entryCount());
  }

  @Test
  void scalarSlabRetainsCapacityAndClearsLogicalStateAcrossFailureAndAbaReuse() {
    final byte[] kinds = {ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT};
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(kinds);
    final long[] longs = new long[1];
    final boolean[] bools = new boolean[1];
    final byte[][] values = new byte[1][];
    final int[] lengths = new int[1];
    final String largeUnicode = "Grüße-世界-🦄-" + "α".repeat(3_000);
    final byte[] a = largeUnicode.getBytes(StandardCharsets.UTF_8);

    values[0] = a;
    lengths[0] = a.length;
    assertTrue(page.appendExtractedUtf8Row(1L, longs, bools, values, lengths, null, null, null, null, null));
    assertTrue(page.appendExtractedUtf8Row(2L, longs, bools, values, lengths, null, null, null, null, null),
        "a duplicate must reuse the first range");
    values[0] = new byte[0];
    lengths[0] = 0;
    assertTrue(page.appendExtractedUtf8Row(3L, longs, bools, values, lengths, null, null, null, null, null));
    final byte[] firstAWire = page.serialize();
    final byte[] retainedSlab = page.stringDictionaryEntryBacking(0, 0);
    final int retainedCapacity = retainedSlab.length;
    assertTrue(page.stringDictionaryIsSlabBacked(0));
    assertEquals(2, page.stringDictionarySize(0));
    assertArrayEquals(a, Arrays.copyOfRange(retainedSlab, page.stringDictionaryEntryOffset(0, 0),
        page.stringDictionaryEntryOffset(0, 0) + page.stringDictionaryEntryLength(0, 0)));

    final byte[][] compatibility = page.stringDictionary(0);
    assertArrayEquals(a, compatibility[0]);
    assertArrayEquals(new byte[0], compatibility[1]);
    assertNotSame(retainedSlab, compatibility[0], "the public byte[][] view must detach entry storage from the slab");

    page.resetForBuilderReuse(null);
    assertEquals(0, page.getRowCount());
    assertEquals(0, page.stringDictionarySize(0));
    assertArrayEquals(a, compatibility[0], "reset must not clear a detached compatibility view retained by a caller");

    values[0] = "B".getBytes(StandardCharsets.UTF_8);
    lengths[0] = values[0].length + 1;
    assertThrows(IllegalArgumentException.class,
        () -> page.appendExtractedUtf8Row(4L, longs, bools, values, lengths, null, null, null, null, null));
    assertEquals(0, page.getRowCount(), "range validation must precede every page mutation");
    assertEquals(0, page.stringDictionarySize(0));

    lengths[0] = values[0].length;
    assertTrue(page.appendExtractedUtf8Row(5L, longs, bools, values, lengths, null, null, null, null, null));
    assertSame(retainedSlab, page.stringDictionaryEntryBacking(0, 0),
        "B must reuse the slab grown by A instead of allocating an exact entry array");
    assertEquals(retainedCapacity, page.stringDictionaryEntryBacking(0, 0).length);

    page.resetForBuilderReuse(null);
    values[0] = a;
    lengths[0] = a.length;
    assertTrue(page.appendExtractedUtf8Row(1L, longs, bools, values, lengths, null, null, null, null, null));
    assertTrue(page.appendExtractedUtf8Row(2L, longs, bools, values, lengths, null, null, null, null, null));
    values[0] = new byte[0];
    lengths[0] = 0;
    assertTrue(page.appendExtractedUtf8Row(3L, longs, bools, values, lengths, null, null, null, null, null));
    assertArrayEquals(firstAWire, page.serialize(), "A/B/A reuse must clear ids, lengths, fences and live bytes");
    assertSame(retainedSlab, page.stringDictionaryEntryBacking(0, 0));
  }
}
