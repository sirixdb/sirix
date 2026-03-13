package io.sirix.query.compiler.vectorized;

import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ColumnBatch} — the columnar batch data structure
 * with selection vector support.
 */
final class ColumnBatchTest {

  @Test
  void createBatchAndSetColumns() {
    final var batch = new ColumnBatch(64, 3);
    batch.setColumnType(0, ColumnType.INT64);
    batch.setColumnType(1, ColumnType.FLOAT64);
    batch.setColumnType(2, ColumnType.STRING);

    assertEquals(64, batch.capacity());
    assertEquals(3, batch.columnCount());
    assertEquals(ColumnType.INT64, batch.getColumnType(0));
    assertEquals(ColumnType.FLOAT64, batch.getColumnType(1));
    assertEquals(ColumnType.STRING, batch.getColumnType(2));
  }

  @Test
  void setAndGetLongValues() {
    final var batch = new ColumnBatch(4, 1);
    batch.setColumnType(0, ColumnType.INT64);

    batch.setLong(0, 0, 100L);
    batch.setLong(0, 1, 200L);
    batch.setLong(0, 2, 300L);
    batch.setRowCount(3);

    assertEquals(100L, batch.getLong(0, 0));
    assertEquals(200L, batch.getLong(0, 1));
    assertEquals(300L, batch.getLong(0, 2));
    assertEquals(3, batch.rowCount());
  }

  @Test
  void setAndGetDoubleValues() {
    final var batch = new ColumnBatch(4, 1);
    batch.setColumnType(0, ColumnType.FLOAT64);

    batch.setDouble(0, 0, 1.5);
    batch.setDouble(0, 1, 2.7);
    batch.setRowCount(2);

    assertEquals(1.5, batch.getDouble(0, 0));
    assertEquals(2.7, batch.getDouble(0, 1));
  }

  @Test
  void setAndGetStringValues() {
    final var batch = new ColumnBatch(4, 1);
    batch.setColumnType(0, ColumnType.STRING);

    batch.setString(0, 0, "hello");
    batch.setString(0, 1, null);
    batch.setRowCount(2);

    assertEquals("hello", batch.getString(0, 0));
    assertNull(batch.getString(0, 1));
    assertFalse(batch.isNull(0, 0));
    assertTrue(batch.isNull(0, 1));
  }

  @Test
  void setAndGetBooleanValues() {
    final var batch = new ColumnBatch(4, 1);
    batch.setColumnType(0, ColumnType.BOOLEAN);

    batch.setBoolean(0, 0, true);
    batch.setBoolean(0, 1, false);
    batch.setRowCount(2);

    assertTrue(batch.getBoolean(0, 0));
    assertFalse(batch.getBoolean(0, 1));
  }

  @Test
  void nullTracking() {
    final var batch = new ColumnBatch(4, 1);
    batch.setColumnType(0, ColumnType.INT64);

    batch.setLong(0, 0, 42L);
    batch.setNull(0, 1);
    batch.setRowCount(2);

    assertFalse(batch.isNull(0, 0));
    assertTrue(batch.isNull(0, 1));
  }

  @Test
  void sealInitializesSelectionVector() {
    final var batch = new ColumnBatch(8, 1);
    batch.setColumnType(0, ColumnType.INT64);

    for (int i = 0; i < 5; i++) {
      batch.setLong(0, i, i * 10L);
    }
    batch.setRowCount(5);
    batch.seal();

    assertEquals(5, batch.selectionCount());
    for (int i = 0; i < 5; i++) {
      assertEquals(i, batch.selectedRow(i));
    }
  }

  @Test
  void selectionVectorCanBeReduced() {
    final var batch = new ColumnBatch(8, 1);
    batch.setColumnType(0, ColumnType.INT64);
    batch.setRowCount(5);
    batch.seal();

    // Simulate filter: keep only rows 1 and 3
    final int[] sel = batch.selectionVector();
    sel[0] = 1;
    sel[1] = 3;
    batch.setSelectionCount(2);

    assertEquals(2, batch.selectionCount());
    assertEquals(1, batch.selectedRow(0));
    assertEquals(3, batch.selectedRow(1));
  }

  @Test
  void resetClearsStateButKeepsAllocations() {
    final var batch = new ColumnBatch(4, 2);
    batch.setColumnType(0, ColumnType.INT64);
    batch.setColumnType(1, ColumnType.STRING);

    batch.setLong(0, 0, 42L);
    batch.setString(1, 0, "test");
    batch.setRowCount(1);
    batch.seal();

    batch.reset();

    assertEquals(0, batch.rowCount());
    assertEquals(0, batch.selectionCount());
    // Column types are preserved
    assertEquals(ColumnType.INT64, batch.getColumnType(0));
    assertEquals(ColumnType.STRING, batch.getColumnType(1));
  }

  @Test
  void directColumnArrayAccess() {
    final var batch = new ColumnBatch(4, 1);
    batch.setColumnType(0, ColumnType.INT64);

    batch.setLong(0, 0, 10L);
    batch.setLong(0, 1, 20L);

    final long[] col = batch.longColumn(0);
    assertEquals(10L, col[0]);
    assertEquals(20L, col[1]);
  }

  @Test
  void multiColumnBatch() {
    final var batch = new ColumnBatch(4, 3);
    batch.setColumnType(0, ColumnType.INT64);
    batch.setColumnType(1, ColumnType.FLOAT64);
    batch.setColumnType(2, ColumnType.STRING);

    batch.setLong(0, 0, 1L);
    batch.setDouble(1, 0, 3.14);
    batch.setString(2, 0, "pi");

    batch.setLong(0, 1, 2L);
    batch.setDouble(1, 1, 2.72);
    batch.setString(2, 1, "e");

    batch.setRowCount(2);
    batch.seal();

    assertEquals(2, batch.selectionCount());
    assertEquals(1L, batch.getLong(0, 0));
    assertEquals(3.14, batch.getDouble(1, 0));
    assertEquals("pi", batch.getString(2, 0));
    assertEquals(2L, batch.getLong(0, 1));
    assertEquals(2.72, batch.getDouble(1, 1));
    assertEquals("e", batch.getString(2, 1));
  }

  // --- Validation ---

  @Test
  void rejectsZeroCapacity() {
    assertThrows(IllegalArgumentException.class, () -> new ColumnBatch(0, 1));
  }

  @Test
  void rejectsNegativeCapacity() {
    assertThrows(IllegalArgumentException.class, () -> new ColumnBatch(-1, 1));
  }

  @Test
  void rejectsZeroColumnCount() {
    assertThrows(IllegalArgumentException.class, () -> new ColumnBatch(4, 0));
  }

  @Test
  void rejectsNullColumnType() {
    final var batch = new ColumnBatch(4, 1);
    assertThrows(IllegalArgumentException.class, () -> batch.setColumnType(0, null));
  }

  @Test
  void rejectsOutOfBoundsColumnIndex() {
    final var batch = new ColumnBatch(4, 2);
    assertThrows(IndexOutOfBoundsException.class, () -> batch.setColumnType(2, ColumnType.INT64));
    assertThrows(IndexOutOfBoundsException.class, () -> batch.setColumnType(-1, ColumnType.INT64));
  }

  @Test
  void rejectsOutOfBoundsRowIndex() {
    final var batch = new ColumnBatch(4, 1);
    batch.setColumnType(0, ColumnType.INT64);
    assertThrows(IndexOutOfBoundsException.class, () -> batch.setLong(0, 4, 0L));
    assertThrows(IndexOutOfBoundsException.class, () -> batch.setLong(0, -1, 0L));
  }

  @Test
  void rejectsInvalidRowCount() {
    final var batch = new ColumnBatch(4, 1);
    assertThrows(IllegalArgumentException.class, () -> batch.setRowCount(-1));
    assertThrows(IllegalArgumentException.class, () -> batch.setRowCount(5));
  }

  @Test
  void rejectsInvalidSelectionCount() {
    final var batch = new ColumnBatch(4, 1);
    batch.setRowCount(3);
    assertThrows(IllegalArgumentException.class, () -> batch.setSelectionCount(-1));
    assertThrows(IllegalArgumentException.class, () -> batch.setSelectionCount(4));
  }

  // --- DEFERRED_BYTES tests ---

  @Test
  void deferredBytesSetAndMaterialize() {
    final var batch = new ColumnBatch(4, 1);
    batch.setColumnType(0, ColumnType.DEFERRED_BYTES);

    // Create a backing page with "hello" at offset 10
    final byte[] pageData = new byte[64];
    final byte[] hello = "hello".getBytes(StandardCharsets.UTF_8);
    System.arraycopy(hello, 0, pageData, 10, hello.length);
    final MemorySegment page = MemorySegment.ofArray(pageData);

    final int pageIdx = batch.addBackingPage(0, page, null);
    assertEquals(0, pageIdx);

    batch.setDeferredBytes(0, 0, pageIdx, 10, hello.length, false);
    batch.setRowCount(1);
    batch.seal();

    // Materialize the string
    final String result = batch.materializeDeferredString(0, 0);
    assertEquals("hello", result);

    // Second call returns cached value
    assertEquals("hello", batch.materializeDeferredString(0, 0));
  }

  @Test
  void deferredBytesMultiPageBacking() {
    final var batch = new ColumnBatch(4, 1);
    batch.setColumnType(0, ColumnType.DEFERRED_BYTES);

    // Page 0: "apple"
    final byte[] page0Data = new byte[64];
    final byte[] apple = "apple".getBytes(StandardCharsets.UTF_8);
    System.arraycopy(apple, 0, page0Data, 5, apple.length);
    final int pg0 = batch.addBackingPage(0, MemorySegment.ofArray(page0Data), null);

    // Page 1: "banana"
    final byte[] page1Data = new byte[64];
    final byte[] banana = "banana".getBytes(StandardCharsets.UTF_8);
    System.arraycopy(banana, 0, page1Data, 20, banana.length);
    final int pg1 = batch.addBackingPage(0, MemorySegment.ofArray(page1Data), null);

    batch.setDeferredBytes(0, 0, pg0, 5, apple.length, false);
    batch.setDeferredBytes(0, 1, pg1, 20, banana.length, false);
    batch.setRowCount(2);
    batch.seal();

    assertEquals("apple", batch.materializeDeferredString(0, 0));
    assertEquals("banana", batch.materializeDeferredString(0, 1));
  }

  @Test
  void deferredBytesBatchMaterializeSelected() {
    final var batch = new ColumnBatch(8, 1);
    batch.setColumnType(0, ColumnType.DEFERRED_BYTES);

    final byte[] pageData = new byte[256];
    final String[] values = {"aaa", "bbb", "ccc", "ddd"};
    int offset = 0;
    for (final String v : values) {
      final byte[] bytes = v.getBytes(StandardCharsets.UTF_8);
      System.arraycopy(bytes, 0, pageData, offset, bytes.length);
      offset += bytes.length + 10; // 10-byte gap between values
    }

    final int pgIdx = batch.addBackingPage(0, MemorySegment.ofArray(pageData), null);

    offset = 0;
    for (int i = 0; i < values.length; i++) {
      batch.setDeferredBytes(0, i, pgIdx, offset, values[i].length(), false);
      offset += values[i].length() + 10;
    }
    batch.setRowCount(4);
    batch.seal();

    // Narrow selection to rows 1 and 3 only
    final int[] sel = batch.selectionVector();
    sel[0] = 1;
    sel[1] = 3;
    batch.setSelectionCount(2);

    // Batch materialize only selected rows
    final String[] output = batch.materializeAllSelected(0);
    assertNotNull(output);
    assertNull(output[0], "Row 0 not selected, should not be materialized");
    assertEquals("bbb", output[1]);
    assertNull(output[2], "Row 2 not selected, should not be materialized");
    assertEquals("ddd", output[3]);
  }

  @Test
  void deferredBytesNullRowReturnsNull() {
    final var batch = new ColumnBatch(4, 1);
    batch.setColumnType(0, ColumnType.DEFERRED_BYTES);

    batch.addBackingPage(0, MemorySegment.ofArray(new byte[64]), null);
    batch.setNull(0, 0);
    batch.setRowCount(1);

    assertNull(batch.materializeDeferredString(0, 0));
  }

  @Test
  void deferredBytesDirectArrayAccess() {
    final var batch = new ColumnBatch(4, 1);
    batch.setColumnType(0, ColumnType.DEFERRED_BYTES);

    batch.addBackingPage(0, MemorySegment.ofArray(new byte[64]), null);
    batch.setDeferredBytes(0, 0, 0, 42, 10, true);
    batch.setRowCount(1);

    assertEquals(42, batch.deferredOffsets(0)[0]);
    assertEquals(10, batch.deferredLengths(0)[0]);
    assertTrue(batch.deferredCompressed(0)[0]);
    assertEquals(0, batch.deferredPageIndices(0)[0]);
  }

  @Test
  void deferredBytesClearBackingPages() {
    final var batch = new ColumnBatch(4, 1);
    batch.setColumnType(0, ColumnType.DEFERRED_BYTES);

    batch.addBackingPage(0, MemorySegment.ofArray(new byte[64]), null);
    batch.addBackingPage(0, MemorySegment.ofArray(new byte[64]), null);
    assertEquals(2, batch.backingPages(0).size());

    batch.clearBackingPages(0);
    assertEquals(0, batch.backingPages(0).size());
    assertEquals(0, batch.parsedFsstSymbolsByPage(0).size());
  }

  // --- BYTES tests ---

  @Test
  void bytesColumnSetAndGet() {
    final var batch = new ColumnBatch(4, 1);
    batch.setColumnType(0, ColumnType.BYTES);

    final byte[] data = {1, 2, 3, 4};
    batch.setBytes(0, 0, data);
    batch.setBytes(0, 1, null);
    batch.setRowCount(2);

    assertArrayEquals(data, batch.getBytes(0, 0));
    assertNull(batch.getBytes(0, 1));
    assertFalse(batch.isNull(0, 0));
    assertTrue(batch.isNull(0, 1));
  }

  // --- DEFERRED_BYTES + selection vector integration ---

  @Test
  void selectionVectorWithDeferredMaterialization() {
    final var batch = new ColumnBatch(8, 2);
    batch.setColumnType(0, ColumnType.INT64);
    batch.setColumnType(1, ColumnType.DEFERRED_BYTES);

    // Set up page with string values
    final byte[] pageData = new byte[128];
    final String[] values = {"cat", "dog", "fish", "bird"};
    int off = 0;
    for (final String v : values) {
      final byte[] b = v.getBytes(StandardCharsets.UTF_8);
      System.arraycopy(b, 0, pageData, off, b.length);
      off += 16;
    }
    final int pgIdx = batch.addBackingPage(1, MemorySegment.ofArray(pageData), null);

    // Fill rows with IDs and deferred strings
    off = 0;
    for (int i = 0; i < 4; i++) {
      batch.setLong(0, i, (i + 1) * 100L);
      batch.setDeferredBytes(1, i, pgIdx, off, values[i].length(), false);
      off += 16;
    }
    batch.setRowCount(4);
    batch.seal();

    // Simulate filter: keep rows where ID > 200 (rows 2 and 3)
    final int[] sel = batch.selectionVector();
    sel[0] = 2;
    sel[1] = 3;
    batch.setSelectionCount(2);

    // Materialize only selected deferred strings
    final String[] output = batch.materializeAllSelected(1);
    assertNull(output[0], "Row 0 filtered out");
    assertNull(output[1], "Row 1 filtered out");
    assertEquals("fish", output[2]);
    assertEquals("bird", output[3]);
  }

  @Test
  void resetClearsDeferredBytesState() {
    final var batch = new ColumnBatch(4, 1);
    batch.setColumnType(0, ColumnType.DEFERRED_BYTES);

    batch.addBackingPage(0, MemorySegment.ofArray(new byte[64]), null);
    batch.setDeferredBytes(0, 0, 0, 10, 5, false);
    batch.setRowCount(1);
    batch.seal();

    batch.reset();

    assertEquals(0, batch.rowCount());
    assertEquals(0, batch.selectionCount());
    assertEquals(0, batch.backingPages(0).size(), "Backing pages should be cleared on reset");
    // Column type preserved
    assertEquals(ColumnType.DEFERRED_BYTES, batch.getColumnType(0));
  }
}
