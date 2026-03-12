package io.sirix.query.compiler.vectorized;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
}
