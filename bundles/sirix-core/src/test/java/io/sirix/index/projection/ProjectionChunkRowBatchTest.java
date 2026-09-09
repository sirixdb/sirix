/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ProjectionChunkRowBatchTest {

  private static final long NUMERIC_PATH_NODE_KEY = 17L;
  private static final int LONG_COLUMN = 0;
  private static final int DOUBLE_COLUMN = 1;

  @Test
  void primitiveIntegralFeedsMatchBoxedLanesAndFlagsAtConversionBoundaries() {
    final IntegralInput[] inputs =
        {new IntegralInput(false, Integer.MIN_VALUE), new IntegralInput(false, Integer.MAX_VALUE),
            new IntegralInput(true, 1L << 53), new IntegralInput(true, (1L << 53) + 1L),
            new IntegralInput(true, Long.MIN_VALUE), new IntegralInput(true, Long.MAX_VALUE)};
    final ProjectionChunkRowBatch primitive = numericBatch(inputs.length);
    final ProjectionChunkRowBatch boxed = numericBatch(inputs.length);

    for (int row = 0; row < inputs.length; row++) {
      final IntegralInput input = inputs[row];
      primitive.beginRecord(100L + row);
      boxed.beginRecord(100L + row);
      if (input.longSource()) {
        primitive.onNamedLong(NUMERIC_PATH_NODE_KEY, input.value());
        boxed.onNamedNumber(NUMERIC_PATH_NODE_KEY, Long.valueOf(input.value()));
      } else {
        primitive.onNamedInt(NUMERIC_PATH_NODE_KEY, (int) input.value());
        boxed.onNamedNumber(NUMERIC_PATH_NODE_KEY, Integer.valueOf((int) input.value()));
      }
    }
    primitive.finishBuild();
    boxed.finishBuild();

    assertEquals(inputs.length, primitive.rowCount());
    assertEquals(inputs.length, boxed.rowCount());
    for (int row = 0; row < inputs.length; row++) {
      for (int column = 0; column < 2; column++) {
        assertEquals(boxed.flagPresent(column, row), primitive.flagPresent(column, row), coordinate(column, row));
        assertEquals(boxed.flagUnrepresentable(column, row), primitive.flagUnrepresentable(column, row),
            coordinate(column, row));
        assertEquals(boxed.flagNonIntegral(column, row), primitive.flagNonIntegral(column, row),
            coordinate(column, row));
        assertEquals(boxed.flagNonDoubleSource(column, row), primitive.flagNonDoubleSource(column, row),
            coordinate(column, row));
        assertEquals(boxed.longValue(column, row), primitive.longValue(column, row), coordinate(column, row));
      }

      assertTrue(primitive.flagPresent(LONG_COLUMN, row));
      assertFalse(primitive.flagUnrepresentable(LONG_COLUMN, row));
      assertFalse(primitive.flagNonIntegral(LONG_COLUMN, row));
      assertFalse(primitive.flagNonDoubleSource(LONG_COLUMN, row));
      assertEquals(inputs[row].value(), primitive.longValue(LONG_COLUMN, row));

      assertTrue(primitive.flagPresent(DOUBLE_COLUMN, row));
      assertFalse(primitive.flagUnrepresentable(DOUBLE_COLUMN, row));
      assertTrue(primitive.flagNonDoubleSource(DOUBLE_COLUMN, row));
      assertEquals(ProjectionDoubleEncoding.encode((double) inputs[row].value()),
          primitive.longValue(DOUBLE_COLUMN, row));
    }

    assertFalse(primitive.flagNonIntegral(DOUBLE_COLUMN, 2), "2^53 is exactly representable as double");
    assertTrue(primitive.flagNonIntegral(DOUBLE_COLUMN, 3), "2^53 + 1 loses precision as double");
    assertFalse(primitive.flagNonIntegral(DOUBLE_COLUMN, 4), "Long.MIN_VALUE is exactly -2^63");
    assertTrue(primitive.flagNonIntegral(DOUBLE_COLUMN, 5), "Long.MAX_VALUE rounds up to 2^63");
  }

  @Test
  void stringArenaCopiesAcrossFixedSizeNonHumongousChunks() {
    final byte kind = ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT;
    final ProjectionChunkRowBatch batch =
        new ProjectionChunkRowBatch(new long[] {17L}, new int[] {0}, new byte[] {kind}, 3, 1L);
    final int arenaChunkBytes = ProjectionChunkRowBatch.stringArenaChunkBytes();
    final byte[] first = patternedBytes(arenaChunkBytes - 7, 3);
    final byte[] crossing = patternedBytes(31, 19);
    final byte[] large = patternedBytes(arenaChunkBytes * 3 + 13, 71);

    batch.beginRecord(2L);
    batch.onNamedString(17L, first, first.length);
    batch.beginRecord(3L);
    batch.onNamedString(17L, crossing, crossing.length);
    batch.beginRecord(4L);
    batch.onNamedString(17L, large, large.length);
    batch.finishBuild();

    assertEquals(64 << 10, arenaChunkBytes);
    assertArrayEquals(first, copiedString(batch, 0));
    assertArrayEquals(crossing, copiedString(batch, 1));
    assertArrayEquals(large, copiedString(batch, 2));
  }

  @Test
  void rowLimitKeepsEveryRowIndexedLaneWithin256KiB() {
    final int columns = 25;
    final int limit = ProjectionChunkRowBatch.maxHftChunkRows(columns);
    assertEquals((256 << 10) / columns, limit);

    final byte[] kinds = new byte[columns];
    Arrays.fill(kinds, ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN);
    new ProjectionChunkRowBatch(new long[0], new int[0], kinds, limit, 1L);
    assertThrows(IllegalArgumentException.class,
        () -> new ProjectionChunkRowBatch(new long[0], new int[0], kinds, limit + 1, 1L));
  }

  @Test
  void rejectsAProjectionWhoseOuterReferenceLanesWouldExceed256KiB() {
    final int excessiveColumns = (256 << 10) / Long.BYTES + 1;
    assertThrows(IllegalArgumentException.class, () -> ProjectionChunkRowBatch.maxHftChunkRows(excessiveColumns));
  }

  private static byte[] copiedString(final ProjectionChunkRowBatch batch, final int row) {
    final byte[] copied = new byte[batch.stringLength(0, row)];
    batch.copyStringTo(0, row, copied);
    return copied;
  }

  private static ProjectionChunkRowBatch numericBatch(final int rows) {
    return new ProjectionChunkRowBatch(new long[] {NUMERIC_PATH_NODE_KEY, NUMERIC_PATH_NODE_KEY},
        new int[] {LONG_COLUMN, DOUBLE_COLUMN}, new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
            ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE},
        rows, 1L);
  }

  private static String coordinate(final int column, final int row) {
    return "column " + column + ", row " + row;
  }

  private static byte[] patternedBytes(final int length, final int seed) {
    final byte[] bytes = new byte[length];
    int value = seed;
    for (int i = 0; i < length; i++) {
      value = value * 1103515245 + 12345;
      bytes[i] = (byte) (value >>> 16);
    }
    return bytes;
  }

  private record IntegralInput(boolean longSource, long value) {
  }
}
