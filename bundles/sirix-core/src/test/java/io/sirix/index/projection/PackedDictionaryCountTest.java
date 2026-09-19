/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.PackedDictionaryIds;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class PackedDictionaryCountTest {
  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 4})
  void countsAndFirstRowsMatchIndependentRowOracle(final int width) {
    final Random random = new Random(939 + width);
    final int alphabet = 1 << width;
    for (final int rows : new int[] {1, 3, 7, 31, 63, 64, 65, 511, 1024}) {
      for (int pattern = 0; pattern < 10; pattern++) {
        final int[] ids = new int[rows];
        final long[] mask = new long[(rows + 63) >>> 6];
        final long[] presence = new long[mask.length];
        // Deliberately dirty unused tail bits: no row beyond rowCount may contribute.
        Arrays.fill(mask, -1L);
        Arrays.fill(presence, -1L);
        final int[] expectedCounts = new int[alphabet + 1];
        final int[] expectedFirst = new int[alphabet + 1];
        Arrays.fill(expectedFirst, -1);
        for (int row = 0; row < rows; row++) {
          ids[row] = random.nextInt(alphabet);
          final boolean selected = pattern == 9 || pattern == 0 || pattern != 1 && random.nextInt(pattern + 1) == 0;
          final boolean present = pattern == 9 || pattern == 2 || pattern != 3 && random.nextInt(5) != 0;
          if (!selected)
            mask[row >>> 6] &= ~(1L << (row & 63));
          if (!present)
            presence[row >>> 6] &= ~(1L << (row & 63));
          if (selected) {
            final int group = present
                ? ids[row]
                : alphabet;
            if (expectedCounts[group]++ == 0)
              expectedFirst[group] = row;
          }
        }
        final PackedDictionaryIds packed = packed(width, ids);
        final int[] counts = new int[alphabet + 1];
        final int[] first = new int[alphabet + 1];
        Arrays.fill(first, -1);
        packed.countSelected(alphabet, mask, presence, counts, first);
        assertArrayEquals(expectedCounts, counts, "width=" + width + " rows=" + rows + " pattern=" + pattern);
        assertArrayEquals(expectedFirst, first);
        assertFalse(packed.isMaterialized(), "counting must never allocate the dense ID lane");
        // A second fold adds counts while preserving each group's first observed row.
        packed.countSelected(alphabet, mask, presence, counts, first);
        for (int group = 0; group < expectedCounts.length; group++)
          expectedCounts[group] *= 2;
        assertArrayEquals(expectedCounts, counts);
        assertArrayEquals(expectedFirst, first);
      }
    }
  }

  @Test
  void invalidSelectedIdsFailWhileMissingOrExcludedCellsDoNotContribute() {
    final PackedDictionaryIds packed = packed(2, new int[] {0, 1, 3, 2});
    assertThrows(IllegalStateException.class,
        () -> packed.countSelected(3, new long[] {15}, new long[] {15}, new int[4], new int[4]));
    final int[] counts = new int[4];
    packed.countSelected(3, new long[] {15}, new long[] {11}, counts, new int[4]);
    assertArrayEquals(new int[] {1, 1, 1, 1}, counts);
    Arrays.fill(counts, 0);
    packed.countSelected(3, new long[] {11}, new long[] {15}, counts, new int[4]);
    assertArrayEquals(new int[] {1, 1, 1, 0}, counts);
    final PackedDictionaryIds constant = packed(0, new int[] {0, 0});
    assertThrows(IllegalStateException.class,
        () -> constant.countSelected(0, new long[] {3}, new long[] {3}, new int[1], new int[1]));
    final int[] missing = new int[1];
    constant.countSelected(0, new long[] {-1}, new long[] {0}, missing, new int[1]);
    assertArrayEquals(new int[] {2}, missing);
    assertThrows(IllegalArgumentException.class,
        () -> packed.countSelected(4, new long[0], new long[] {15}, new int[5], new int[5]));
  }

  private static PackedDictionaryIds packed(final int width, final int[] ids) {
    final byte[] bytes = new byte[1 + ((ids.length * width + 7) >>> 3)];
    bytes[0] = (byte) width;
    if (width > 0) {
      for (int row = 0; row < ids.length; row++) {
        final int bit = row * width;
        bytes[1 + (bit >>> 3)] |= (byte) (ids[row] << (bit & 7));
      }
    }
    return new PackedDictionaryIds(bytes, 1, ids.length, width);
  }
}
