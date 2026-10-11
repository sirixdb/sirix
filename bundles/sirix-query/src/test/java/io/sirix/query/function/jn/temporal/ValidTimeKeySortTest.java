package io.sirix.query.function.jn.temporal;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

final class ValidTimeKeySortTest {
  @Test
  void matchesIndependentComparisonSortAcrossPrefixesDuplicatesAndStorageExtremes() {
    final Random random = new Random(9237621);
    for (final int length : new int[] {0, 1, 4095, 4096, 5000, 17000}) {
      for (final long mask : new long[] {0, 255, (1L << 22) - 1, (1L << 48) - 1, Long.MAX_VALUE}) {
        for (final long prefix : new long[] {0, 1L << 47, Long.MIN_VALUE}) {
          final long[] keys = new long[length];
          for (int row = 0; row < keys.length; row++) {
            keys[row] = prefix | (random.nextLong() & mask);
          }
          final long[] expected = keys.clone();
          Arrays.sort(expected);
          ValidTimeKeySort.sort(keys);
          assertArrayEquals(expected, keys);
        }
      }
    }
  }

  @Test
  void handlesOrderedDescendingAndRepeatedChunkBoundaries() {
    final long[] keys = new long[8000];
    final long[] boundaries = {0, 65535, 65536, (1L << 47) + 3, (1L << 48) - 1};
    for (int row = 0; row < keys.length; row++) {
      keys[row] = boundaries[row % boundaries.length];
    }
    final long[] expected = keys.clone();
    Arrays.sort(expected);
    ValidTimeKeySort.sort(keys);
    assertArrayEquals(expected, keys);
    ValidTimeKeySort.sort(keys);
    assertArrayEquals(expected, keys);
    for (int row = 0; row < keys.length; row++) {
      keys[row] = (long) keys.length - row;
    }
    final long[] descendingExpected = keys.clone();
    Arrays.sort(descendingExpected);
    ValidTimeKeySort.sort(keys);
    assertArrayEquals(descendingExpected, keys);
  }
}
