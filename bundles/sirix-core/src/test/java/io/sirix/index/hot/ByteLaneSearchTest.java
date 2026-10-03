/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class ByteLaneSearchTest {
  @Test
  void everyBytePairAndLiveLaneCountMatchesScalarOracle() {
    final byte[] entries = new byte[8];
    for (int stored = 0; stored < 256; stored++) {
      entries[0] = (byte) stored;
      entries[1] = 0;
      entries[2] = 1;
      entries[3] = 127;
      entries[4] = (byte) 128;
      entries[5] = (byte) 255;
      entries[6] = (byte) (stored + 1);
      entries[7] = (byte) ~stored;
      for (int query = 0; query < 256; query++) {
        int equal = 0;
        int subset = 0;
        for (int count = 0; count <= entries.length; count++) {
          if (count > 0) {
            final int entry = entries[count - 1] & 255;
            if (entry == query)
              equal |= 1 << (count - 1);
            if ((entry & query) == entry)
              subset |= 1 << (count - 1);
          }
          assertEquals(equal, ByteLaneSearch.equal(entries, count, (byte) query));
          assertEquals(subset, ByteLaneSearch.subset(entries, count, (byte) query));
        }
      }
    }
  }

  @Test
  void sparseSearchPreservesMasksAcrossTheMachineWordBoundary() {
    for (int count = 0; count <= SparsePartialKeys.MAX_ENTRIES; count++) {
      final SparsePartialKeys<Byte> keys = SparsePartialKeys.forBytes(count);
      for (int row = 0; row < count; row++)
        keys.setEntry(row, (byte) (row * 19));
      for (int key = 0; key < 256; key++) {
        int expected = 0;
        for (int row = 0; row < count; row++) {
          final int entry = (row * 19) & 255;
          if ((key & entry) == entry)
            expected |= 1 << row;
        }
        assertEquals(expected, keys.search(key));
      }
    }
  }

  @Test
  void invalidLaneCountsAndShortStorageAreRejected() {
    assertThrows(IllegalArgumentException.class, () -> ByteLaneSearch.equal(new byte[8], -1, (byte) 0));
    assertThrows(IllegalArgumentException.class, () -> ByteLaneSearch.subset(new byte[8], 9, (byte) 0));
    assertThrows(IndexOutOfBoundsException.class, () -> ByteLaneSearch.equal(new byte[7], 7, (byte) 0));
  }
}
