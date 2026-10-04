/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.index.IndexType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises unsigned masked suffix comparisons and the exact-segment tail boundary. */
final class HOTLongSuffixSearchTest {
  private static final int ENTRIES = 33;

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 3, 4, 5, 6, 7})
  void searchesEveryPrefixLengthWithPaddedAndExactTail(final int prefixLength) {
    for (final int padding : new int[] {0, Long.BYTES}) {
      final int suffixLength = Long.BYTES - prefixLength;
      final int slotSize = Short.BYTES + suffixLength + Short.BYTES;
      final byte[] prefix = new byte[prefixLength];
      Arrays.fill(prefix, (byte) 0xa3);
      final int[] offsets = new int[ENTRIES];
      final byte[] slots = new byte[ENTRIES * slotSize + padding];
      Arrays.fill(slots, (byte) 0xe7);
      for (int row = 0; row < ENTRIES; row++) {
        final int offset = row * slotSize;
        offsets[row] = offset;
        slots[offset] = (byte) suffixLength;
        slots[offset + 1] = 0;
        Arrays.fill(slots, offset + Short.BYTES, offset + Short.BYTES + suffixLength, (byte) 0x91);
        slots[offset + Short.BYTES + suffixLength - 1] = (byte) (128 + row * 2);
        slots[offset + Short.BYTES + suffixLength] = 0;
        slots[offset + Short.BYTES + suffixLength + 1] = 0;
      }
      try (Arena arena = Arena.ofConfined()) {
        final MemorySegment segment = arena.allocate(slots.length);
        MemorySegment.copy(slots, 0, segment, ValueLayout.JAVA_BYTE, 0, slots.length);
        try (HOTLeafPage page = new HOTLeafPage(1, 65, IndexType.PROJECTION, segment, null, offsets, ENTRIES,
            ENTRIES * slotSize, prefix, prefixLength)) {
          for (int row = 0; row < ENTRIES; row++) {
            final byte[] key = key(prefix, 128 + row * 2);
            assertEquals(row, page.findEntry(key));
            final byte[] oversized = Arrays.copyOf(key, key.length + 5);
            Arrays.fill(oversized, key.length, oversized.length, (byte) 0xff);
            assertEquals(row, page.findEntry(oversized, key.length));
            assertArrayEquals(new byte[0], page.copyStoredValue(row));
            assertEquals(-(row + 2), page.findEntry(key(prefix, 129 + row * 2)));
          }
          if (prefixLength == 0) {
            final byte[] unsignedBefore = key(prefix, 128);
            unsignedBefore[0] = 0x7f;
            assertEquals(-1, page.findEntry(unsignedBefore));
            final byte[] unsignedAfter = key(prefix, 128);
            unsignedAfter[0] = (byte) 0xff;
            assertEquals(-(ENTRIES + 1), page.findEntry(unsignedAfter));
          }
          final byte[] below = key(prefix, 127);
          assertEquals(-1, page.findEntry(below));
          final byte[] shorter = Arrays.copyOf(key(prefix, 128), Long.BYTES - 1);
          assertEquals(-1, page.findEntry(shorter));
          final byte[] longer = Arrays.copyOf(key(prefix, 128), Long.BYTES + 1);
          assertEquals(-2, page.findEntry(longer));
        }
      }
    }
  }

  @Test
  void eightByteProbesKeepOrderingAmongShorterAndLongerStoredKeys() {
    final byte[][] keys = new byte[ENTRIES][];
    for (int group = 0; group < ENTRIES / 3; group++) {
      final byte[] base = new byte[Long.BYTES];
      Arrays.fill(base, (byte) 0x81);
      base[6] = (byte) group;
      for (int length = 7; length <= 9; length++) {
        keys[group * 3 + length - 7] = Arrays.copyOf(base, length);
      }
    }
    try (HOTLeafPage page = new HOTLeafPage(1, 65, IndexType.PROJECTION)) {
      for (int row = 0; row < keys.length; row++) {
        assertTrue(page.put(keys[row], new byte[] {(byte) row}));
      }
      for (int row = 0; row < keys.length; row++) {
        assertEquals(row, page.findEntry(keys[row]));
        assertArrayEquals(new byte[] {(byte) row}, page.copyStoredValue(row));
      }
    }
  }

  private static byte[] key(final byte[] prefix, final int last) {
    final byte[] key = new byte[Long.BYTES];
    Arrays.fill(key, (byte) 0x91);
    System.arraycopy(prefix, 0, key, 0, prefix.length);
    key[key.length - 1] = (byte) last;
    return key;
  }
}
