/*
 * Copyright (c) 2024, Sirix Contributors
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.page;

import io.sirix.index.IndexType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Capacity contract of the prefix-shrinking insert paths of {@link HOTLeafPage}.
 *
 * <p>
 * A leaf stores one common prefix and per-entry suffixes. A key that shares fewer bytes with the
 * prefix shortens it, so <em>every</em> resident entry grows by the reclaimed bytes. The rebuilt
 * image can therefore exceed the leaf's byte budget even though the pending entry alone is tiny.
 * The leaf must then report "does not fit" - the signal its callers already turn into a split, a
 * skipped consolidation or a multi-page rebuild - and stay exactly as it was.
 * </p>
 *
 * <p>
 * The geometry mirrors the production failure this guards against (a valid-time interval index
 * whose posting values were 1163 bytes): a few dozen large values fill a leaf, so a handful of
 * reclaimed prefix bytes per entry is enough to tip the rebuilt image over 64 KiB.
 * </p>
 */
class HOTLeafPagePrefixShrinkCapacityTest {

  private static final int PAGE_SIZE = HOTLeafPage.DEFAULT_SIZE;

  /** Bytes shared by every resident key; all of them are reclaimed by the outlier key. */
  private static final int SHARED_PREFIX_LEN = 40;

  /** Bytes after the shared prefix: a distinguishing ordinal byte and one filler byte. */
  private static final int ORDINAL_LEN = 2;

  /** Value size of the production failure ({@code 2 + valueLen == 1165} in its stack trace). */
  private static final int VALUE_LEN = 1163;

  /** {@code [u16 suffixLen][suffix][u16 valueLen][value]} once the prefix has settled. */
  private static final int RESIDENT_ENTRY_SIZE = 2 + ORDINAL_LEN + 2 + VALUE_LEN;

  /** As many residents as fit the frame: 56 entries, 72 bytes to spare. */
  private static final int RESIDENT_COUNT = PAGE_SIZE / RESIDENT_ENTRY_SIZE;

  private Arena arena;
  private HOTLeafPage leaf;

  @BeforeEach
  void setUp() {
    arena = Arena.ofConfined();
    leaf = new HOTLeafPage(1L, 1, IndexType.VALIDTIME, arena.allocate(PAGE_SIZE), null,
        new int[HOTLeafPage.MAX_ENTRIES], 0, 0);
  }

  @AfterEach
  void tearDown() {
    if (leaf != null && !leaf.isClosed()) {
      leaf.close();
    }
    if (arena != null) {
      arena.close();
    }
  }

  /** The insert entry points that establish or shorten the common prefix. */
  enum InsertPath {
    PUT {
      @Override
      boolean insert(final HOTLeafPage leaf, final byte[] key, final byte[] value) {
        return leaf.put(key, value);
      }
    },
    PUT_RANGE_ARRAY {
      @Override
      boolean insert(final HOTLeafPage leaf, final byte[] key, final byte[] value) {
        return leaf.putRange(key, value, 0, value.length);
      }
    },
    PUT_RANGE_SEGMENT {
      @Override
      boolean insert(final HOTLeafPage leaf, final byte[] key, final byte[] value) {
        return leaf.putRange(key, MemorySegment.ofArray(value), 0L, value.length);
      }
    },
    PUT_OR_REPLACE {
      @Override
      boolean insert(final HOTLeafPage leaf, final byte[] key, final byte[] value) {
        return leaf.putOrReplace(key, value);
      }
    },
    MERGE_WITH_NODE_REFS {
      @Override
      boolean insert(final HOTLeafPage leaf, final byte[] key, final byte[] value) {
        return leaf.mergeWithNodeRefs(key, key.length, value, value.length);
      }
    },
    MERGE_WITH_NODE_REFS_STRICT {
      @Override
      boolean insert(final HOTLeafPage leaf, final byte[] key, final byte[] value) {
        return leaf.mergeWithNodeRefsStrict(key, key.length, value, value.length) == 1;
      }
    };

    abstract boolean insert(HOTLeafPage leaf, byte[] key, byte[] value);
  }

  @Test
  @DisplayName("put reports 'does not fit' when shortening the prefix would overflow the leaf")
  void putRefusesPrefixShrinkThatOverflowsTheLeaf() {
    final byte[][] keys = fillWithResidents(RESIDENT_COUNT);
    final LeafImage before = LeafImage.of(leaf);

    // Shares no byte with the 40-byte prefix: all 56 residents would grow by 40 bytes (2240 in
    // total) while only 72 bytes are free.
    assertFalse(leaf.put(outlierKey(), value(RESIDENT_COUNT)),
        "a prefix shrink that cannot fit must be reported as 'leaf full', never written past the buffer");

    before.assertUnchanged(leaf);
    assertResidentsSearchable(keys);
  }

  @ParameterizedTest
  @EnumSource(InsertPath.class)
  @DisplayName("every prefix-shrinking insert path refuses an overflowing rebuild and leaves the leaf intact")
  void everyInsertPathRefusesAnOverflowingPrefixShrink(final InsertPath path) {
    final byte[][] keys = fillWithResidents(RESIDENT_COUNT);
    final LeafImage before = LeafImage.of(leaf);

    assertFalse(path.insert(leaf, outlierKey(), value(RESIDENT_COUNT)));

    before.assertUnchanged(leaf);
    assertResidentsSearchable(keys);
  }

  @ParameterizedTest
  @EnumSource(InsertPath.class)
  @DisplayName("a refused prefix shrink leaves a leaf that still accepts keys under its prefix")
  void refusedLeafRemainsWritable(final InsertPath path) {
    // One resident fewer: the rebuild still overflows (55 * 40 = 2200 > 1241 free bytes), but a
    // further key under the existing prefix fits.
    final int residents = RESIDENT_COUNT - 1;
    final byte[][] keys = fillWithResidents(residents);

    assertFalse(path.insert(leaf, outlierKey(), value(residents)));
    assertTrue(path.insert(leaf, residentKey(residents), value(residents)),
        "the refusal must not consume space or disturb the prefix");

    assertEquals(residents + 1, leaf.getEntryCount());
    assertEquals(SHARED_PREFIX_LEN, leaf.getCommonPrefixLen());
    assertResidentsSearchable(keys);
    assertArrayEquals(value(residents), leaf.copyStoredValue(leaf.findEntry(residentKey(residents))));
  }

  @Test
  @DisplayName("a prefix shrink that fits exactly is performed and stores the key")
  void prefixShrinkThatFitsExactlySucceeds() {
    // n residents of 1169 bytes grow by 40 each; the outlier stores its whole 42-byte key as the
    // suffix. Pick the largest n whose rebuilt image plus the pending entry still fits, then size the
    // pending value so the frame is filled to the last byte.
    final int grownEntrySize = RESIDENT_ENTRY_SIZE + SHARED_PREFIX_LEN;
    final int residents = (PAGE_SIZE - (2 + SHARED_PREFIX_LEN + ORDINAL_LEN + 2)) / grownEntrySize;
    final byte[][] keys = fillWithResidents(residents);
    final byte[] outlier = outlierKey();
    final int exactValueLen = PAGE_SIZE - residents * grownEntrySize - (2 + outlier.length + 2);
    final byte[] exactValue = filled(exactValueLen, (byte) 0x5A);

    assertTrue(leaf.put(outlier, exactValue), "a rebuilt image that fills the frame exactly still fits");

    assertEquals(residents + 1, leaf.getEntryCount());
    assertEquals(0, leaf.getCommonPrefixLen(), "the outlier shares no byte with the residents");
    assertEquals(0L, leaf.getRemainingSpace());
    assertResidentsSearchable(keys);
    assertArrayEquals(exactValue, leaf.copyStoredValue(leaf.findEntry(outlier)));
  }

  @Test
  @DisplayName("one byte beyond an exact fit is refused without shortening the prefix")
  void prefixShrinkOneByteOverIsRefused() {
    final int grownEntrySize = RESIDENT_ENTRY_SIZE + SHARED_PREFIX_LEN;
    final int residents = (PAGE_SIZE - (2 + SHARED_PREFIX_LEN + ORDINAL_LEN + 2)) / grownEntrySize;
    final byte[][] keys = fillWithResidents(residents);
    final byte[] outlier = outlierKey();
    final int exactValueLen = PAGE_SIZE - residents * grownEntrySize - (2 + outlier.length + 2);
    final LeafImage before = LeafImage.of(leaf);

    // The rebuilt residents alone would fit; together with the pending entry they do not. The leaf
    // must not pay for (or keep) a rebuild whose key it cannot store.
    assertFalse(leaf.put(outlier, filled(exactValueLen + 1, (byte) 0x5A)));

    before.assertUnchanged(leaf);
    assertResidentsSearchable(keys);
  }

  @Test
  @DisplayName("dead bytes do not count against a prefix shrink: the rebuild repacks the leaf")
  void prefixShrinkIgnoresDeadBytes() {
    final int residents = 28;
    final byte[][] keys = fillWithResidents(residents);
    // Grow every value by one byte: each update appends a 1170-byte replacement and strands the
    // 1169-byte original, so the heap's high-water mark reaches 28 * (1169 + 1170) = 65492 of 65536
    // bytes while only 28 * 1170 = 32760 of them are live.
    final byte[] grown = filled(VALUE_LEN + 1, (byte) 0x33);
    for (int i = 0; i < residents; i++) {
      assertTrue(leaf.updateValue(leaf.findEntry(keys[i]), grown));
    }
    assertTrue(leaf.getRemainingSpace() < 64, "the fixture must leave the heap fragmented to the brim");
    final byte[] outlier = outlierKey();

    // Rebuilt image: 28 * 1210 = 33880 bytes, plus the 1209-byte pending entry.
    assertTrue(leaf.put(outlier, value(residents)),
        "live bytes, not the fragmented high-water mark, decide whether the rebuilt image fits");

    assertEquals(residents + 1, leaf.getEntryCount());
    assertEquals(0, leaf.getCommonPrefixLen());
    for (int i = 0; i < residents; i++) {
      final int index = leaf.findEntry(keys[i]);
      assertTrue(index >= 0, "resident " + i + " must survive the rebuild");
      assertArrayEquals(keys[i], leaf.getKey(index));
      assertArrayEquals(grown, leaf.copyStoredValue(index));
    }
    assertArrayEquals(value(residents), leaf.copyStoredValue(leaf.findEntry(outlier)));
  }

  // ===== Fixtures =====

  /**
   * Insert {@code count} residents sharing a 40-byte prefix; returns their full keys in key order.
   */
  private byte[][] fillWithResidents(final int count) {
    final byte[][] keys = new byte[count][];
    for (int i = 0; i < count; i++) {
      keys[i] = residentKey(i);
      assertTrue(leaf.put(keys[i], value(i)), "resident " + i + " must fit");
    }
    assertEquals(count, leaf.getEntryCount());
    if (count > 1) {
      assertEquals(SHARED_PREFIX_LEN, leaf.getCommonPrefixLen());
    }
    return keys;
  }

  private static byte[] residentKey(final int ordinal) {
    if (ordinal < 0 || ordinal > 0xFF) {
      throw new IllegalArgumentException("resident ordinal must fit one byte: " + ordinal);
    }
    final byte[] key = new byte[SHARED_PREFIX_LEN + ORDINAL_LEN];
    Arrays.fill(key, 0, SHARED_PREFIX_LEN, (byte) 0x11);
    // The ordinal leads the suffix so residents 0 and 1 already differ at byte 40: the settled
    // prefix is exactly SHARED_PREFIX_LEN bytes, and unsigned byte order equals ordinal order.
    key[SHARED_PREFIX_LEN] = (byte) ordinal;
    key[SHARED_PREFIX_LEN + 1] = (byte) 0xA5;
    return key;
  }

  /** Same length as a resident key but differing in the very first byte: LCP with the prefix is 0. */
  private static byte[] outlierKey() {
    final byte[] key = new byte[SHARED_PREFIX_LEN + ORDINAL_LEN];
    Arrays.fill(key, (byte) 0x7F);
    return key;
  }

  private static byte[] value(final int seed) {
    return filled(VALUE_LEN, (byte) (0x40 + seed));
  }

  private static byte[] filled(final int length, final byte fill) {
    final byte[] bytes = new byte[length];
    Arrays.fill(bytes, fill);
    return bytes;
  }

  private void assertResidentsSearchable(final byte[][] keys) {
    for (int i = 0; i < keys.length; i++) {
      assertEquals(i, leaf.findEntry(keys[i]), "resident " + i + " must stay addressable at its slot");
      assertArrayEquals(keys[i], leaf.getKey(i));
      assertArrayEquals(value(i), leaf.copyStoredValue(i));
    }
  }

  /**
   * Every observable property of a leaf, captured so a refused insert can be proven side-effect free.
   */
  private record LeafImage(int entryCount, byte[] prefix, int prefixLen, long remainingSpace, byte[][] keys,
      byte[][] values, int[] slotOffsets) {

    static LeafImage of(final HOTLeafPage leaf) {
      final int count = leaf.getEntryCount();
      final byte[][] keys = new byte[count][];
      final byte[][] values = new byte[count][];
      final int[] offsets = new int[count];
      for (int i = 0; i < count; i++) {
        keys[i] = leaf.getKey(i);
        values[i] = leaf.copyStoredValue(i);
        offsets[i] = leaf.getSlotOffset(i);
      }
      return new LeafImage(count, leaf.getCommonPrefix().clone(), leaf.getCommonPrefixLen(), leaf.getRemainingSpace(),
          keys, values, offsets);
    }

    void assertUnchanged(final HOTLeafPage leaf) {
      assertEquals(entryCount, leaf.getEntryCount(), "entry count");
      assertEquals(prefixLen, leaf.getCommonPrefixLen(), "common prefix length");
      assertArrayEquals(Arrays.copyOf(prefix, prefixLen), Arrays.copyOf(leaf.getCommonPrefix(), prefixLen),
          "common prefix bytes");
      assertEquals(remainingSpace, leaf.getRemainingSpace(), "remaining space");
      for (int i = 0; i < entryCount; i++) {
        assertEquals(slotOffsets[i], leaf.getSlotOffset(i), "slot offset " + i);
        assertArrayEquals(keys[i], leaf.getKey(i), "key " + i);
        assertArrayEquals(values[i], leaf.copyStoredValue(i), "value " + i);
      }
    }
  }
}
