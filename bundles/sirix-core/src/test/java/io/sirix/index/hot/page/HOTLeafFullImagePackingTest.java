/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot.page;

import io.sirix.page.PagePersister;
import io.sirix.page.SerializationType;
import io.sirix.access.ResourceConfiguration;
import io.sirix.index.IndexType;
import io.sirix.node.ByteArrayBytesIn;
import io.sirix.node.Bytes;
import io.sirix.node.MemorySegmentBytesOut;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Packed leaf images; the work-counter tests read process-wide counters, hence isolated. */
@Isolated
final class HOTLeafFullImagePackingTest {
  @Test
  void packedCopyOwnsTheDirectoryAndPayload() {
    final byte[] frame = {0, 0, 4, 0, 1, 7, 1, 42, 1, 9, 1, 51};
    final HOTLeafBody original = HOTLeafBody.read(new ByteArrayBytesIn(frame), 3, 2, 8);
    final byte[] destination = new byte[frame.length + 1];
    Arrays.fill(destination, (byte) -1);
    final HOTLeafBody copy = original.copyTo(MemorySegment.ofArray(destination));
    assertArrayEquals(frame, Arrays.copyOf(destination, frame.length));
    assertEquals(-1, destination[frame.length]);
    frame[11] = 99;
    assertArrayEquals(new byte[] {51}, copy.copyValue(copy.valueRef(1)));
    assertArrayEquals(new byte[] {99}, original.copyValue(original.valueRef(1)));
  }

  @Test
  void finalPackedWordCanUseABoundedShortReadOrAnAlignedBackRead() {
    for (int prefix = 0; prefix < Long.BYTES; prefix++) {
      for (final int count : new int[] {1, 3, 9}) {
        final int suffix = Long.BYTES - prefix;
        final int slotSize = suffix + 2;
        final ByteBuffer frame = ByteBuffer.allocate(count * (Short.BYTES + slotSize)).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < count; i++) {
          frame.putShort((short) (i * slotSize));
        }
        final byte[][] keys = new byte[count][Long.BYTES];
        for (int i = 0; i < count; i++) {
          Arrays.fill(keys[i], (byte) 0x7e);
          keys[i][Long.BYTES - 1] = (byte) (0x80 + i * 2);
          frame.put((byte) suffix).put(keys[i], prefix, suffix).put((byte) 0);
        }
        final HOTLeafBody body = HOTLeafBody.read(new ByteArrayBytesIn(frame.array()), 3, count, count * slotSize);
        body.validate(prefix, IndexType.PROJECTION);
        for (int i = 0; i < count; i++) {
          assertEquals(i, body.findLongSuffix(keys[i], prefix));
          final byte[] next = keys[i].clone();
          next[Long.BYTES - 1]++;
          assertEquals(-i - 2, body.findLongSuffix(next, prefix));
        }
        for (int i = 0; i < count; i++) {
          assertEquals(body.checkedValueRef(i), body.valueRef(i));
        }
      }
    }
  }

  @Test
  void packedWordLookupHonorsLengthAndGroupPrefix() {
    final byte[] mixed = {0, 0, 9, 0, 7, 1, 2, 3, 4, 5, 6, 7, 0, 8, 1, 2, 3, 4, 5, 6, 7, 1, 0};
    final HOTLeafBody widths = HOTLeafBody.read(new ByteArrayBytesIn(mixed), 3, 2, 19);
    widths.validate(0, IndexType.PROJECTION);
    assertEquals(-2, widths.findLongSuffix(new byte[] {1, 2, 3, 4, 5, 6, 7, 0}, 0));
    assertTrue(widths.compareKeyTail(1, new byte[] {1, 2, 3, 4, 5, 6, 7, 2}, 0, Long.BYTES) < 0);

    final byte[] grouped = {0, 0, 1, 0, 0, 1, 8, 2, 3, 4, 5, 6, 7, 8, 9, 0};
    final HOTLeafBody prefixed = HOTLeafBody.read(new ByteArrayBytesIn(grouped), 11, 1, 10);
    prefixed.validate(0, IndexType.PROJECTION);
    assertEquals(-2, prefixed.findLongSuffix(new byte[] {2, 3, 4, 5, 6, 7, 8, 9}, 0));

    final byte[] equalUntilEnd = {0, 0, 3, 0, 1, 7, 0, 2, 7, 0, 0};
    final HOTLeafBody prefixes = HOTLeafBody.read(new ByteArrayBytesIn(equalUntilEnd), 3, 2, 7);
    prefixes.validate(0, IndexType.PROJECTION);
    assertEquals(Byte.SIZE, prefixes.differingBit(0, 1));
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void completeImageContainsOnlyLiveRecordsWithoutMutatingSource(final VersioningType type) throws IOException {
    for (final int window : new int[] {3, 32}) {
      final ResourceConfiguration config = ResourceConfiguration.newBuilder("packing")
                                                                .versioningApproach(type)
                                                                .maxNumberOfRevisionsToRestore(window)
                                                                .build();
      try (HOTLeafPage leaf = new HOTLeafPage(7, 1, IndexType.PROJECTION)) {
        for (int i = 0; i < 4; i++) {
          assertTrue(leaf.put(new byte[] {(byte) i}, new byte[16]));
        }
        assertTrue(leaf.updateValue(0, new byte[64]));
        assertTrue(leaf.updateValue(1, new byte[3]));
        assertTrue(leaf.deleteAt(2));
        leaf.setCompleteDump(true);
        final byte[] before = leaf.slots().asSlice(0, leaf.getUsedSlotsSize()).toArray(ValueLayout.JAVA_BYTE);
        final int[] offsets = new int[leaf.getEntryCount()];
        int liveSize = 0;
        for (int i = 0; i < offsets.length; i++) {
          offsets[i] = leaf.getSlotOffset(i);
          liveSize += leaf.getSlotAsByteArray(i).length;
        }
        assertTrue(before.length > liveSize, "fixture must leave retired growth/shrink/delete records");
        try (MemorySegmentBytesOut sink = new MemorySegmentBytesOut(1 << 16)) {
          new PagePersister().serializePage(config, sink, leaf, SerializationType.DATA);
          final byte[] wire = sink.toByteArray();
          // kind/version/flags + varlong page key + revision + index kind + prefix length + counts
          assertEquals(HOTLeafPage.FLAG_NARROW_SLOT_OFFSETS, wire[2] & HOTLeafPage.FLAG_NARROW_SLOT_OFFSETS);
          assertEquals(19 + 1 + leaf.getCommonPrefixLen() + 2 * offsets.length + liveSize - 2 * offsets.length,
              wire.length);
          try (HOTLeafPage decoded = (HOTLeafPage) new PagePersister().deserializePage(config, Bytes.wrapForRead(wire),
              SerializationType.DATA)) {
            assertEquals(liveSize - 2 * offsets.length, decoded.getUsedSlotsSize());
            assertTrue(decoded.isCompleteDump());
            int offset = 0;
            for (int i = 0; i < offsets.length; i++) {
              assertEquals(offset, decoded.getSlotOffset(i));
              assertArrayEquals(leaf.getKey(i), decoded.getKey(i));
              assertArrayEquals(leaf.copyStoredValue(i), decoded.copyStoredValue(i));
              offset += decoded.getSlotAsByteArray(i).length - 2;
            }
          }
          assertArrayEquals(before, leaf.slots().asSlice(0, leaf.getUsedSlotsSize()).toArray(ValueLayout.JAVA_BYTE));
          for (int i = 0; i < offsets.length; i++) {
            assertEquals(offsets[i], leaf.getSlotOffset(i));
            assertTrue(leaf.isEntryDirty(i));
          }
        }

      }
      try (HOTLeafPage empty = new HOTLeafPage(7, 1, IndexType.PROJECTION);
          MemorySegmentBytesOut sink = new MemorySegmentBytesOut(1 << 16)) {
        new PagePersister().serializePage(config, sink, empty, SerializationType.DATA);
        try (HOTLeafPage decoded = (HOTLeafPage) new PagePersister().deserializePage(config,
            Bytes.wrapForRead(sink.toByteArray()), SerializationType.DATA)) {
          assertEquals(0, decoded.getEntryCount());
          assertEquals(0, decoded.getUsedSlotsSize());
        }
      }

    }
  }

  /**
   * The suffix-probe counter's contract: one lane per word or residual-byte load. A fixed-width
   * suffix is read with one 8-byte load whenever an 8-byte window starting at the suffix, or ending
   * at its end, lies inside the slot region; otherwise with one load per set bit of its width (int,
   * short, byte). Other suffixes are compared word by word, then byte by byte, up to the first
   * difference. The search is the ordinary binary search over the sorted slots.
   */
  @Test
  void packedWordLookupCountsOneLanePerLoad() {
    assumeTrue(VersioningType.hotMergeDiagEnabled(), "suffix probes are counted with sirix.hot.mergeDiag");
    for (int prefix = 0; prefix < Long.BYTES; prefix++) {
      for (final int count : new int[] {1, 3, 9}) {
        final byte[][] keys = new byte[count][Long.BYTES];
        final byte[][] suffixes = new byte[count][];
        for (int i = 0; i < count; i++) {
          Arrays.fill(keys[i], (byte) 0x7e);
          keys[i][Long.BYTES - 1] = (byte) (0x80 + i * 2);
          suffixes[i] = Arrays.copyOfRange(keys[i], prefix, Long.BYTES);
        }
        assertLookupWork(suffixes, keys, prefix);
      }
    }
    // Mixed suffix widths take the compared-range path; keys differ only in their last bytes.
    for (final int prefix : new int[] {0, 1, 3}) {
      final byte[][] stored = {bytes(prefix, 5, 0x10), bytes(prefix, 8, 0x20), bytes(prefix, 8, 0x30),
          bytes(prefix, 9, 0x30), bytes(prefix, 9, 0x40), bytes(prefix, 12, 0x50)};
      final byte[][] probes = new byte[stored.length][];
      for (int i = 0; i < stored.length; i++) {
        probes[i] = Arrays.copyOf(stored[i], Long.BYTES);
      }
      assertLookupWork(Arrays.stream(stored).map(key -> Arrays.copyOfRange(key, prefix, key.length))
                             .toArray(byte[][]::new), probes, prefix);
    }
  }

  /** One-byte keys on a heap frame: one lane per probed slot with a nonempty key tail. */
  @Test
  void borrowedOneByteLookupCountsOneLanePerProbedSlot() {
    assumeTrue(VersioningType.hotMergeDiagEnabled(), "suffix probes are counted with sirix.hot.mergeDiag");
    final byte[][] suffixes = {{10}, {20}, {30}, {40}, {50}};
    final byte[] frame = frame(suffixes);
    for (int wanted = 5; wanted <= 55; wanted += 5) {
      int low = 0;
      int high = suffixes.length - 1;
      int steps = 0;
      while (low <= high) {
        final int middle = (low + high) >>> 1;
        steps++;
        final int comparison = Byte.toUnsignedInt(suffixes[middle][0]) - wanted;
        if (comparison < 0) {
          low = middle + 1;
        } else if (comparison > 0) {
          high = middle - 1;
        } else {
          break;
        }
      }
      final long before = HOTLeafPage.suffixProbeReads();
      HOTLeafBody.selectBorrowed(new ByteArrayBytesIn(frame), MODE, suffixes.length, used(suffixes),
          new byte[] {(byte) wanted}, 0, true);
      assertEquals(steps, HOTLeafPage.suffixProbeReads() - before, "lanes for key " + wanted);
    }
  }

  private static final int MODE = HOTLeafBody.KEY_U8 | HOTLeafBody.VALUE_U8;

  /** Key whose bytes after {@code prefix} are equal up to its last byte, which is {@code last}. */
  private static byte[] bytes(final int prefix, final int suffixLength, final int last) {
    final byte[] key = new byte[prefix + suffixLength];
    Arrays.fill(key, (byte) 0x61);
    key[key.length - 1] = (byte) last;
    return key;
  }

  /** Directory and slots ([u8 suffix length][suffix][u8 value length 0]) of a mode-3 body. */
  private static byte[] frame(final byte[][] suffixes) {
    final ByteBuffer frame =
        ByteBuffer.allocate(suffixes.length * Short.BYTES + used(suffixes)).order(ByteOrder.LITTLE_ENDIAN);
    int offset = 0;
    for (final byte[] suffix : suffixes) {
      frame.putShort((short) offset);
      offset += suffix.length + 2;
    }
    for (final byte[] suffix : suffixes) {
      frame.put((byte) suffix.length).put(suffix).put((byte) 0);
    }
    return frame.array();
  }

  private static int used(final byte[][] suffixes) {
    int used = 0;
    for (final byte[] suffix : suffixes) {
      used += suffix.length + 2;
    }
    return used;
  }

  private static void assertLookupWork(final byte[][] suffixes, final byte[][] keys, final int prefix) {
    final HOTLeafBody body = HOTLeafBody.read(new ByteArrayBytesIn(frame(suffixes)), MODE, suffixes.length,
        used(suffixes));
    body.validate(prefix, IndexType.PROJECTION);
    for (final byte[] key : keys) {
      for (final byte[] probe : new byte[][] {key, successor(key)}) {
        final long before = HOTLeafPage.suffixProbeReads();
        final int found = body.findLongSuffix(probe, prefix);
        final long lanes = HOTLeafPage.suffixProbeReads() - before;
        final long[] expected = expectedSearch(suffixes, probe, prefix);
        assertEquals(expected[0], found, "slot for " + Arrays.toString(probe) + " prefix " + prefix);
        assertEquals(expected[1], lanes, "lanes for " + Arrays.toString(probe) + " prefix " + prefix);
      }
    }
  }

  private static byte[] successor(final byte[] key) {
    final byte[] next = key.clone();
    next[Long.BYTES - 1]++;
    return next;
  }

  /** {slot or -(insertion)-1, lanes} of the binary search the counter contract describes. */
  private static long[] expectedSearch(final byte[][] suffixes, final byte[] key, final int prefix) {
    final int wanted = Long.BYTES - prefix;
    final byte[] tail = Arrays.copyOfRange(key, prefix, Long.BYTES);
    final int used = used(suffixes);
    long lanes = 0;
    int low = 0;
    int high = suffixes.length;
    while (low < high) {
      final int middle = (low + high) >>> 1;
      final byte[] stored = suffixes[middle];
      int start = 1;
      for (int i = 0; i < middle; i++) {
        start += suffixes[i].length + 2;
      }
      final int comparison;
      if (stored.length == wanted) {
        final boolean oneWord = start + Long.BYTES <= used || start + wanted - Long.BYTES >= 0;
        lanes += oneWord
            ? 1
            : Integer.bitCount(wanted);
        comparison = Arrays.compareUnsigned(stored, tail);
      } else {
        final int length = Math.min(stored.length, wanted);
        lanes += comparedLanes(stored, tail, length);
        final int prefixComparison = Arrays.compareUnsigned(stored, 0, length, tail, 0, length);
        comparison = prefixComparison == 0
            ? stored.length - wanted
            : prefixComparison;
      }
      if (comparison < 0) {
        low = middle + 1;
      } else if (comparison > 0) {
        high = middle;
      } else {
        return new long[] {middle, lanes};
      }
    }
    return new long[] {-low - 1, lanes};
  }

  /** Word loads, then byte loads, up to and including the first differing lane. */
  private static int comparedLanes(final byte[] stored, final byte[] key, final int length) {
    int lanes = 0;
    int i = 0;
    for (; i + Long.BYTES <= length; i += Long.BYTES) {
      lanes++;
      if (!Arrays.equals(stored, i, i + Long.BYTES, key, i, i + Long.BYTES)) {
        return lanes;
      }
    }
    for (; i < length; i++) {
      lanes++;
      if (stored[i] != key[i]) {
        return lanes;
      }
    }
    return lanes;
  }
}
