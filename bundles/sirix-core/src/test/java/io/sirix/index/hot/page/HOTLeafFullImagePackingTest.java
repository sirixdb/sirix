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
   * What a packed lookup finds, and how much counted work it may spend finding it.
   *
   * <p>
   * The result is pinned against {@link Arrays#binarySearch(Object[], Object, java.util.Comparator)}
   * over the same suffixes with {@link Arrays#compareUnsigned(byte[], byte[])}, whose ordering —
   * common prefix first, then shorter-is-less — is the HOT suffix order, and whose
   * {@code -(insertion) - 1} miss encoding is the one {@code findLongSuffix} returns. That
   * expectation is independent of any schedule: every correct binary search over a sorted array of
   * distinct keys returns it.
   *
   * <p>
   * The work is bounded, not equated, because a comparison COUNT is not specified: the textbook bound
   * on a binary search over {@code n} elements is {@code floor(log2 n) + 1} comparisons, and two
   * correct searches inside that bound legitimately probe different slots. Per comparison the
   * suffix-probe counter's contract is one lane per word or residual-byte load: a fixed-width suffix
   * costs one 8-byte load whenever an 8-byte window starting at the suffix, or ending at its end,
   * lies inside the slot region, and otherwise one load per set bit of its width; a mixed-width
   * suffix is compared word by word then byte by byte up to the first difference, so it can never
   * exceed one load per compared byte. The product of those two is the ceiling asserted here, which
   * is what rejects a lookup that scans instead of searching, or that re-reads a suffix it already
   * holds. The exact-count cases live in {@link #suffixReadAtTheSlotRegionBoundaryCostsOneLoad()} and
   * {@link #borrowedOneByteLookupCountsOneLanePerProbedSlot()}, where the schedule is pinned by a
   * singleton body and by the canonical search respectively.
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
        // Every suffix is full width, so each comparison takes the single-word arm or its
        // set-bit fallback.
        assertLookupWork(suffixes, keys, prefix, Integer.bitCount(Long.BYTES - prefix));
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
      // A compared range reads at most one load per byte it compares, and compares at most the
      // probe's own tail.
      assertLookupWork(
          Arrays.stream(stored).map(key -> Arrays.copyOfRange(key, prefix, key.length)).toArray(byte[][]::new), probes,
          prefix, Long.BYTES - prefix);
    }
  }

  /**
   * One-byte keys on a heap frame: one lane per probed slot with a nonempty key tail.
   *
   * <p>
   * A one-byte suffix is one byte load per probed slot, so the counter's contract fixes loads per
   * comparison at exactly one and the lane count becomes the comparison count itself. That count is
   * taken from {@link Arrays#binarySearch(Object[], Object, java.util.Comparator)} through a counting
   * comparator rather than from a search written here, so the expectation is the canonical binary
   * search's own work over the same keys — the equality is what would catch this path scanning a slot
   * it had no reason to read, or reading one twice.
   */
  @Test
  void borrowedOneByteLookupCountsOneLanePerProbedSlot() {
    assumeTrue(VersioningType.hotMergeDiagEnabled(), "suffix probes are counted with sirix.hot.mergeDiag");
    final byte[][] suffixes = {{10}, {20}, {30}, {40}, {50}};
    final byte[] frame = frame(suffixes);
    for (int wanted = 5; wanted <= 55; wanted += 5) {
      final byte[] tail = {(byte) wanted};
      final CanonicalSearch canonical = canonicalSearch(suffixes, tail);
      final long before = HOTLeafPage.suffixProbeReads();
      final byte[] value =
          HOTLeafBody.selectBorrowed(new ByteArrayBytesIn(frame), MODE, suffixes.length, used(suffixes), tail, 0, true);
      assertEquals(canonical.result() >= 0, value != null, "presence of key " + wanted);
      assertEquals(canonical.comparisons(), HOTLeafPage.suffixProbeReads() - before, "lanes for key " + wanted);
    }
  }

  /**
   * Suffix-read cost exactly at the slot-region boundary.
   *
   * <p>
   * The counter contract is one probe per load ({@link HOTLeafPage#suffixProbeReads()}), and a
   * full-width suffix costs ONE load whenever an eight-byte window containing it lies inside the slot
   * region — taken forward from the suffix start, or back-aligned so it ends at the suffix's last
   * byte. Only when neither window fits does the read fall back to the int-plus-short residual loads,
   * which cost {@code Integer.bitCount(wanted)}. That is the documented at-most-one-extra-step cost
   * of the long-suffix decision.
   *
   * <p>
   * The four geometries sit on each of those two boundaries and one byte outside each: a slot whose
   * forward word ends exactly at the region end (mode 3, one-byte lengths, so the suffix starts one
   * byte in), the same slot one byte shorter, a slot whose back-read starts exactly at the region
   * start (mode 2, whose two-byte key length equals the page prefix), and a mode-2 slot whose prefix
   * is wider than that length. Each geometry carries only whether an eight-byte window fits; the
   * number of loads that implies comes from the contract above, not from a count written here.
   *
   * <p>
   * These stay exact where the bounded cases in {@link #packedWordLookupCountsOneLanePerLoad()}
   * cannot: a singleton body is one comparison under every correct binary search, which
   * {@link Arrays#binarySearch(Object[], Object, java.util.Comparator)} confirms over the same
   * suffix, so the lane count is the per-comparison load count on its own.
   */
  @Test
  void suffixReadAtTheSlotRegionBoundaryCostsOneLoad() {
    assumeTrue(VersioningType.hotMergeDiagEnabled(), "suffix probes are counted with sirix.hot.mergeDiag");
    // {mode, page prefix, value length, 1 if an eight-byte window fits inside the slot region}
    for (final int[] geometry : new int[][] {{HOTLeafBody.KEY_U8 | HOTLeafBody.VALUE_U8, 2, 1, 1},
        {HOTLeafBody.KEY_U8 | HOTLeafBody.VALUE_U8, 2, 0, 0}, {HOTLeafBody.VALUE_U8, 2, 0, 1},
        {HOTLeafBody.VALUE_U8, 3, 0, 0}}) {
      final int mode = geometry[0];
      final int prefix = geometry[1];
      final int valueLength = geometry[2];
      final int wanted = Long.BYTES - prefix;
      final long expectedLoads = geometry[3] == 1
          ? 1L
          : Integer.bitCount(wanted);
      final byte[] key = bytes(prefix, wanted, 0x7a);
      final byte[] frame = singletonFrame(mode, key, prefix, valueLength);
      final int used = frame.length - Short.BYTES;
      final String where = "mode " + mode + ", prefix " + prefix + ", used " + used;
      final HOTLeafBody body = HOTLeafBody.read(new ByteArrayBytesIn(frame), mode, 1, used);
      body.validate(prefix, IndexType.PROJECTION);
      final byte[] suffix = Arrays.copyOfRange(key, prefix, key.length);
      final CanonicalSearch canonical = canonicalSearch(new byte[][] {suffix}, suffix);
      assertEquals(1, canonical.comparisons(), "a singleton body is one comparison: " + where);
      final long before = HOTLeafPage.suffixProbeReads();
      assertEquals(canonical.result(), body.findLongSuffix(key, prefix), "slot for " + where);
      assertEquals(canonical.comparisons() * expectedLoads, HOTLeafPage.suffixProbeReads() - before,
          "loads for " + where);
    }
  }

  /** One slot at offset zero: [key length][suffix][value length][value], widths from the mode. */
  private static byte[] singletonFrame(final int mode, final byte[] key, final int prefix, final int valueLength) {
    final int keyWidth = (mode & HOTLeafBody.KEY_U8) == 0
        ? Short.BYTES
        : 1;
    final int valueWidth = (mode & HOTLeafBody.VALUE_U8) == 0
        ? Short.BYTES
        : 1;
    final int suffixLength = key.length - prefix;
    final ByteBuffer frame = ByteBuffer.allocate(Short.BYTES + keyWidth + suffixLength + valueWidth + valueLength)
                                       .order(ByteOrder.LITTLE_ENDIAN);
    frame.putShort((short) 0);
    putLength(frame, keyWidth, suffixLength);
    frame.put(key, prefix, suffixLength);
    putLength(frame, valueWidth, valueLength);
    for (int i = 0; i < valueLength; i++) {
      frame.put((byte) 0x5a);
    }
    return frame.array();
  }

  private static void putLength(final ByteBuffer frame, final int width, final int length) {
    if (width == 1) {
      frame.put((byte) length);
    } else {
      frame.putShort((short) length);
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

  private static void assertLookupWork(final byte[][] suffixes, final byte[][] keys, final int prefix,
      final int maxLoadsPerComparison) {
    final HOTLeafBody body =
        HOTLeafBody.read(new ByteArrayBytesIn(frame(suffixes)), MODE, suffixes.length, used(suffixes));
    body.validate(prefix, IndexType.PROJECTION);
    // floor(log2 n) + 1, the textbook comparison bound for a binary search over n elements.
    final int maxComparisons = Integer.SIZE - Integer.numberOfLeadingZeros(suffixes.length);
    final long maxLanes = (long) maxComparisons * maxLoadsPerComparison;
    for (final byte[] key : keys) {
      for (final byte[] probe : new byte[][] {key, successor(key)}) {
        final CanonicalSearch canonical = canonicalSearch(suffixes, Arrays.copyOfRange(probe, prefix, Long.BYTES));
        final long before = HOTLeafPage.suffixProbeReads();
        final int found = body.findLongSuffix(probe, prefix);
        final long lanes = HOTLeafPage.suffixProbeReads() - before;
        final String where = Arrays.toString(probe) + " prefix " + prefix;
        assertEquals(canonical.result(), found, "slot for " + where);
        assertTrue(lanes >= 1, "a lookup over " + suffixes.length + " slots must read a suffix: " + where);
        assertTrue(lanes <= maxLanes, "lanes for " + where + ": " + lanes + " exceeds " + maxComparisons
            + " comparisons of at most " + maxLoadsPerComparison + " loads");
      }
    }
  }

  private static byte[] successor(final byte[] key) {
    final byte[] next = key.clone();
    next[Long.BYTES - 1]++;
    return next;
  }

  /** What the canonical binary search finds, and how many key comparisons it spent finding it. */
  private record CanonicalSearch(int result, int comparisons) {
  }

  /**
   * The JDK's own binary search over the same suffixes, counting its comparisons.
   * {@link Arrays#compareUnsigned(byte[], byte[])} supplies the HOT suffix order — unsigned over the
   * common prefix, then shorter-is-less — and {@code Arrays.binarySearch} supplies both the
   * {@code -(insertion) - 1} miss encoding and the comparison count, so neither comes from the page.
   */
  private static CanonicalSearch canonicalSearch(final byte[][] sortedSuffixes, final byte[] tail) {
    final int[] comparisons = new int[1];
    final int result = Arrays.binarySearch(sortedSuffixes, tail, (stored, probe) -> {
      comparisons[0]++;
      return Arrays.compareUnsigned(stored, probe);
    });
    return new CanonicalSearch(result, comparisons[0]);
  }
}
