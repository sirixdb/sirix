/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.JsonTestHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.Allocators;
import io.sirix.index.IndexType;
import io.sirix.node.ByteArrayBytesIn;
import io.sirix.node.Bytes;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.node.MemorySegmentBytesOut;
import io.sirix.node.PooledBytesOut;
import io.sirix.node.PooledGrowingSegment;
import io.sirix.node.Utils;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;
import java.util.SplittableRandom;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Wire and ownership coverage for the native HOT-leaf full-payload write path. */
final class HOTLeafPageNativeWireTest {

  private static final int SERIALIZATION_CAPACITY =
      HOTLeafPage.DEFAULT_SIZE + HOTLeafPage.MAX_ENTRIES * Integer.BYTES + 1_024;

  @BeforeAll
  static void initializeAllocator() {
    Allocators.getInstance().init(1L * 1_024 * 1_024 * 1_024);
  }

  @Test
  void fullPayloadUsesExactSegmentWriteAndRetainsLegacyWire() {
    final ResourceConfiguration config = fullConfig("hot-leaf-native-wire");
    final HOTLeafPage pageA = populatedLeaf(256);
    final HOTLeafPage pageB = populatedLeaf(17);
    pageA.setCompleteDump(true);

    try (TrackingMemorySegmentBytesOut actualSink = new TrackingMemorySegmentBytesOut(SERIALIZATION_CAPACITY)) {
      final byte[] firstAWire = serialize(config, actualSink, pageA);

      assertEquals(1, actualSink.segmentWriteCount);
      assertSame(pageA.slots(), actualSink.segmentSource,
          "the serializer must pass the leaf-owned slot segment directly to BytesOut");
      assertEquals(0L, actualSink.segmentSourceOffset);
      assertEquals(pageA.getUsedSlotsSize(), actualSink.segmentLength,
          "only the declared live slot prefix belongs on the wire");

      serialize(config, actualSink, pageB);
      final byte[] secondAWire = serialize(config, actualSink, pageA);

      assertEquals(3, actualSink.segmentWriteCount);
      assertSame(pageA.slots(), actualSink.segmentSource);
      assertArrayEquals(firstAWire, secondAWire, "A/B/A reuse must not retain page B's bounds or payload");
      assertArrayEquals(serializeWithLegacyHeapPayload(config, pageA), secondAWire,
          "the native bulk write must not change the V0 HOT-leaf wire layout");
    } finally {
      pageB.close();
      pageA.close();
    }
  }

  @Test
  void sparsePayloadAbaUsesDirectSlotRangesAndRetainsLegacyWire() {
    final ResourceConfiguration config = slidingConfig("hot-leaf-native-sparse-wire");
    final HOTLeafPage baseA = populatedLeaf(64);
    final HOTLeafPage baseB = populatedLeaf(64);
    final HOTLeafPage pageA = baseA.copy();
    final HOTLeafPage pageB = baseB.copy();

    try (TrackingMemorySegmentBytesOut sink = new TrackingMemorySegmentBytesOut(SERIALIZATION_CAPACITY)) {
      assertTrue(pageA.put(keyOf(64), valueOf(64)));
      for (int i = 64; i < 96; i++) {
        assertTrue(pageB.put(keyOf(i), valueOf(i)));
      }

      final byte[] firstAWire = serialize(config, sink, pageA);
      serialize(config, sink, pageB);
      final byte[] secondAWire = serialize(config, sink, pageA);

      assertEquals(34, sink.segmentWriteCount, "sparse serialization must issue one exact segment copy per dirty slot");
      assertSame(pageA.slots(), sink.segmentSource);
      assertArrayEquals(firstAWire, secondAWire, "A/B/A reuse must not retain page B's packed offsets or payload");
      assertArrayEquals(serializeWithLegacyHeapPayload(config, pageA), secondAWire,
          "the direct two-pass sparse encoding must retain the packed V0 wire");
    } finally {
      pageB.close();
      pageA.close();
      baseB.close();
      baseA.close();
    }
  }

  @Test
  void sideMapAbaRetainsSortedWireAcrossScratchGrowth() throws IOException {
    final ResourceConfiguration config = fullConfig("hot-leaf-native-side-map-wire");
    final HOTLeafPage pageA = populatedLeaf(8);
    final HOTLeafPage pageB = populatedLeaf(8);
    HOTLeafPage coldPage = null;

    addDurableSideReference(pageA, HOTLeafPage.overflowPageRefKey(9L, 31), 31_000L);
    addDurableSideReference(pageA, HOTLeafPage.overflowPageRefKey(-2L, 17), 17_000L);
    addDurableSideReference(pageA, HOTLeafPage.overflowPageRefKey(0L, 0), 7_000L);
    // Reverse insertion order, exercise the large-prefix sort shape from the pinned-spill profile,
    // and cross the scratch's initial 512-key capacity twice.
    for (int subId = 2_047; subId >= 0; subId--) {
      addDurableSideReference(pageB, HOTLeafPage.overflowPageRefKey(7L, subId), 100_000L + subId);
    }

    try (MemorySegmentBytesOut sink = new MemorySegmentBytesOut(SERIALIZATION_CAPACITY)) {
      final byte[] firstAWire = serialize(config, sink, pageA);
      final byte[] pageBWire = serialize(config, sink, pageB);
      final byte[] secondAWire = serialize(config, sink, pageA);

      assertArrayEquals(serializeWithLegacyHeapPayload(config, pageB), pageBWire,
          "allocation-free large-prefix sort must retain the legacy signed-long wire order");
      assertArrayEquals(firstAWire, secondAWire, "A/B/A scratch reuse must ignore page B's stale sorted-key tail");
      assertArrayEquals(serializeWithLegacyHeapPayload(config, pageA), secondAWire,
          "reusable primitive key scratch must retain deterministic signed-long ordering");

      coldPage = (HOTLeafPage) new PagePersister().deserializePage(config, Bytes.wrapForRead(secondAWire),
          SerializationType.DATA);
      assertEquals(3, coldPage.segmentRefCount());
      assertEquals(31_000L, coldPage.getPageReference(HOTLeafPage.overflowPageRefKey(9L, 31)).getKey());
      assertEquals(17_000L, coldPage.getPageReference(HOTLeafPage.overflowPageRefKey(-2L, 17)).getKey());
      assertEquals(7_000L, coldPage.getPageReference(HOTLeafPage.overflowPageRefKey(0L, 0)).getKey());
    } finally {
      if (coldPage != null) {
        coldPage.close();
      }
      pageB.close();
      pageA.close();
    }
  }

  @Test
  void sideReferencePrefixSortMatchesSignedJdkOrderAndLeavesStaleTailUntouched() {
    final SplittableRandom random = new SplittableRandom(0x5E6D_E17A_11E5L);
    final int[] sizes = {0, 1, 2, 22, 23, 24, 25, 26, 127, 512, 2_048, 4_097};
    for (final int size : sizes) {
      final long[] randomValues = new long[size];
      for (int index = 0; index < size; index++) {
        randomValues[index] = random.nextLong();
      }
      assertPrefixSortMatchesJdk("random-" + size, randomValues);

      final long[] sortedValues = randomValues.clone();
      Arrays.sort(sortedValues);
      assertPrefixSortMatchesJdk("sorted-" + size, sortedValues);

      final long[] reverseValues = sortedValues.clone();
      for (int left = 0, right = size - 1; left < right; left++, right--) {
        final long value = reverseValues[left];
        reverseValues[left] = reverseValues[right];
        reverseValues[right] = value;
      }
      assertPrefixSortMatchesJdk("reverse-" + size, reverseValues);

      final long[] equalValues = new long[size];
      Arrays.fill(equalValues, Long.MIN_VALUE);
      assertPrefixSortMatchesJdk("all-equal-" + size, equalValues);

      final long[] duplicateHeavyValues = new long[size];
      final long[] duplicatePalette = {Long.MIN_VALUE, -1L, 0L, 0L, 1L, Long.MAX_VALUE};
      for (int index = 0; index < size; index++) {
        duplicateHeavyValues[index] = duplicatePalette[random.nextInt(duplicatePalette.length)];
      }
      assertPrefixSortMatchesJdk("duplicate-heavy-" + size, duplicateHeavyValues);
    }

    assertPrefixSortMatchesJdk("signed-extremes",
        new long[] {Long.MAX_VALUE, Long.MIN_VALUE, 0L, -1L, 1L, Long.MIN_VALUE, Long.MAX_VALUE});
  }

  @Test
  void warmPooledSerializationOwnsBytesThatColdReadAfterSourceReuse() throws IOException {
    final ResourceConfiguration config = fullConfig("hot-leaf-native-cold-read");
    final HOTLeafPage page = populatedLeaf(256);
    final byte[] pooledBacking = new byte[SERIALIZATION_CAPACITY];
    final PooledGrowingSegment pooledSegment = new PooledGrowingSegment(MemorySegment.ofArray(pooledBacking));
    final PooledBytesOut sink = new PooledBytesOut(pooledSegment);
    HOTLeafPage coldPage = null;

    try {
      PageKind.HOT_LEAF_PAGE.serializePage(config, sink, page, SerializationType.DATA);
      final byte[] firstWire = sink.toByteArray();

      // The second pass reuses the already-sized sink: this is the recurring pinned-spill shape.
      sink.clear();
      PageKind.HOT_LEAF_PAGE.serializePage(config, sink, page, SerializationType.DATA);
      assertEquals(firstWire.length, sink.position());

      // writeSegment is a synchronous copy, not an ownership hand-off. Reusing both source and sink
      // storage after detaching the persisted bytes must not affect a cold decode.
      page.slots().asSlice(0L, page.getUsedSlotsSize()).fill((byte) 0xA5);
      final byte[] persistedWire = sink.toByteArray();
      assertArrayEquals(firstWire, persistedWire);
      pooledSegment.reset();
      MemorySegment.ofArray(pooledBacking).fill((byte) 0xCC);

      coldPage = (HOTLeafPage) new PagePersister().deserializePage(config, Bytes.wrapForRead(persistedWire),
          SerializationType.DATA);
      assertEquals(256, coldPage.getEntryCount());
      assertEquals(page.getPageKey(), coldPage.getPageKey());
      assertEquals(page.getRevision(), coldPage.getRevision());
      assertEquals(page.getIndexType(), coldPage.getIndexType());
      for (int i = 0; i < 256; i++) {
        final int entryIndex = coldPage.findEntry(keyOf(i));
        assertTrue(entryIndex >= 0, "missing cold-read key " + i);
        assertArrayEquals(valueOf(i), coldPage.getValue(entryIndex), "cold-read value mismatch for key " + i);
      }
    } finally {
      if (coldPage != null) {
        coldPage.close();
      }
      page.close();
    }
  }

  @Test
  void bulkSlotOffsetsReadLegacyWireAtEveryAlignment() throws IOException {
    final ResourceConfiguration config = fullConfig("hot-leaf-bulk-offsets");
    for (final int count : new int[] {0, 1, HOTLeafPage.MAX_ENTRIES - 1, HOTLeafPage.MAX_ENTRIES}) {
      final HOTLeafPage original = new HOTLeafPage(17L, 23, IndexType.PROJECTION);
      try {
        for (int i = 0; i < count; i++) {
          assertTrue(original.put(keyOf(i), Arrays.copyOf(valueOf(i), 1 + i % 23)));
        }
        original.setCompleteDump(true);
        final byte[] wire = serializeWithLegacyHeapPayload(config, original);
        assertDecodedLeaf(config, original, new ByteArrayBytesIn(wire), wire.length);

        try (Arena arena = Arena.ofConfined()) {
          final MemorySegment nativeWire = arena.allocate(wire.length + 8L);
          for (int shift = 0; shift < 8; shift++) {
            MemorySegment.copy(wire, 0, nativeWire, ValueLayout.JAVA_BYTE, shift, wire.length);
            final MemorySegmentBytesIn source = new MemorySegmentBytesIn(nativeWire);
            source.position(shift);
            assertDecodedLeaf(config, original, source, shift + wire.length);
          }
        }
      } finally {
        original.close();
      }
    }
  }

  @Test
  void sparseOffsetReadLeavesFollowingLeafUnread() throws IOException {
    final ResourceConfiguration config = slidingConfig("hot-leaf-sparse-offsets");
    final HOTLeafPage base = populatedLeaf(64);
    final HOTLeafPage changed = base.copy();
    try {
      assertTrue(changed.put(keyOf(64), valueOf(64)));
      assertEquals(1, changed.getDirtyEntryCount());
      final byte[] sparseWire = serializeWithLegacyHeapPayload(config, changed);
      final byte[] nextWire = serializeWithLegacyHeapPayload(config, base);
      final byte[] stream = Arrays.copyOf(sparseWire, sparseWire.length + nextWire.length);
      System.arraycopy(nextWire, 0, stream, sparseWire.length, nextWire.length);
      final BytesIn<?>[] sources =
          {new ByteArrayBytesIn(stream), new MemorySegmentBytesIn(MemorySegment.ofArray(stream))};
      for (final BytesIn<?> source : sources) {
        final HOTLeafPage sparse =
            (HOTLeafPage) new PagePersister().deserializePage(config, source, SerializationType.DATA);
        try {
          assertEquals(1, sparse.getEntryCount(), "only the changed slot belongs to this fragment");
          assertArrayEquals(keyOf(64), sparse.getKey(0));
          assertArrayEquals(valueOf(64), sparse.getValue(0));
          assertEquals(sparseWire.length, source.position(), "the following leaf must remain unread");
          assertDecodedLeaf(config, base, source, stream.length);
        } finally {
          sparse.close();
        }
      }
    } finally {
      changed.close();
      base.close();
    }
  }

  @Test
  void truncatedSlotOffsetsAreRejectedByBulkAndScalarReaders() {
    final ResourceConfiguration config = fullConfig("hot-leaf-truncated-offsets");
    final HOTLeafPage original = populatedLeaf(17);
    try {
      final byte[] wire = serializeWithLegacyHeapPayload(config, original);
      final byte[] truncated = Arrays.copyOf(wire, wire.length - original.getUsedSlotsSize() - 1);
      final BytesIn<?>[] sources =
          {new ByteArrayBytesIn(truncated), new MemorySegmentBytesIn(MemorySegment.ofArray(truncated))};
      for (final BytesIn<?> source : sources) {
        assertThrows(IndexOutOfBoundsException.class,
            () -> new PagePersister().deserializePage(config, source, SerializationType.DATA));
      }
    } finally {
      original.close();
    }
  }

  private static void assertDecodedLeaf(final ResourceConfiguration config, final HOTLeafPage original,
      final BytesIn<?> source, final long expectedPosition) throws IOException {
    final HOTLeafPage decoded =
        (HOTLeafPage) new PagePersister().deserializePage(config, source, SerializationType.DATA);
    try {
      assertEquals(expectedPosition, source.position(), "read exactly the offsets and the following payload");
      assertEquals(original.getEntryCount(), decoded.getEntryCount());
      assertEquals(original.isCompleteDump(), decoded.isCompleteDump());
      for (int i = 0; i < original.getEntryCount(); i++) {
        assertArrayEquals(original.getKey(i), decoded.getKey(i));
        assertArrayEquals(original.getValue(i), decoded.getValue(i));
      }
    } finally {
      decoded.close();
    }
  }

  private static byte[] serialize(final ResourceConfiguration config, final MemorySegmentBytesOut sink,
      final HOTLeafPage page) {
    sink.clear();
    PageKind.HOT_LEAF_PAGE.serializePage(config, sink, page, SerializationType.DATA);
    return sink.toByteArray();
  }

  private static byte[] serializeWithLegacyHeapPayload(final ResourceConfiguration config, final HOTLeafPage page) {
    try (MemorySegmentBytesOut sink = new MemorySegmentBytesOut(SERIALIZATION_CAPACITY)) {
      final boolean hasSegmentReferences = page.segmentRefCount() > 0;
      sink.writeByte(PageKind.HOT_LEAF_PAGE.getID());
      PageKind.writeVersionAndFlags(sink, hasSegmentReferences
          ? HOTLeafPage.FLAG_OVERFLOW_PAGE_REFS
          : 0);
      Utils.putVarLong(sink, page.getPageKey());
      sink.writeInt(page.getRevision());
      sink.writeByte(page.getIndexType().getID());

      final byte[] prefix = page.getCommonPrefix();
      final int prefixLength = page.getCommonPrefixLen();
      sink.writeShort((short) prefixLength);
      sink.write(prefix, 0, prefixLength);

      final boolean sparseEmit =
          config.versioningType != VersioningType.FULL && page.getCompletePageRef() != null && page.hasDirty();
      if (sparseEmit) {
        final int dirtyCount = page.getDirtyEntryCount();
        final int dirtyBytes = page.getDirtyEntriesUsedSize();
        sink.writeInt(page.isCompleteDump()
            ? dirtyCount | 0x80000000
            : dirtyCount);
        sink.writeInt(dirtyBytes);

        final byte[] packedPayload = new byte[dirtyBytes];
        final int[] packedOffsets = new int[dirtyCount];
        assertEquals(dirtyBytes, page.packDirtyEntries(packedPayload, packedOffsets));
        for (int i = 0; i < dirtyCount; i++) {
          sink.writeInt(packedOffsets[i]);
        }
        sink.write(packedPayload);
      } else {
        sink.writeInt(page.isCompleteDump()
            ? page.getEntryCount() | 0x80000000
            : page.getEntryCount());
        sink.writeInt(page.getUsedSlotsSize());
        for (int i = 0; i < page.getEntryCount(); i++) {
          sink.writeInt(page.getSlotOffset(i));
        }

        final byte[] slotPayload = new byte[page.getUsedSlotsSize()];
        MemorySegment.copy(page.slots(), ValueLayout.JAVA_BYTE, 0L, slotPayload, 0, slotPayload.length);
        sink.write(slotPayload);
      }

      if (hasSegmentReferences) {
        final long[] keys = page.overflowPageRefKeysSorted();
        Utils.putVarLong(sink, keys.length);
        for (final long key : keys) {
          sink.writeLong(key);
          sink.writeLong(page.getPageReference(key).getKey());
        }
      }
      return sink.toByteArray();
    }
  }

  private static void addDurableSideReference(final HOTLeafPage page, final long compositeKey, final long durableKey) {
    page.setPageReference(compositeKey, new PageReference().setKey(durableKey));
  }

  private static void assertPrefixSortMatchesJdk(final String shape, final long[] prefix) {
    final long[] staleTail = {0x62A9_9ED7_0B1CL, Long.MIN_VALUE, 17L, Long.MAX_VALUE, -83L};
    final long[] actual = Arrays.copyOf(prefix, prefix.length + staleTail.length);
    System.arraycopy(staleTail, 0, actual, prefix.length, staleTail.length);
    final long[] expected = actual.clone();
    Arrays.sort(expected, 0, prefix.length);

    PageKind.sortHotLeafSideReferenceKeyPrefix(actual, prefix.length);

    assertArrayEquals(expected, actual, shape + " must match signed Arrays.sort prefix order");
    assertArrayEquals(staleTail, Arrays.copyOfRange(actual, prefix.length, actual.length),
        shape + " must not read or overwrite stale serializer scratch tail");
  }

  private static HOTLeafPage populatedLeaf(final int entryCount) {
    final HOTLeafPage page = new HOTLeafPage(17L, 23, IndexType.PROJECTION);
    for (int i = 0; i < entryCount; i++) {
      assertTrue(page.put(keyOf(i), valueOf(i)), "failed to populate key " + i);
    }
    return page;
  }

  private static byte[] keyOf(final int value) {
    return new byte[] {'p', 'r', 'o', 'j', ':', (byte) (value >>> 8), (byte) value};
  }

  private static byte[] valueOf(final int value) {
    final byte[] bytes = new byte[192];
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte) (value * 31 + i * 17);
    }
    return bytes;
  }

  private static ResourceConfiguration fullConfig(final String name) {
    return new ResourceConfiguration.Builder(JsonTestHelper.RESOURCE + '-' + name)
                                                                                  .versioningApproach(
                                                                                      VersioningType.FULL)
                                                                                  .build();
  }

  private static ResourceConfiguration slidingConfig(final String name) {
    return new ResourceConfiguration.Builder(JsonTestHelper.RESOURCE + '-' + name)
                                                                                  .versioningApproach(
                                                                                      VersioningType.SLIDING_SNAPSHOT)
                                                                                  .build();
  }

  private static final class TrackingMemorySegmentBytesOut extends MemorySegmentBytesOut {
    private int segmentWriteCount;
    private MemorySegment segmentSource;
    private long segmentSourceOffset;
    private long segmentLength;

    private TrackingMemorySegmentBytesOut(final int initialCapacity) {
      super(initialCapacity);
    }

    @Override
    public BytesOut<MemorySegment> writeSegment(final MemorySegment source, final long sourceOffset,
        final long length) {
      segmentWriteCount++;
      segmentSource = source;
      segmentSourceOffset = sourceOffset;
      segmentLength = length;
      return super.writeSegment(source, sourceOffset, length);
    }
  }
}
