/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.JsonTestHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.StorageEngineReader;
import io.sirix.cache.Allocators;
import io.sirix.cache.FrameSlotAllocator;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.index.IndexType;
import io.sirix.node.Bytes;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.page.interfaces.PageFragmentKey;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.lang.foreign.Arena;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Slot-granular CoW correctness for {@link HOTLeafPage}: verifies that under non-FULL versioning a
 * single-entry mutation produces a sparse on-disk fragment, that the source leaf's persisted bytes
 * are unchanged across the mutation, and that round-tripping the sparse fragment through the wire
 * format preserves only the dirty entries.
 *
 * <p>
 * Cross-IndexType coverage uses {@link IndexType#PATH}, {@link IndexType#CAS},
 * {@link IndexType#NAME}, {@link IndexType#PROJECTION} since all four go through the same
 * {@link HOTLeafPage} machinery; the exercise is to confirm dirty marking is uniform.
 * </p>
 */
final class HOTLeafPageCowTest {

  private static MemorySegmentAllocator allocator;

  @BeforeAll
  static void setUpClass() {
    allocator = Allocators.getInstance();
    allocator.init(1L * 1024 * 1024 * 1024); // 1 GiB
  }

  @Test
  @DisplayName("copy() leaves dirty bitmap clean and links completePageRef")
  void copyClearsDirtyAndLinksCompleteRef() {
    final HOTLeafPage src = new HOTLeafPage(1L, 1, IndexType.PATH);
    try {
      assertTrue(src.put(keyOf(0), valueOf(0)));
      assertTrue(src.put(keyOf(1), valueOf(1)));
      assertTrue(src.put(keyOf(2), valueOf(2)));

      final HOTLeafPage cow = src.copy();
      try {
        assertFalse(cow.hasDirty(), "dirty bitmap must be clean immediately after copy()");
        assertEquals(src, cow.getCompletePageRef());
        assertEquals(src.getEntryCount(), cow.getEntryCount());
      } finally {
        cow.close();
      }
    } finally {
      src.close();
    }
  }

  @Test
  @DisplayName("copy() releases a newly allocated frame when the source read fails")
  void copyReleasesRawFrameWhenSourceReadFails() {
    final FrameSlotAllocator frameAllocator = assertInstanceOf(FrameSlotAllocator.class, allocator);
    final HOTLeafPage source;
    try (Arena sourceArena = Arena.ofConfined()) {
      source = new HOTLeafPage(2L, 1, IndexType.PATH, sourceArena.allocate(HOTLeafPage.DEFAULT_SIZE), null,
          new int[HOTLeafPage.MAX_ENTRIES], 0, 1);
    }

    final int frameClass = FrameSlotAllocator.indexForSize(HOTLeafPage.DEFAULT_SIZE);
    final int liveBefore = frameAllocator.liveSlotCount(frameClass);
    try {
      assertThrows(IllegalStateException.class, source::copy);
      assertEquals(liveBefore, frameAllocator.liveSlotCount(frameClass),
          "the frame allocated before reading the closed source segment must be returned");
    } finally {
      source.close();
    }
  }

  @Test
  @DisplayName("versioning retires its local copy and preserves fragment cleanup failures")
  void versioningRetiresLocalCopyAndPreservesCleanupFailure() {
    final FrameSlotAllocator frameAllocator = assertInstanceOf(FrameSlotAllocator.class, allocator);
    final HOTLeafPage source = newPopulatedLeaf(IndexType.PATH, 1);
    final HOTLeafPage failingFragment = mock(HOTLeafPage.class);
    final StorageEngineReader reader = mock(StorageEngineReader.class);
    final List<PageFragmentKey> originalFragments = List.of(new PageFragmentKeyImpl(1, 16L, 1L, 1L));
    final PageReference reference = new PageReference().setKey(17L).setPageFragments(originalFragments);
    final List<HOTLeafPage> fragments = List.of(failingFragment);
    final AssertionError carryFailure = new AssertionError("injected carry-forward failure");
    final IllegalStateException releaseFailure = new IllegalStateException("injected fragment release failure");

    when(reader.loadHOTLeafFragments(reference)).thenReturn(fragments);
    when(reader.getRevisionNumber()).thenReturn(2);
    when(failingFragment.getEntryCount()).thenThrow(carryFailure);
    doThrow(releaseFailure).when(reader).releaseHOTLeafFragments(fragments, source);

    final int frameClass = FrameSlotAllocator.indexForSize(HOTLeafPage.DEFAULT_SIZE);
    final int liveBefore = frameAllocator.liveSlotCount(frameClass);
    try {
      final AssertionError thrown = assertThrows(AssertionError.class,
          () -> VersioningType.SLIDING_SNAPSHOT.combineHOTLeafPagesForModification(source, 2, reader, reference));

      assertSame(carryFailure, thrown);
      assertEquals(1, thrown.getSuppressed().length);
      assertSame(releaseFailure, thrown.getSuppressed()[0]);
      assertSame(originalFragments, reference.getPageFragments(),
          "a failed carry-forward must restore the exact pre-bump fragment chain");
      assertEquals(liveBefore, frameAllocator.liveSlotCount(frameClass),
          "the unreturned CoW page must be retired after carry-forward fails");
    } finally {
      source.close();
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  @DisplayName("every strategy restores the exact fragment chain when CoW allocation fails")
  void everyVersioningTypeRestoresFragmentChainWhenCopyFails(final VersioningType versioningType) {
    final HOTLeafPage source = mock(HOTLeafPage.class);
    final StorageEngineReader reader = mock(StorageEngineReader.class);
    final ArrayList<PageFragmentKey> originalFragments = new ArrayList<>();
    final PageReference reference = new PageReference().setKey(17L).setPageFragments(originalFragments);
    final OutOfMemoryError copyFailure = new OutOfMemoryError("injected CoW allocation failure");

    when(source.getRevision()).thenReturn(1);
    when(source.copyForRevision(2)).thenThrow(copyFailure);
    when(reader.getRevisionNumber()).thenReturn(2);
    when(reader.getDatabaseId()).thenReturn(1L);
    when(reader.getResourceId()).thenReturn(1L);

    final OutOfMemoryError thrown = assertThrows(OutOfMemoryError.class,
        () -> versioningType.combineHOTLeafPagesForModification(source, 4, reader, reference));

    assertSame(copyFailure, thrown);
    assertSame(originalFragments, reference.getPageFragments(),
        "the failed " + versioningType + " attempt must restore the exact list object");
    assertTrue(reference.getPageFragments().isEmpty());
  }

  @Test
  @DisplayName("versioning restores the fragment chain when borrowed-fragment release fails")
  void versioningRestoresFragmentChainWhenFragmentReleaseFails() {
    final FrameSlotAllocator frameAllocator = assertInstanceOf(FrameSlotAllocator.class, allocator);
    final HOTLeafPage source = newPopulatedLeaf(IndexType.PATH, 1);
    final HOTLeafPage olderFragment = newPopulatedLeaf(IndexType.PATH, 1);
    final StorageEngineReader reader = mock(StorageEngineReader.class);
    final List<PageFragmentKey> originalFragments = List.of(new PageFragmentKeyImpl(1, 16L, 1L, 1L));
    final PageReference reference = new PageReference().setKey(17L).setPageFragments(originalFragments);
    final List<HOTLeafPage> fragments = List.of(olderFragment);
    final IllegalStateException releaseFailure = new IllegalStateException("injected fragment release failure");

    when(reader.loadHOTLeafFragments(reference)).thenReturn(fragments);
    when(reader.getRevisionNumber()).thenReturn(2);
    when(reader.getDatabaseId()).thenReturn(1L);
    when(reader.getResourceId()).thenReturn(1L);
    doThrow(releaseFailure).when(reader).releaseHOTLeafFragments(fragments, source);

    final int frameClass = FrameSlotAllocator.indexForSize(HOTLeafPage.DEFAULT_SIZE);
    final int liveBefore = frameAllocator.liveSlotCount(frameClass);
    try {
      final IllegalStateException thrown = assertThrows(IllegalStateException.class,
          () -> VersioningType.SLIDING_SNAPSHOT.combineHOTLeafPagesForModification(source, 2, reader, reference));

      assertSame(releaseFailure, thrown);
      assertSame(originalFragments, reference.getPageFragments(),
          "release failure must restore the exact pre-bump fragment chain");
      assertEquals(liveBefore, frameAllocator.liveSlotCount(frameClass),
          "the successful-but-unreturned CoW page must be retired when fragment release fails");
    } finally {
      olderFragment.close();
      source.close();
    }
  }

  @Test
  @DisplayName("single-entry mutation marks exactly one bit dirty")
  void singleMutationMarksOneBit() {
    final HOTLeafPage src = newPopulatedLeaf(IndexType.PATH, 16);
    try {
      final HOTLeafPage cow = src.copy();
      try {
        assertTrue(cow.put(keyOf(99), valueOf(99))); // new key — inserts somewhere
        assertEquals(1, cow.getDirtyEntryCount(), "exactly one dirty entry expected after a single insert");
        assertTrue(cow.hasDirty());
      } finally {
        cow.close();
      }
    } finally {
      src.close();
    }
  }

  @Test
  @DisplayName("update of existing key marks one bit dirty (no shift)")
  void updateOfExistingMarksOneBit() {
    final HOTLeafPage src = newPopulatedLeaf(IndexType.PATH, 16);
    try {
      final HOTLeafPage cow = src.copy();
      try {
        // Update a value; same length so it's an in-place update.
        final byte[] existingKey = keyOf(8);
        final int idx = cow.findEntry(existingKey);
        assertTrue(idx >= 0);
        final byte[] newValue = valueOf(8); // same length
        // Through the public surface, updateValue is invoked indirectly; do it via mergeWithNodeRefs
        // analogue: same-length put is treated as no-op (returns false), so use updateValueRange.
        assertTrue(cow.updateValueRange(idx, newValue, 0, newValue.length));
        assertEquals(1, cow.getDirtyEntryCount());
      } finally {
        cow.close();
      }
    } finally {
      src.close();
    }
  }

  @Test
  @DisplayName("range update shrinks a value in its existing slot")
  void shrinkingRangeUpdateReusesExistingSlot() {
    final HOTLeafPage src = new HOTLeafPage(1L, 1, IndexType.PATH);
    try {
      final byte[] key = keyOf(8);
      assertTrue(src.put(key, "original-value".getBytes(StandardCharsets.UTF_8)));
      final HOTLeafPage cow = src.copy();
      try {
        final int index = cow.findEntry(key);
        final int usedBefore = cow.getUsedSlotsSize();
        final byte[] source = "xxnewyy".getBytes(StandardCharsets.UTF_8);

        assertTrue(cow.updateValueRange(index, source, 2, 3));
        assertArrayEquals("new".getBytes(StandardCharsets.UTF_8), cow.copyStoredValue(index));
        assertEquals(usedBefore - ("original-value".length() - 3), cow.getUsedSlotsSize(),
            "a shrunken tail slot should release its trailing bytes immediately");
        assertEquals(1, cow.getDirtyEntryCount());
      } finally {
        cow.close();
      }
    } finally {
      src.close();
    }
  }

  @ParameterizedTest
  @EnumSource(value = IndexType.class, names = {"PATH", "CAS", "NAME", "PROJECTION"})
  @DisplayName("sparse fragment under SLIDING_SNAPSHOT carries only the dirty entries")
  void sparseFragmentSlidingSnapshot(final IndexType indexType) {
    final ResourceConfiguration config = newConfig(VersioningType.SLIDING_SNAPSHOT);
    final HOTLeafPage src = newPopulatedLeaf(indexType, 32);
    try {
      // Establish baseline: serialize the full source leaf.
      final BytesOut<?> baselineSink = Bytes.elasticOffHeapByteBuffer();
      PageKind.HOT_LEAF_PAGE.serializePage(config, baselineSink, src, SerializationType.DATA);
      final long baselineSize = baselineSink.writePosition();

      // CoW + single mutation.
      final HOTLeafPage cow = src.copy();
      try {
        assertTrue(cow.put(keyOf(999), valueOf(999)));

        // Serialize the modified page (sparse emit).
        final BytesOut<?> sparseSink = Bytes.elasticOffHeapByteBuffer();
        PageKind.HOT_LEAF_PAGE.serializePage(config, sparseSink, cow, SerializationType.DATA);
        final long sparseSize = sparseSink.writePosition();

        // Sparse fragment must be strictly smaller than baseline (it carries 1 entry vs 32).
        assertTrue(sparseSize < baselineSize, "sparse=" + sparseSize + " baseline=" + baselineSize);

        // Round-trip the sparse fragment and confirm only the dirty entry is present.
        final BytesIn<?> source = sparseSink.bytesForRead();
        source.readByte(); // skip pageKind id
        final HOTLeafPage deserialized =
            (HOTLeafPage) PageKind.HOT_LEAF_PAGE.deserializePage(config, source, SerializationType.DATA);
        try {
          assertEquals(1, deserialized.getEntryCount(), "deserialized sparse fragment must contain exactly 1 entry");
          final int idx = deserialized.findEntry(keyOf(999));
          assertTrue(idx >= 0, "dirty key must be in the sparse fragment");
        } finally {
          deserialized.close();
        }
      } finally {
        cow.close();
      }
    } finally {
      src.close();
    }
  }

  @ParameterizedTest
  @EnumSource(value = IndexType.class, names = {"PATH", "CAS", "NAME", "PROJECTION"})
  @DisplayName("FULL strategy emits all entries regardless of dirty bitmap")
  void fullStrategyEmitsAll(final IndexType indexType) {
    final ResourceConfiguration config = newConfig(VersioningType.FULL);
    final HOTLeafPage src = newPopulatedLeaf(indexType, 32);
    try {
      final HOTLeafPage cow = src.copy();
      try {
        assertTrue(cow.put(keyOf(999), valueOf(999)));

        final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
        PageKind.HOT_LEAF_PAGE.serializePage(config, sink, cow, SerializationType.DATA);
        final BytesIn<?> source = sink.bytesForRead();
        source.readByte();
        final HOTLeafPage deserialized =
            (HOTLeafPage) PageKind.HOT_LEAF_PAGE.deserializePage(config, source, SerializationType.DATA);
        try {
          // FULL = every entry must round-trip (32 + 1 inserted = 33).
          assertEquals(cow.getEntryCount(), deserialized.getEntryCount(), "FULL strategy must emit every entry");
          assertNotNull(deserialized.getCommonPrefix());
        } finally {
          deserialized.close();
        }
      } finally {
        cow.close();
      }
    } finally {
      src.close();
    }
  }

  @Test
  @DisplayName("fresh leaf (no completePageRef) emits all entries even under SLIDING_SNAPSHOT")
  void freshLeafEmitsAll() {
    final ResourceConfiguration config = newConfig(VersioningType.SLIDING_SNAPSHOT);
    final HOTLeafPage fresh = new HOTLeafPage(42L, 1, IndexType.PATH);
    try {
      assertTrue(fresh.put(keyOf(0), valueOf(0)));
      assertTrue(fresh.put(keyOf(1), valueOf(1)));
      assertTrue(fresh.put(keyOf(2), valueOf(2)));

      final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
      PageKind.HOT_LEAF_PAGE.serializePage(config, sink, fresh, SerializationType.DATA);
      final BytesIn<?> source = sink.bytesForRead();
      source.readByte();
      final HOTLeafPage deserialized =
          (HOTLeafPage) PageKind.HOT_LEAF_PAGE.deserializePage(config, source, SerializationType.DATA);
      try {
        assertEquals(3, deserialized.getEntryCount(),
            "fresh leaf must emit all entries (no completePageRef → no sparse path)");
      } finally {
        deserialized.close();
      }
    } finally {
      fresh.close();
    }
  }

  @Test
  @DisplayName("dirty bitmap shifts on insert mirror slotOffsets shift")
  void dirtyBitmapShiftMirrorsSlotOffsets() {
    final HOTLeafPage src = new HOTLeafPage(1L, 1, IndexType.PATH);
    try {
      // Pre-populate with a sequence that will allow inserting at known positions.
      assertTrue(src.put(keyOf(10), valueOf(10)));
      assertTrue(src.put(keyOf(20), valueOf(20)));
      assertTrue(src.put(keyOf(30), valueOf(30)));

      final HOTLeafPage cow = src.copy();
      try {
        // Insert at the head — pos 0. Pre-existing entries 10/20/30 shift to indices 1/2/3.
        // Dirty bitmap was clean, so post-shift bits 1/2/3 are clean; bit 0 (the new entry)
        // is marked.
        assertTrue(cow.put(keyOf(5), valueOf(5)));
        final int idx0 = cow.findEntry(keyOf(5));
        assertEquals(0, idx0);
        assertTrue(cow.isEntryDirty(0));
        assertFalse(cow.isEntryDirty(1));
        assertFalse(cow.isEntryDirty(2));
        assertFalse(cow.isEntryDirty(3));
      } finally {
        cow.close();
      }
    } finally {
      src.close();
    }
  }

  @Test
  @DisplayName("dirty bitmap iterator visits dirty bits in ascending order")
  void dirtyBitmapIterator() {
    final HOTLeafPage leaf = new HOTLeafPage(1L, 1, IndexType.PATH);
    try {
      // Reach into the package-private mark API directly (same package).
      leaf.markEntryDirty(0);
      leaf.markEntryDirty(63);
      leaf.markEntryDirty(64);
      leaf.markEntryDirty(127);
      leaf.markEntryDirty(511);

      final int[] visited = new int[5];
      final int[] cursor = new int[1];
      leaf.iterateDirtyEntries(idx -> visited[cursor[0]++] = idx);
      assertEquals(5, cursor[0]);
      assertEquals(0, visited[0]);
      assertEquals(63, visited[1]);
      assertEquals(64, visited[2]);
      assertEquals(127, visited[3]);
      assertEquals(511, visited[4]);
    } finally {
      leaf.close();
    }
  }

  @Test
  @DisplayName("clearDirtyBitmap resets every word")
  void clearDirtyBitmapResets() {
    final HOTLeafPage leaf = new HOTLeafPage(1L, 1, IndexType.PATH);
    try {
      leaf.markEntryDirty(0);
      leaf.markEntryDirty(100);
      leaf.markEntryDirty(500);
      assertTrue(leaf.hasDirty());
      leaf.clearDirtyBitmap();
      assertFalse(leaf.hasDirty());
      assertEquals(0, leaf.getDirtyEntryCount());
    } finally {
      leaf.close();
    }
  }

  @Test
  @DisplayName("SLIDING_SNAPSHOT carry-forward marks only aging, unshadowed, non-tombstone entries")
  void slidingSnapshotCarryForwardMarksAgingEntries() {
    // A three-fragment window, newest first. Each fragment holds only the entries WRITTEN at its
    // revision (sparse), exactly as it would persist. The oldest fragment is about to age out.
    final HOTLeafPage newest = new HOTLeafPage(7L, 3, IndexType.PATH);
    final HOTLeafPage middle = new HOTLeafPage(7L, 2, IndexType.PATH);
    final HOTLeafPage oldest = new HOTLeafPage(7L, 1, IndexType.PATH);
    // The combined leaf the writer copies for modification — every live key.
    final HOTLeafPage complete = new HOTLeafPage(7L, 3, IndexType.PATH);
    HOTLeafPage modified = null;
    try {
      assertTrue(newest.put(keyOf(10), valueOf(10))); // key 10 re-emitted recently
      assertTrue(middle.put(keyOf(20), valueOf(20))); // key 20 re-emitted recently
      // Oldest fragment: 10 & 20 (shadowed by newer fragments), 30 & 40 (unique, still live),
      // 50 (deleted at this revision -> tombstone).
      assertTrue(oldest.put(keyOf(10), valueOf(10)));
      assertTrue(oldest.put(keyOf(20), valueOf(20)));
      assertTrue(oldest.put(keyOf(30), valueOf(30)));
      assertTrue(oldest.put(keyOf(40), valueOf(40)));
      assertTrue(oldest.put(keyOf(50), valueOf(50)));
      assertTrue(oldest.delete(keyOf(50))); // 50 -> tombstone in the oldest fragment

      for (final int k : new int[] {10, 20, 30, 40}) { // live keys in the merged leaf (50 deleted)
        assertTrue(complete.put(keyOf(k), valueOf(k)));
      }
      modified = complete.copyForRevision(4); // mirrors the writer path: dirty cleared, new wire revision
      assertEquals(3, complete.getRevision(), "the historical source revision must remain unchanged");
      assertEquals(4, modified.getRevision(), "the CoW image must carry its writing revision");
      assertFalse(modified.hasDirty());

      VersioningType.carryForwardAgingHOTEntries(java.util.List.of(newest, middle, oldest), modified);

      // Only 30 and 40 must be carried forward: unique to the oldest fragment, live, non-tombstone.
      assertEquals(2, modified.getDirtyEntryCount(), "exactly the two aging live keys are carried");
      assertTrue(modified.isEntryDirty(modified.findEntry(keyOf(30))), "key 30 (aging) carried");
      assertTrue(modified.isEntryDirty(modified.findEntry(keyOf(40))), "key 40 (aging) carried");
      assertFalse(modified.isEntryDirty(modified.findEntry(keyOf(10))), "key 10 shadowed by newest");
      assertFalse(modified.isEntryDirty(modified.findEntry(keyOf(20))), "key 20 shadowed by middle");
      assertTrue(modified.findEntry(keyOf(50)) < 0, "deleted key absent from the merged leaf");
    } finally {
      for (final HOTLeafPage p : new HOTLeafPage[] {newest, middle, oldest, complete, modified}) {
        if (p != null && !p.isClosed()) {
          p.close();
        }
      }
    }
  }

  // ---------- helpers ----------

  private static HOTLeafPage newPopulatedLeaf(final IndexType indexType, final int n) {
    final HOTLeafPage leaf = new HOTLeafPage(1L, 1, indexType);
    for (int i = 0; i < n; i++) {
      assertTrue(leaf.put(keyOf(i), valueOf(i)));
    }
    return leaf;
  }

  private static byte[] keyOf(final int i) {
    return ("key-" + String.format("%04d", i)).getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] valueOf(final int i) {
    return ("val-" + String.format("%04d", i)).getBytes(StandardCharsets.UTF_8);
  }

  private static ResourceConfiguration newConfig(final VersioningType versioningType) {
    return new ResourceConfiguration.Builder(JsonTestHelper.RESOURCE).versioningApproach(versioningType).build();
  }
}
