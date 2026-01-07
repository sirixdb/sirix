/*
 * Copyright (c) 2024, SirixDB
 *
 * All rights reserved.
 */
package io.sirix.page;

import io.sirix.api.StorageEngineReader;
import io.sirix.cache.PageContainer;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.index.IndexType;
import io.sirix.index.hot.ChunkDirectory;
import io.sirix.index.hot.ChunkDirectorySerializer;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Integration tests for BitmapChunkPage versioning across different strategies.
 */
@DisplayName("Bitmap Chunk Versioning Integration")
class BitmapChunkVersioningIntegrationTest {

  @Nested
  @DisplayName("FULL versioning")
  class FullVersioning {

    @Test
    @DisplayName("FULL always combines to single page")
    void testFullVersioningCombine() {
      VersioningType type = VersioningType.FULL;
      StorageEngineReader reader = mock(StorageEngineReader.class);
      
      Roaring64Bitmap bitmap = new Roaring64Bitmap();
      bitmap.add(100);
      bitmap.add(200);
      bitmap.add(300);
      
      BitmapChunkPage page = BitmapChunkPage.createFull(
          1L, 5, IndexType.PATH, 0, 65536, bitmap);
      
      List<BitmapChunkPage> fragments = List.of(page);
      BitmapChunkPage result = type.combineBitmapChunks(fragments, 1, reader);
      
      assertNotNull(result);
      assertTrue(result.isFullSnapshot());
      assertEquals(3, result.getBitmap().getLongCardinality());
      assertTrue(result.getBitmap().contains(100));
      assertTrue(result.getBitmap().contains(200));
      assertTrue(result.getBitmap().contains(300));
    }

    @Test
    @DisplayName("FULL shouldStoreBitmapFullSnapshot always returns true")
    void testFullAlwaysFull() {
      VersioningType type = VersioningType.FULL;
      List<BitmapChunkPage> fragments = new ArrayList<>();
      
      assertTrue(type.shouldStoreBitmapFullSnapshot(fragments, 1, 5));
      assertTrue(type.shouldStoreBitmapFullSnapshot(fragments, 2, 5));
      assertTrue(type.shouldStoreBitmapFullSnapshot(fragments, 100, 5));
    }
  }

  @Nested
  @DisplayName("DIFFERENTIAL versioning")
  class DifferentialVersioning {

    @Test
    @DisplayName("DIFFERENTIAL combines base + delta")
    void testDifferentialCombine() {
      VersioningType type = VersioningType.DIFFERENTIAL;
      StorageEngineReader reader = mock(StorageEngineReader.class);
      
      // Base snapshot (revision 1)
      Roaring64Bitmap baseBitmap = new Roaring64Bitmap();
      baseBitmap.add(100);
      baseBitmap.add(200);
      baseBitmap.add(300);
      BitmapChunkPage basePage = BitmapChunkPage.createFull(
          1L, 1, IndexType.PATH, 0, 65536, baseBitmap);
      
      // Delta (revision 2): add 400, remove 200
      Roaring64Bitmap additions = new Roaring64Bitmap();
      additions.add(400);
      Roaring64Bitmap removals = new Roaring64Bitmap();
      removals.add(200);
      BitmapChunkPage deltaPage = BitmapChunkPage.createDelta(
          1L, 2, IndexType.PATH, 0, 65536, additions, removals);
      
      // Combine (newest first)
      List<BitmapChunkPage> fragments = List.of(deltaPage, basePage);
      BitmapChunkPage result = type.combineBitmapChunks(fragments, 5, reader);
      
      assertNotNull(result);
      assertTrue(result.isFullSnapshot());
      assertEquals(3, result.getBitmap().getLongCardinality());
      assertTrue(result.getBitmap().contains(100));
      assertFalse(result.getBitmap().contains(200)); // Removed
      assertTrue(result.getBitmap().contains(300));
      assertTrue(result.getBitmap().contains(400)); // Added
    }

    @Test
    @DisplayName("DIFFERENTIAL snapshot at revision % revsToRestore == 0")
    void testDifferentialSnapshotDecision() {
      VersioningType type = VersioningType.DIFFERENTIAL;
      List<BitmapChunkPage> fragments = createDummyFragments(1);
      
      // revsToRestore = 5
      assertTrue(type.shouldStoreBitmapFullSnapshot(fragments, 5, 5));  // 5 % 5 == 0
      assertTrue(type.shouldStoreBitmapFullSnapshot(fragments, 10, 5)); // 10 % 5 == 0
      assertFalse(type.shouldStoreBitmapFullSnapshot(fragments, 2, 5)); // 2 % 5 != 0
      assertFalse(type.shouldStoreBitmapFullSnapshot(fragments, 3, 5)); // 3 % 5 != 0
    }
  }

  @Nested
  @DisplayName("INCREMENTAL versioning")
  class IncrementalVersioning {

    @Test
    @DisplayName("INCREMENTAL combines chain of deltas")
    void testIncrementalChainCombine() {
      VersioningType type = VersioningType.INCREMENTAL;
      StorageEngineReader reader = mock(StorageEngineReader.class);
      
      // Base (revision 1): {100, 200}
      Roaring64Bitmap baseBitmap = new Roaring64Bitmap();
      baseBitmap.add(100);
      baseBitmap.add(200);
      BitmapChunkPage basePage = BitmapChunkPage.createFull(
          1L, 1, IndexType.PATH, 0, 65536, baseBitmap);
      
      // Delta 1 (revision 2): +{300}
      BitmapChunkPage delta1 = BitmapChunkPage.createDelta(
          1L, 2, IndexType.PATH, 0, 65536,
          createBitmap(300), new Roaring64Bitmap());
      
      // Delta 2 (revision 3): +{400}, -{100}
      BitmapChunkPage delta2 = BitmapChunkPage.createDelta(
          1L, 3, IndexType.PATH, 0, 65536,
          createBitmap(400), createBitmap(100));
      
      // Delta 3 (revision 4): +{500}
      BitmapChunkPage delta3 = BitmapChunkPage.createDelta(
          1L, 4, IndexType.PATH, 0, 65536,
          createBitmap(500), new Roaring64Bitmap());
      
      // Combine (newest first)
      List<BitmapChunkPage> fragments = List.of(delta3, delta2, delta1, basePage);
      BitmapChunkPage result = type.combineBitmapChunks(fragments, 5, reader);
      
      // Expected: {100} removed, {300, 400, 500} added = {200, 300, 400, 500}
      assertNotNull(result);
      assertTrue(result.isFullSnapshot());
      assertEquals(4, result.getBitmap().getLongCardinality());
      assertFalse(result.getBitmap().contains(100)); // Removed in delta2
      assertTrue(result.getBitmap().contains(200));
      assertTrue(result.getBitmap().contains(300));
      assertTrue(result.getBitmap().contains(400));
      assertTrue(result.getBitmap().contains(500));
    }

    @Test
    @DisplayName("INCREMENTAL snapshot when chain >= revsToRestore - 1")
    void testIncrementalSnapshotDecision() {
      VersioningType type = VersioningType.INCREMENTAL;
      
      // revsToRestore = 5, chain length threshold = 4
      assertTrue(type.shouldStoreBitmapFullSnapshot(createDummyFragments(4), 5, 5));  // 4 >= 4
      assertTrue(type.shouldStoreBitmapFullSnapshot(createDummyFragments(5), 6, 5));  // 5 >= 4
      assertFalse(type.shouldStoreBitmapFullSnapshot(createDummyFragments(2), 3, 5)); // 2 < 4
      assertFalse(type.shouldStoreBitmapFullSnapshot(createDummyFragments(3), 4, 5)); // 3 < 4
    }
  }

  @Nested
  @DisplayName("SLIDING_SNAPSHOT versioning")
  class SlidingSnapshotVersioning {

    @Test
    @DisplayName("SLIDING_SNAPSHOT combines like INCREMENTAL")
    void testSlidingSnapshotCombine() {
      VersioningType type = VersioningType.SLIDING_SNAPSHOT;
      StorageEngineReader reader = mock(StorageEngineReader.class);
      
      // Base + one delta
      Roaring64Bitmap baseBitmap = new Roaring64Bitmap();
      baseBitmap.add(100);
      baseBitmap.add(200);
      BitmapChunkPage basePage = BitmapChunkPage.createFull(
          1L, 1, IndexType.PATH, 0, 65536, baseBitmap);
      
      BitmapChunkPage delta = BitmapChunkPage.createDelta(
          1L, 2, IndexType.PATH, 0, 65536,
          createBitmap(300), new Roaring64Bitmap());
      
      List<BitmapChunkPage> fragments = List.of(delta, basePage);
      BitmapChunkPage result = type.combineBitmapChunks(fragments, 5, reader);
      
      assertEquals(3, result.getBitmap().getLongCardinality());
      assertTrue(result.getBitmap().contains(100));
      assertTrue(result.getBitmap().contains(200));
      assertTrue(result.getBitmap().contains(300));
    }

    @Test
    @DisplayName("SLIDING_SNAPSHOT uses chain length like INCREMENTAL")
    void testSlidingSnapshotDecision() {
      VersioningType type = VersioningType.SLIDING_SNAPSHOT;
      
      // Same as INCREMENTAL
      assertTrue(type.shouldStoreBitmapFullSnapshot(createDummyFragments(4), 5, 5));
      assertFalse(type.shouldStoreBitmapFullSnapshot(createDummyFragments(2), 3, 5));
    }
  }

  @Nested
  @DisplayName("Tombstone handling")
  class TombstoneHandling {

    @Test
    @DisplayName("Tombstone clears all data")
    void testTombstoneClearsData() {
      VersioningType type = VersioningType.INCREMENTAL;
      StorageEngineReader reader = mock(StorageEngineReader.class);
      
      // Base with data
      Roaring64Bitmap baseBitmap = new Roaring64Bitmap();
      baseBitmap.add(100);
      baseBitmap.add(200);
      BitmapChunkPage basePage = BitmapChunkPage.createFull(
          1L, 1, IndexType.PATH, 0, 65536, baseBitmap);
      
      // Tombstone marks chunk as deleted
      BitmapChunkPage tombstone = BitmapChunkPage.createTombstone(
          1L, 2, IndexType.PATH, 0, 65536);
      
      // New data after tombstone
      BitmapChunkPage newData = BitmapChunkPage.createDelta(
          1L, 3, IndexType.PATH, 0, 65536,
          createBitmap(300), new Roaring64Bitmap());
      
      List<BitmapChunkPage> fragments = List.of(newData, tombstone, basePage);
      BitmapChunkPage result = type.combineBitmapChunks(fragments, 5, reader);
      
      // Should only contain data added after tombstone
      assertEquals(1, result.getBitmap().getLongCardinality());
      assertFalse(result.getBitmap().contains(100));
      assertFalse(result.getBitmap().contains(200));
      assertTrue(result.getBitmap().contains(300));
    }
  }

  @Nested
  @DisplayName("ChunkDirectory integration")
  class ChunkDirectoryIntegration {

    @Test
    @DisplayName("ChunkDirectory manages multiple chunks")
    void testChunkDirectoryMultipleChunks() {
      ChunkDirectory dir = new ChunkDirectory();
      
      // Add references for chunks 0, 2, 5
      PageReference ref0 = dir.getOrCreateChunkRef(0);
      ref0.setKey(100);
      
      PageReference ref2 = dir.getOrCreateChunkRef(2);
      ref2.setKey(200);
      
      PageReference ref5 = dir.getOrCreateChunkRef(5);
      ref5.setKey(500);
      
      assertEquals(3, dir.chunkCount());
      
      // Serialize and deserialize
      int size = ChunkDirectorySerializer.serializedSize(dir);
      byte[] bytes = new byte[size];
      ChunkDirectorySerializer.serialize(dir, bytes, 0);
      
      ChunkDirectory restored = ChunkDirectorySerializer.deserialize(bytes, 0, size);
      
      assertEquals(3, restored.chunkCount());
      assertEquals(100, restored.getChunkRef(0).getKey());
      assertEquals(200, restored.getChunkRef(2).getKey());
      assertEquals(500, restored.getChunkRef(5).getKey());
    }

    @Test
    @DisplayName("ChunkDirectory.chunkIndexFor maps document keys correctly")
    void testChunkIndexMapping() {
      // Keys 0-65535 go to chunk 0
      assertEquals(0, ChunkDirectory.chunkIndexFor(0));
      assertEquals(0, ChunkDirectory.chunkIndexFor(32000));
      assertEquals(0, ChunkDirectory.chunkIndexFor(65535));
      
      // Keys 65536-131071 go to chunk 1
      assertEquals(1, ChunkDirectory.chunkIndexFor(65536));
      assertEquals(1, ChunkDirectory.chunkIndexFor(100000));
      assertEquals(1, ChunkDirectory.chunkIndexFor(131071));
      
      // Large keys
      assertEquals(15, ChunkDirectory.chunkIndexFor(1_000_000));
    }
  }

  @Nested
  @DisplayName("Edge cases")
  class EdgeCases {

    @Test
    @DisplayName("Empty bitmap handling")
    void testEmptyBitmapHandling() {
      VersioningType type = VersioningType.INCREMENTAL;
      StorageEngineReader reader = mock(StorageEngineReader.class);
      
      // Empty base
      BitmapChunkPage emptyPage = BitmapChunkPage.createEmptyFull(
          1L, 1, IndexType.PATH, 0, 65536);
      
      // Add some data
      BitmapChunkPage delta = BitmapChunkPage.createDelta(
          1L, 2, IndexType.PATH, 0, 65536,
          createBitmap(100, 200), new Roaring64Bitmap());
      
      List<BitmapChunkPage> fragments = List.of(delta, emptyPage);
      BitmapChunkPage result = type.combineBitmapChunks(fragments, 5, reader);
      
      assertEquals(2, result.getBitmap().getLongCardinality());
    }

    @Test
    @DisplayName("Single delta without base")
    void testSingleDeltaWithoutBase() {
      VersioningType type = VersioningType.INCREMENTAL;
      StorageEngineReader reader = mock(StorageEngineReader.class);
      
      // Just a delta (shouldn't normally happen, but should handle gracefully)
      BitmapChunkPage delta = BitmapChunkPage.createDelta(
          1L, 2, IndexType.PATH, 0, 65536,
          createBitmap(100), new Roaring64Bitmap());
      
      List<BitmapChunkPage> fragments = List.of(delta);
      BitmapChunkPage result = type.combineBitmapChunks(fragments, 5, reader);
      
      // Should convert to full with empty base
      assertTrue(result.isFullSnapshot());
    }

    @Test
    @DisplayName("First revision is always full snapshot")
    void testFirstRevisionAlwaysFull() {
      for (VersioningType type : VersioningType.values()) {
        List<BitmapChunkPage> emptyFragments = List.of();
        assertTrue(type.shouldStoreBitmapFullSnapshot(emptyFragments, 1, 5),
            type.name() + " should return true for revision 1");
      }
    }

    @Test
    @DisplayName("Multiple full snapshots - newer wins")
    void testMultipleFullSnapshots() {
      VersioningType type = VersioningType.DIFFERENTIAL;
      StorageEngineReader reader = mock(StorageEngineReader.class);
      
      // Older full snapshot
      BitmapChunkPage older = BitmapChunkPage.createFull(
          1L, 1, IndexType.PATH, 0, 65536, createBitmap(100, 200));
      
      // Newer full snapshot (replaces older)
      BitmapChunkPage newer = BitmapChunkPage.createFull(
          1L, 5, IndexType.PATH, 0, 65536, createBitmap(300, 400, 500));
      
      List<BitmapChunkPage> fragments = List.of(newer, older);
      BitmapChunkPage result = type.combineBitmapChunks(fragments, 5, reader);
      
      // Newer snapshot should replace older completely
      assertEquals(3, result.getBitmap().getLongCardinality());
      assertFalse(result.getBitmap().contains(100));
      assertFalse(result.getBitmap().contains(200));
      assertTrue(result.getBitmap().contains(300));
      assertTrue(result.getBitmap().contains(400));
      assertTrue(result.getBitmap().contains(500));
    }
  }

  // ===== Helper methods =====

  private static Roaring64Bitmap createBitmap(long... keys) {
    Roaring64Bitmap bitmap = new Roaring64Bitmap();
    for (long key : keys) {
      bitmap.add(key);
    }
    return bitmap;
  }

  private static List<BitmapChunkPage> createDummyFragments(int count) {
    List<BitmapChunkPage> fragments = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      fragments.add(BitmapChunkPage.createEmptyDelta(
          1L, i + 1, IndexType.PATH, 0, 65536));
    }
    return fragments;
  }
}

